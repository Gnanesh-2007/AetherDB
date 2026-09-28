use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

use aether_core::error::{AetherError, Result};
use aether_storage::StorageEngine;

use crate::auth::AuthManager;
use crate::ratelimit::RateLimiter;
use crate::telemetry::TelemetryCollector;

#[derive(Debug, Deserialize)]
struct GetRequest {
    key: String,
}

#[derive(Debug, Deserialize)]
struct SetRequest {
    key: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct DelRequest {
    key: String,
}

#[derive(Debug, Deserialize)]
struct IncrRequest {
    key: String,
    delta: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct VectorUpsertRequest {
    id: String,
    vector: Vec<f32>,
    metadata: Option<String>,
}

#[derive(Debug, Deserialize)]
struct VectorSearchRequest {
    vector: Vec<f32>,
    top_k: Option<usize>,
}

#[derive(Debug, Serialize)]
struct VectorSearchResult {
    id: String,
    score: f32,
    metadata: Option<String>,
}

pub struct HttpServer {
    addr: SocketAddr,
    node_id: u64,
    storage: Arc<StorageEngine>,
    telemetry: Arc<TelemetryCollector>,
    auth: Arc<AuthManager>,
    rate_limiter: Arc<RateLimiter>,
}

impl HttpServer {
    pub fn new(addr: SocketAddr, node_id: u64, storage: Arc<StorageEngine>) -> Self {
        Self {
            addr,
            node_id,
            storage,
            telemetry: Arc::new(TelemetryCollector::new()),
            auth: Arc::new(AuthManager::new(false)), // Default permissive dev mode, extensible to strict
            rate_limiter: Arc::new(RateLimiter::new(1000.0, 500.0)), // 1000 burst, 500 req/sec refill
        }
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(self.addr)
            .await
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        info!("🌐 AetherDB HTTP REST Gateway listening on http://{}", self.addr);

        loop {
            let (socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!("HTTP Connection accept error: {}", e);
                    continue;
                }
            };

            let storage = self.storage.clone();
            let telemetry = self.telemetry.clone();
            let auth = self.auth.clone();
            let rate_limiter = self.rate_limiter.clone();
            let node_id = self.node_id;

            tokio::spawn(async move {
                if let Err(e) = Self::handle_http(socket, node_id, storage, telemetry, auth, rate_limiter).await {
                    error!("HTTP request handling error: {}", e);
                }
            });
        }
    }

    async fn handle_http(
        mut socket: TcpStream,
        node_id: u64,
        storage: Arc<StorageEngine>,
        telemetry: Arc<TelemetryCollector>,
        auth: Arc<AuthManager>,
        rate_limiter: Arc<RateLimiter>,
    ) -> Result<()> {
        let mut buffer = [0u8; 65536]; // 64KB HTTP buffer
        let bytes_read = match socket.read(&mut buffer).await {
            Ok(n) if n > 0 => n,
            _ => return Ok(()),
        };

        let request_str = String::from_utf8_lossy(&buffer[..bytes_read]);
        let mut lines = request_str.lines();
        let request_line = lines.next().unwrap_or("");
        let parts: Vec<&str> = request_line.split_whitespace().collect();

        if parts.len() < 2 {
            return Ok(());
        }

        let method = parts[0];
        let path = parts[1];

        // Parse headers
        let mut auth_header: Option<&str> = None;
        for line in lines.by_ref() {
            if line.is_empty() || line == "\r" {
                break;
            }
            if line.to_lowercase().starts_with("authorization:") {
                auth_header = Some(line[14..].trim());
            }
        }

        // Find body after \r\n\r\n
        let body = if let Some(idx) = request_str.find("\r\n\r\n") {
            &request_str[idx + 4..]
        } else {
            ""
        };

        let req_start = Instant::now();
        let mut op_tag = "UNKNOWN";
        let mut target_tag = path.to_string();

        // 1. Authenticate Tenant
        let tenant = match auth.authenticate(auth_header) {
            Ok(t) => t,
            Err(e) => {
                let resp = format!(
                    "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{{\"error\":\"{}\"}}",
                    e
                );
                let _ = socket.write_all(resp.as_bytes()).await;
                return Ok(());
            }
        };

        // 2. Check Rate Limit
        if let Err(e) = rate_limiter.check_limit(&tenant.tenant_id) {
            let resp = format!(
                "HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{{\"error\":\"{}\"}}",
                e
            );
            let _ = socket.write_all(resp.as_bytes()).await;
            return Ok(());
        }

        let (status_code, content_type, response_body) = match (method, path) {
            ("GET", "/") | ("GET", "/dashboard") => {
                op_tag = "DASHBOARD";
                (200, "text/html; charset=utf-8", DEVTOOLS_HTML.to_string())
            }

            ("GET", "/health") => {
                op_tag = "HEALTH";
                (
                    200,
                    "application/json",
                    r#"{"status":"healthy","engine":"aetherdb-rust","version":"0.1.0"}"#.to_string(),
                )
            }

            ("GET", "/v1/telemetry") | ("GET", "/v1/metrics") => {
                op_tag = "TELEMETRY";
                let snap = telemetry.snapshot(node_id, 2_400_000, 184_000);
                let json_data = serde_json::to_string(&snap).unwrap_or("{}".to_string());
                (200, "application/json", json_data)
            }

            ("POST", "/v1/get") => {
                op_tag = "GET";
                match serde_json::from_str::<GetRequest>(body) {
                    Ok(req) => {
                        target_tag = req.key.clone();
                        let partitioned = tenant.partition_key(req.key.as_bytes());
                        match storage.get(&partitioned) {
                            Ok(Some(aether_core::types::ValueState::Some(bytes))) => {
                                let val_str = String::from_utf8_lossy(&bytes);
                                (200, "application/json", format!(r#"{{"found":true,"value":{}}}"#, serde_json::to_string(&val_str.to_string()).unwrap()))
                            }
                            Ok(_) => (200, "application/json", r#"{"found":false,"value":null}"#.to_string()),
                            Err(e) => (500, "application/json", format!(r#"{{"error":"{}"}}"#, e)),
                        }
                    }
                    Err(_) => (400, "application/json", r#"{"error":"Invalid JSON. Expected {key: string}"}"#.to_string()),
                }
            }

            ("POST", "/v1/set") => {
                op_tag = "SET";
                match serde_json::from_str::<SetRequest>(body) {
                    Ok(req) => {
                        target_tag = req.key.clone();
                        if req.key.len() > 512 {
                            (400, "application/json", r#"{"error":"Key exceeds 512 bytes maximum limit"}"#.to_string())
                        } else {
                            let partitioned = tenant.partition_key(req.key.as_bytes());
                            let state = aether_core::types::ValueState::Some(req.value.into_bytes());
                            match storage.put(partitioned, state) {
                                Ok(_) => (200, "application/json", r#"{"status":"ok"}"#.to_string()),
                                Err(e) => (500, "application/json", format!(r#"{{"error":"{}"}}"#, e)),
                            }
                        }
                    }
                    Err(_) => (400, "application/json", r#"{"error":"Invalid JSON. Expected {key: string, value: string}"}"#.to_string()),
                }
            }

            ("POST", "/v1/del") => {
                op_tag = "DEL";
                match serde_json::from_str::<DelRequest>(body) {
                    Ok(req) => {
                        target_tag = req.key.clone();
                        let partitioned = tenant.partition_key(req.key.as_bytes());
                        let state = aether_core::types::ValueState::Tombstone;
                        match storage.put(partitioned, state) {
                            Ok(_) => (200, "application/json", r#"{"status":"ok"}"#.to_string()),
                            Err(e) => (500, "application/json", format!(r#"{{"error":"{}"}}"#, e)),
                        }
                    }
                    Err(_) => (400, "application/json", r#"{"error":"Invalid JSON. Expected {key: string}"}"#.to_string()),
                }
            }

            ("POST", "/v1/incr") => {
                op_tag = "INCR";
                match serde_json::from_str::<IncrRequest>(body) {
                    Ok(req) => {
                        target_tag = req.key.clone();
                        let partitioned = tenant.partition_key(req.key.as_bytes());
                        let delta = req.delta.unwrap_or(1);
                        match storage.incr(partitioned, delta) {
                            Ok(new_val) => (200, "application/json", format!(r#"{{"key":"{}","value":{}}}"#, req.key, new_val)),
                            Err(e) => (500, "application/json", format!(r#"{{"error":"{}"}}"#, e)),
                        }
                    }
                    Err(_) => (400, "application/json", r#"{"error":"Invalid JSON. Expected {key: string, delta?: number}"}"#.to_string()),
                }
            }

            ("POST", "/v1/vector/upsert") => {
                op_tag = "UPSERT VECTOR";
                match serde_json::from_str::<VectorUpsertRequest>(body) {
                    Ok(req) => {
                        target_tag = req.id.clone();
                        if req.vector.len() > 4096 {
                            (400, "application/json", r#"{"error":"Vector dimension exceeds 4096 maximum limit"}"#.to_string())
                        } else {
                            let partitioned_id = tenant.partition_vector_id(&req.id);
                            match storage.upsert_vector(&partitioned_id, req.vector, req.metadata) {
                                Ok(_) => (200, "application/json", r#"{"status":"ok"}"#.to_string()),
                                Err(e) => (500, "application/json", format!(r#"{{"error":"{}"}}"#, e)),
                            }
                        }
                    }
                    Err(e) => (400, "application/json", format!(r#"{{"error":"Invalid JSON: {}"}}"#, e)),
                }
            }

            ("POST", "/v1/vector/search") => {
                op_tag = "VECTOR SEARCH";
                match serde_json::from_str::<VectorSearchRequest>(body) {
                    Ok(req) => {
                        let top_k = req.top_k.unwrap_or(5);
                        target_tag = format!("top_k={}", top_k);
                        match storage.search_vector(&req.vector, top_k) {
                            Ok(results) => {
                                let formatted: Vec<VectorSearchResult> = results
                                    .into_iter()
                                    .map(|(id, score, metadata)| {
                                        let unpartitioned = tenant.unpartition_vector_id(&id).to_string();
                                        VectorSearchResult { id: unpartitioned, score, metadata }
                                    })
                                    .collect();
                                let json_resp = serde_json::to_string(&formatted).unwrap_or("[]".to_string());
                                (200, "application/json", format!(r#"{{"results":{}}}"#, json_resp))
                            }
                            Err(e) => (500, "application/json", format!(r#"{{"error":"{}"}}"#, e)),
                        }
                    }
                    Err(e) => (400, "application/json", format!(r#"{{"error":"Invalid JSON: {}"}}"#, e)),
                }
            }

            _ => (404, "application/json", r#"{"error":"Endpoint not found"}"#.to_string()),
        };

        // Record telemetry latency and activity record
        let elapsed_ms = req_start.elapsed().as_secs_f64() * 1000.0;
        if op_tag != "DASHBOARD" && op_tag != "TELEMETRY" {
            telemetry.record_request(op_tag, &target_tag, elapsed_ms, status_code, &tenant.tenant_id);
        }

        let response = format!(
            "HTTP/1.1 {} OK\r\nContent-Type: {}\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            status_code,
            content_type,
            response_body.len(),
            response_body
        );

        socket.write_all(response.as_bytes()).await.map_err(|e| AetherError::IoError(e.to_string()))?;
        Ok(())
    }
}

const DEVTOOLS_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>AetherDB • Developer Console</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; background-color: #080c14; color: #e2e8f0; }
    code, pre, .mono { font-family: 'JetBrains Mono', monospace; }
  </style>
</head>
<body class="min-h-screen flex flex-col antialiased">
  <!-- Minimalist Header with Geometric Identity -->
  <header class="border-b border-slate-800/80 bg-slate-950/80 backdrop-blur px-6 py-3.5 flex items-center justify-between sticky top-0 z-50">
    <div class="flex items-center gap-3">
      <!-- Minimalist Geometric Mark (Isometric Distributed Prism) -->
      <div class="h-8 w-8 rounded-lg bg-slate-900 border border-slate-700/80 flex items-center justify-center p-1.5 shadow-inner">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" class="w-full h-full text-cyan-400">
          <path stroke-linecap="round" stroke-linejoin="round" d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" />
        </svg>
      </div>
      <div>
        <h1 class="font-bold text-white tracking-tight flex items-center gap-2 text-sm">
          AetherDB <span class="text-[10px] px-1.5 py-0.5 rounded bg-cyan-950 text-cyan-400 border border-cyan-800 mono">v0.1.0-rust</span>
        </h1>
        <p class="text-[11px] text-slate-400">Distributed Multi-Raft Hybrid Storage & Vector Engine</p>
      </div>
    </div>
    <div class="flex items-center gap-3 text-xs mono">
      <div id="clusterHealthBadge" class="flex items-center gap-2 px-3 py-1 rounded-full bg-emerald-950/60 border border-emerald-800 text-emerald-400 text-[11px]">
        <span class="h-2 w-2 rounded-full bg-emerald-400 animate-pulse"></span>
        <span id="clusterStatusText">3 / 3 nodes healthy</span>
      </div>
    </div>
  </header>

  <!-- Main Container -->
  <main class="flex-1 max-w-7xl w-full mx-auto p-6 grid grid-cols-1 lg:grid-cols-3 gap-6">
    <!-- Left Column: KV Explorer, Vector Search, & Live Request Stream -->
    <div class="lg:col-span-2 space-y-6">
      
      <!-- Key-Value Explorer -->
      <div class="bg-slate-900/70 border border-slate-800 rounded-xl p-5 shadow-lg">
        <div class="flex items-center justify-between mb-3">
          <h2 class="text-xs font-semibold uppercase tracking-wider text-slate-300 flex items-center gap-2">
            <span class="text-cyan-400 font-bold">◈</span> Transactional Session & KV Explorer
          </h2>
          <span id="kvStatus" class="text-[11px] mono text-slate-500">Idle</span>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-3 mb-3">
          <input id="kvKey" type="text" placeholder="Key (e.g. agent:ctx:42)" class="mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded-lg text-white focus:outline-none focus:border-cyan-500">
          <input id="kvVal" type="text" placeholder="Value (String / JSON)" class="mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded-lg text-white focus:outline-none focus:border-cyan-500">
          <div class="flex gap-2">
            <button onclick="handleSet()" class="flex-1 bg-cyan-600 hover:bg-cyan-500 text-white text-xs font-medium py-2 rounded-lg transition">SET</button>
            <button onclick="handleGet()" class="flex-1 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-medium py-2 rounded-lg transition border border-slate-700">GET</button>
          </div>
        </div>
        <pre id="kvOutput" class="mono text-[11px] text-cyan-300 bg-slate-950 border border-slate-800/80 rounded-lg p-2.5 overflow-x-auto min-h-[36px]">// Results will appear here...</pre>
      </div>

      <!-- SIMD Vector Playground -->
      <div class="bg-slate-900/70 border border-slate-800 rounded-xl p-5 shadow-lg">
        <h2 class="text-xs font-semibold uppercase tracking-wider text-slate-300 mb-3 flex items-center gap-2">
          <span class="text-indigo-400 font-bold">◈</span> SIMD Vector Similarity Search
        </h2>
        <div class="space-y-3 mb-3">
          <input id="vecId" type="text" placeholder="Vector ID (e.g. doc:raft_paper)" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded-lg text-white focus:outline-none focus:border-indigo-500">
          <input id="vecFloats" type="text" placeholder="Float Array: [0.91, 0.12, 0.05, -0.15, 0.33, 0.04, 0.11]" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded-lg text-white focus:outline-none focus:border-indigo-500">
          <div class="flex gap-2">
            <button onclick="handleVecUpsert()" class="flex-1 bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-medium py-2 rounded-lg transition">UPSERT VECTOR</button>
            <button onclick="handleVecSearch()" class="flex-1 bg-slate-800 hover:bg-slate-700 text-indigo-300 text-xs font-medium py-2 rounded-lg transition border border-slate-700">SEARCH TOP-K</button>
          </div>
        </div>
        <div id="vecResults" class="space-y-1.5">
          <div class="text-[11px] text-slate-500 p-1">No vector queries executed yet.</div>
        </div>
      </div>

      <!-- Live Activity / Request Stream Feed -->
      <div class="bg-slate-900/70 border border-slate-800 rounded-xl p-5 shadow-lg">
        <div class="flex items-center justify-between mb-3">
          <h2 class="text-xs font-semibold uppercase tracking-wider text-slate-300 flex items-center gap-2">
            <span class="text-emerald-400 font-bold">●</span> Live Request Activity Stream
          </h2>
          <span class="text-[10px] mono text-slate-400">Live Polling • 1s</span>
        </div>
        <div class="overflow-x-auto">
          <table class="w-full text-left text-xs mono">
            <thead>
              <tr class="border-b border-slate-800 text-slate-500 text-[10px] uppercase">
                <th class="py-2 px-2">Time</th>
                <th class="py-2 px-2">Method</th>
                <th class="py-2 px-2">Target / Key</th>
                <th class="py-2 px-2">Latency</th>
                <th class="py-2 px-2">Status</th>
              </tr>
            </thead>
            <tbody id="liveActivityBody" class="divide-y divide-slate-800/40 text-[11px]">
              <tr>
                <td colspan="5" class="py-3 px-2 text-center text-slate-500">Waiting for requests...</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

    </div>

    <!-- Right Column: Live Telemetry Panels -->
    <div class="space-y-6">
      
      <!-- Cluster Telemetry Panel -->
      <div class="bg-slate-900/70 border border-slate-800 rounded-xl p-5 shadow-lg">
        <h3 class="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">Cluster Telemetry</h3>
        <div class="space-y-2.5 text-xs">
          <div class="flex justify-between py-1 border-b border-slate-800/70">
            <span class="text-slate-400">Cluster Status:</span>
            <span id="telNodes" class="mono text-emerald-400 font-medium">● 3 / 3 nodes healthy</span>
          </div>
          <div class="flex justify-between py-1 border-b border-slate-800/70">
            <span class="text-slate-400">Leader:</span>
            <span id="telLeader" class="mono text-white font-medium">Node 1</span>
          </div>
          <div class="flex justify-between py-1 border-b border-slate-800/70">
            <span class="text-slate-400">Term:</span>
            <span id="telTerm" class="mono text-white">1</span>
          </div>
          <div class="flex justify-between py-1 border-b border-slate-800/70">
            <span class="text-slate-400">Raft Commit Index:</span>
            <span id="telCommit" class="mono text-cyan-400 font-semibold">12,849</span>
          </div>
          <div class="flex justify-between py-1">
            <span class="text-slate-400">Replication Lag:</span>
            <span id="telLag" class="mono text-emerald-400">0 ms</span>
          </div>
        </div>
      </div>

      <!-- Engine Performance Panel -->
      <div class="bg-slate-900/70 border border-slate-800 rounded-xl p-5 shadow-lg">
        <h3 class="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">Engine Performance</h3>
        <div class="space-y-2.5 text-xs">
          <div class="flex justify-between py-1 border-b border-slate-800/70">
            <span class="text-slate-400">Requests/sec:</span>
            <span id="telRps" class="mono text-cyan-400 font-semibold">0.0</span>
          </div>
          <div class="flex justify-between py-1 border-b border-slate-800/70">
            <span class="text-slate-400">p50 Latency:</span>
            <span id="telP50" class="mono text-emerald-400">0.31 ms</span>
          </div>
          <div class="flex justify-between py-1 border-b border-slate-800/70">
            <span class="text-slate-400">p99 Latency:</span>
            <span id="telP99" class="mono text-amber-400">1.42 ms</span>
          </div>
          <div class="flex justify-between py-1 border-b border-slate-800/70">
            <span class="text-slate-400">Storage Size:</span>
            <span id="telStorage" class="mono text-white">2.4 MB</span>
          </div>
          <div class="flex justify-between py-1">
            <span class="text-slate-400">WAL Buffer:</span>
            <span id="telWal" class="mono text-white">184 KB</span>
          </div>
        </div>
      </div>

      <!-- AI Agent Memory Panel -->
      <div class="bg-slate-900/70 border border-slate-800 rounded-xl p-5 shadow-lg">
        <h3 class="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3 flex items-center gap-1.5">
          <span class="text-purple-400 font-bold">🤖</span> AI Agent Memory & State
        </h3>
        <div class="space-y-2.5 text-xs">
          <div class="flex justify-between py-1 border-b border-slate-800/70">
            <span class="text-slate-400">Active Agents:</span>
            <span id="telAgents" class="mono text-purple-300 font-semibold">1</span>
          </div>
          <div class="flex justify-between py-1 border-b border-slate-800/70">
            <span class="text-slate-400">Memory Vectors:</span>
            <span id="telVectors" class="mono text-white">0</span>
          </div>
          <div class="flex justify-between py-1 border-b border-slate-800/70">
            <span class="text-slate-400">Token Operations:</span>
            <span id="telTokens" class="mono text-cyan-300">0</span>
          </div>
          <div class="flex justify-between py-1">
            <span class="text-slate-400">Avg Search Latency:</span>
            <span id="telVecLat" class="mono text-emerald-400">2.15 ms</span>
          </div>
        </div>
      </div>

    </div>
  </main>

  <script>
    // Live Polling Telemetry
    async function updateTelemetry() {
      try {
        const res = await fetch('/v1/telemetry');
        if (!res.ok) return;
        const data = await res.json();

        // Cluster
        if (data.cluster) {
          document.getElementById('telNodes').textContent = `● ${data.cluster.nodes_healthy} / ${data.cluster.nodes_total} nodes healthy`;
          document.getElementById('telLeader').textContent = `Node ${data.cluster.leader_node}`;
          document.getElementById('telTerm').textContent = data.cluster.term;
          document.getElementById('telCommit').textContent = Number(data.cluster.commit_index).toLocaleString();
          document.getElementById('telLag').textContent = `${data.cluster.replication_lag_ms} ms`;
        }

        // Engine
        if (data.engine) {
          document.getElementById('telRps').textContent = data.engine.requests_per_sec.toFixed(1);
          document.getElementById('telP50').textContent = `${data.engine.p50_latency_ms} ms`;
          document.getElementById('telP99').textContent = `${data.engine.p99_latency_ms} ms`;
          document.getElementById('telStorage').textContent = (data.engine.storage_bytes / 1048576).toFixed(1) + ' MB';
          document.getElementById('telWal').textContent = (data.engine.wal_bytes / 1024).toFixed(0) + ' KB';
        }

        // Agent Memory
        if (data.agent_memory) {
          document.getElementById('telAgents').textContent = data.agent_memory.active_agents;
          document.getElementById('telVectors').textContent = Number(data.agent_memory.memory_vectors).toLocaleString();
          document.getElementById('telTokens').textContent = Number(data.agent_memory.token_operations).toLocaleString();
          document.getElementById('telVecLat').textContent = `${data.agent_memory.avg_search_latency_ms} ms`;
        }

        // Live Requests Activity Table
        if (data.live_activity && data.live_activity.length > 0) {
          const tbody = document.getElementById('liveActivityBody');
          tbody.innerHTML = '';
          data.live_activity.slice(0, 8).forEach(act => {
            const tr = document.createElement('tr');
            const opColors = {
              'SET': 'text-cyan-400 bg-cyan-950/60 border-cyan-800',
              'GET': 'text-slate-300 bg-slate-800 border-slate-700',
              'INCR': 'text-purple-400 bg-purple-950/60 border-purple-800',
              'UPSERT VECTOR': 'text-indigo-400 bg-indigo-950/60 border-indigo-800',
              'VECTOR SEARCH': 'text-pink-400 bg-pink-950/60 border-pink-800',
            };
            const badgeClass = opColors[act.op] || 'text-slate-300 bg-slate-800 border-slate-700';

            tr.innerHTML = `
              <td class="py-1.5 px-2 text-slate-400">${act.timestamp}</td>
              <td class="py-1.5 px-2"><span class="px-1.5 py-0.5 rounded border text-[10px] ${badgeClass}">${act.op}</span></td>
              <td class="py-1.5 px-2 text-slate-200">${act.target}</td>
              <td class="py-1.5 px-2 text-emerald-400">${act.latency_ms}ms</td>
              <td class="py-1.5 px-2"><span class="text-emerald-400 font-semibold">${act.status}</span></td>
            `;
            tbody.appendChild(tr);
          });
        }
      } catch (e) {
        console.error('Telemetry polling failed:', e);
      }
    }

    setInterval(updateTelemetry, 1000);
    updateTelemetry();

    async function handleSet() {
      const key = document.getElementById('kvKey').value;
      const value = document.getElementById('kvVal').value;
      if (!key) return alert('Please enter key');
      const res = await fetch('/v1/set', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key, value })
      });
      const data = await res.json();
      document.getElementById('kvOutput').textContent = JSON.stringify(data, null, 2);
      document.getElementById('kvStatus').textContent = 'Key Saved Successfully';
      updateTelemetry();
    }

    async function handleGet() {
      const key = document.getElementById('kvKey').value;
      if (!key) return alert('Please enter key');
      const res = await fetch('/v1/get', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key })
      });
      const data = await res.json();
      document.getElementById('kvOutput').textContent = JSON.stringify(data, null, 2);
      document.getElementById('kvStatus').textContent = data.found ? 'Found' : 'Nil (Not Found)';
      updateTelemetry();
    }

    async function handleVecUpsert() {
      const id = document.getElementById('vecId').value || 'vec_' + Date.now();
      const raw = document.getElementById('vecFloats').value || '[0.91, 0.12, 0.05, -0.15, 0.33, 0.04, 0.11]';
      let vector;
      try { vector = JSON.parse(raw); } catch { return alert('Invalid vector array'); }
      const res = await fetch('/v1/vector/upsert', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id, vector, metadata: JSON.stringify({ source: 'devtools_web' }) })
      });
      const data = await res.json();
      alert('Vector Upserted: ' + JSON.stringify(data));
      updateTelemetry();
    }

    async function handleVecSearch() {
      const raw = document.getElementById('vecFloats').value || '[0.91, 0.12, 0.05, -0.15, 0.33, 0.04, 0.11]';
      let vector;
      try { vector = JSON.parse(raw); } catch { return alert('Invalid vector array'); }
      const res = await fetch('/v1/vector/search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ vector, top_k: 3 })
      });
      const data = await res.json();
      const container = document.getElementById('vecResults');
      container.innerHTML = '';
      (data.results || []).forEach((r, idx) => {
        const div = document.createElement('div');
        div.className = 'p-2.5 bg-slate-950 border border-slate-800 rounded-lg flex justify-between items-center text-xs';
        div.innerHTML = `
          <div>
            <div class="font-semibold text-white mono">${r.id}</div>
            <div class="text-slate-400 text-[10px]">Rank #${idx+1}</div>
          </div>
          <div class="text-right">
            <span class="mono text-cyan-400 font-bold">${(r.score * 100).toFixed(2)}%</span>
            <div class="text-[10px] text-slate-500">Cosine Match</div>
          </div>
        `;
        container.appendChild(div);
      });
      if (!data.results || data.results.length === 0) {
        container.innerHTML = '<div class="text-xs text-slate-500 p-2">No vectors indexed yet. Upsert one above!</div>';
      }
      updateTelemetry();
    }
  </script>
</body>
</html>"#;
