use std::net::SocketAddr;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

use aether_core::error::{AetherError, Result};
use aether_storage::StorageEngine;

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
    storage: Arc<StorageEngine>,
}

impl HttpServer {
    pub fn new(addr: SocketAddr, storage: Arc<StorageEngine>) -> Self {
        Self { addr, storage }
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
            tokio::spawn(async move {
                if let Err(e) = Self::handle_http(socket, storage).await {
                    error!("HTTP request handling error: {}", e);
                }
            });
        }
    }

    async fn handle_http(mut socket: TcpStream, storage: Arc<StorageEngine>) -> Result<()> {
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

        // Find body after \r\n\r\n
        let body = if let Some(idx) = request_str.find("\r\n\r\n") {
            &request_str[idx + 4..]
        } else {
            ""
        };

        let (status_code, content_type, response_body) = match (method, path) {
            ("GET", "/") | ("GET", "/dashboard") => (
                200,
                "text/html; charset=utf-8",
                DEVTOOLS_HTML.to_string(),
            ),

            ("GET", "/health") => (
                200,
                "application/json",
                r#"{"status":"healthy","engine":"aetherdb-rust","version":"0.1.0"}"#.to_string(),
            ),
            
            ("POST", "/v1/get") => {
                match serde_json::from_str::<GetRequest>(body) {
                    Ok(req) => match storage.get(req.key.as_bytes()) {
                        Ok(Some(aether_core::types::ValueState::Some(bytes))) => {
                            let val_str = String::from_utf8_lossy(&bytes);
                            (200, "application/json", format!(r#"{{"found":true,"value":{}}}"#, serde_json::to_string(&val_str.to_string()).unwrap()))
                        }
                        Ok(_) => (200, "application/json", r#"{"found":false,"value":null}"#.to_string()),
                        Err(e) => (500, "application/json", format!(r#"{{"error":"{}"}}"#, e)),
                    },
                    Err(_) => (400, "application/json", r#"{"error":"Invalid JSON. Expected {key: string}"}"#.to_string()),
                }
            }

            ("POST", "/v1/set") => {
                match serde_json::from_str::<SetRequest>(body) {
                    Ok(req) => {
                        let state = aether_core::types::ValueState::Some(req.value.into_bytes());
                        match storage.put(req.key.into_bytes(), state) {
                            Ok(_) => (200, "application/json", r#"{"status":"ok"}"#.to_string()),
                            Err(e) => (500, "application/json", format!(r#"{{"error":"{}"}}"#, e)),
                        }
                    }
                    Err(_) => (400, "application/json", r#"{"error":"Invalid JSON. Expected {key: string, value: string}"}"#.to_string()),
                }
            }

            ("POST", "/v1/del") => {
                match serde_json::from_str::<DelRequest>(body) {
                    Ok(req) => {
                        let state = aether_core::types::ValueState::Tombstone;
                        match storage.put(req.key.into_bytes(), state) {
                            Ok(_) => (200, "application/json", r#"{"status":"ok"}"#.to_string()),
                            Err(e) => (500, "application/json", format!(r#"{{"error":"{}"}}"#, e)),
                        }
                    }
                    Err(_) => (400, "application/json", r#"{"error":"Invalid JSON. Expected {key: string}"}"#.to_string()),
                }
            }

            ("POST", "/v1/incr") => {
                match serde_json::from_str::<IncrRequest>(body) {
                    Ok(req) => {
                        let delta = req.delta.unwrap_or(1);
                        match storage.incr(req.key.clone().into_bytes(), delta) {
                            Ok(new_val) => (200, "application/json", format!(r#"{{"key":"{}","value":{}}}"#, req.key, new_val)),
                            Err(e) => (500, "application/json", format!(r#"{{"error":"{}"}}"#, e)),
                        }
                    }
                    Err(_) => (400, "application/json", r#"{"error":"Invalid JSON. Expected {key: string, delta?: number}"}"#.to_string()),
                }
            }

            ("POST", "/v1/vector/upsert") => {
                match serde_json::from_str::<VectorUpsertRequest>(body) {
                    Ok(req) => match storage.upsert_vector(&req.id, req.vector, req.metadata) {
                        Ok(_) => (200, "application/json", r#"{"status":"ok"}"#.to_string()),
                        Err(e) => (500, "application/json", format!(r#"{{"error":"{}"}}"#, e)),
                    },
                    Err(e) => (400, "application/json", format!(r#"{{"error":"Invalid JSON: {}"}}"#, e)),
                }
            }

            ("POST", "/v1/vector/search") => {
                match serde_json::from_str::<VectorSearchRequest>(body) {
                    Ok(req) => {
                        let top_k = req.top_k.unwrap_or(5);
                        match storage.search_vector(&req.vector, top_k) {
                            Ok(results) => {
                                let formatted: Vec<VectorSearchResult> = results
                                    .into_iter()
                                    .map(|(id, score, metadata)| VectorSearchResult { id, score, metadata })
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
  <title>AetherDB • DevTools Console</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    body { font-family: 'Inter', sans-serif; background-color: #0b0f17; color: #e2e8f0; }
    code, pre, .mono { font-family: 'JetBrains Mono', monospace; }
  </style>
</head>
<body class="min-h-screen flex flex-col">
  <!-- Header -->
  <header class="border-b border-slate-800 bg-slate-900/60 backdrop-blur px-6 py-4 flex items-center justify-between sticky top-0 z-50">
    <div class="flex items-center gap-3">
      <div class="h-8 w-8 rounded-lg bg-gradient-to-tr from-cyan-500 to-indigo-500 flex items-center justify-center font-bold text-black text-lg">⚡</div>
      <div>
        <h1 class="font-bold text-white tracking-tight flex items-center gap-2">
          AetherDB <span class="text-xs px-2 py-0.5 rounded bg-cyan-950 text-cyan-400 border border-cyan-800 font-mono">v0.1.0-rust</span>
        </h1>
        <p class="text-xs text-slate-400">Unified Storage Engine • KV + Vector + Multi-Raft</p>
      </div>
    </div>
    <div class="flex items-center gap-4 text-xs mono">
      <div class="flex items-center gap-2 px-3 py-1.5 rounded-full bg-emerald-950/60 border border-emerald-800 text-emerald-400">
        <span class="h-2 w-2 rounded-full bg-emerald-400 animate-pulse"></span>
        Node 1 Online (8301)
      </div>
    </div>
  </header>

  <!-- Main Container -->
  <main class="flex-1 max-w-7xl w-full mx-auto p-6 grid grid-cols-1 lg:grid-cols-3 gap-6">
    <!-- Left Column: KV & Vector Interactive Explorer -->
    <div class="lg:col-span-2 space-y-6">
      
      <!-- Card: Key-Value Explorer -->
      <div class="bg-slate-900/80 border border-slate-800 rounded-xl p-5 shadow-xl">
        <h2 class="text-sm font-semibold text-slate-200 mb-3 flex items-center gap-2">
          <span class="text-cyan-400">❖</span> Key-Value & Session State Explorer
        </h2>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-3 mb-4">
          <input id="kvKey" type="text" placeholder="Key (e.g. user:session:100)" class="mono text-xs px-3 py-2 bg-slate-950 border border-slate-700 rounded-lg text-white focus:outline-none focus:border-cyan-500">
          <input id="kvVal" type="text" placeholder="Value (String or JSON)" class="mono text-xs px-3 py-2 bg-slate-950 border border-slate-700 rounded-lg text-white focus:outline-none focus:border-cyan-500">
          <div class="flex gap-2">
            <button onclick="handleSet()" class="flex-1 bg-cyan-600 hover:bg-cyan-500 text-white text-xs font-semibold py-2 px-3 rounded-lg transition">SET</button>
            <button onclick="handleGet()" class="flex-1 bg-slate-800 hover:bg-slate-700 text-white text-xs font-semibold py-2 px-3 rounded-lg transition border border-slate-700">GET</button>
          </div>
        </div>
        <div class="bg-slate-950 border border-slate-800/80 rounded-lg p-3">
          <div class="text-xs text-slate-400 mb-1 flex justify-between"><span>Output Console:</span><span id="kvStatus" class="text-emerald-400">Ready</span></div>
          <pre id="kvOutput" class="mono text-xs text-cyan-300 overflow-x-auto min-h-[40px]">// Result will appear here...</pre>
        </div>
      </div>

      <!-- Card: SIMD Vector Playground -->
      <div class="bg-slate-900/80 border border-slate-800 rounded-xl p-5 shadow-xl">
        <h2 class="text-sm font-semibold text-slate-200 mb-3 flex items-center gap-2">
          <span class="text-indigo-400">◈</span> SIMD Vector Similarity Search
        </h2>
        <div class="space-y-3 mb-4">
          <input id="vecId" type="text" placeholder="Vector ID (e.g. doc:raft_paper)" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700 rounded-lg text-white focus:outline-none focus:border-indigo-500">
          <input id="vecFloats" type="text" placeholder="Float Array: [0.91, 0.12, 0.05, -0.15, 0.33, 0.04, 0.11]" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700 rounded-lg text-white focus:outline-none focus:border-indigo-500">
          <div class="flex gap-3">
            <button onclick="handleVecUpsert()" class="flex-1 bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-semibold py-2 rounded-lg transition">UPSERT VECTOR</button>
            <button onclick="handleVecSearch()" class="flex-1 bg-slate-800 hover:bg-slate-700 text-indigo-300 text-xs font-semibold py-2 rounded-lg transition border border-slate-700">SEARCH TOP-K</button>
          </div>
        </div>
        <div id="vecResults" class="space-y-2">
          <!-- Dynamic Vector Results -->
        </div>
      </div>

    </div>

    <!-- Right Column: System Status & Topology -->
    <div class="space-y-6">
      
      <!-- Cluster Status Widget -->
      <div class="bg-slate-900/80 border border-slate-800 rounded-xl p-5 shadow-xl">
        <h3 class="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-4">Cluster Engine Telemetry</h3>
        <div class="space-y-3 text-xs">
          <div class="flex justify-between py-1.5 border-b border-slate-800">
            <span class="text-slate-400">Consensus State:</span>
            <span class="mono text-emerald-400 font-semibold">Leader (Term 1)</span>
          </div>
          <div class="flex justify-between py-1.5 border-b border-slate-800">
            <span class="text-slate-400">Storage Architecture:</span>
            <span class="mono text-white">LSM + 4KB SSTable</span>
          </div>
          <div class="flex justify-between py-1.5 border-b border-slate-800">
            <span class="text-slate-400">Vector Math Kernel:</span>
            <span class="mono text-cyan-400 font-semibold">AVX2/NEON SIMD</span>
          </div>
          <div class="flex justify-between py-1.5">
            <span class="text-slate-400">HTTP REST Port:</span>
            <span class="mono text-white">8301</span>
          </div>
        </div>
      </div>

      <!-- Quickstart Snippet -->
      <div class="bg-slate-900/80 border border-slate-800 rounded-xl p-5 shadow-xl">
        <h3 class="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">SDK Quickstart</h3>
        <pre class="mono text-[11px] text-slate-300 bg-slate-950 p-3 rounded-lg border border-slate-800 overflow-x-auto">
<span class="text-cyan-400">import</span> { AetherDB } <span class="text-cyan-400">from</span> <span class="text-emerald-400">"@aetherdb/sdk"</span>;

<span class="text-cyan-400">const</span> db = <span class="text-cyan-400">new</span> AetherDB();

<span class="text-slate-500">// Transactional Session State</span>
<span class="text-cyan-400">await</span> db.set(<span class="text-emerald-400">"agent:ctx"</span>, { task: <span class="text-emerald-400">"eval"</span> });

<span class="text-slate-500">// Semantic Memory Search</span>
<span class="text-cyan-400">const</span> mems = <span class="text-cyan-400">await</span> db.vector.search(queryVec, 5);</pre>
      </div>

    </div>
  </main>

  <script>
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
        div.className = 'p-3 bg-slate-950 border border-slate-800 rounded-lg flex justify-between items-center text-xs';
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
    }
  </script>
</body>
</html>"#;

