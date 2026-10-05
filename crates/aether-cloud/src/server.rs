use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

use crate::apikey::ApiKeyManager;
use crate::billing::BillingCalculator;
use crate::metering::MeteringEngine;
use crate::tenant::{PlanTier, TenantManager};
use aether_core::error::{AetherError, Result};

#[derive(Debug, Deserialize)]
struct CreateProjectRequest {
    name: String,
    region: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateKeyRequest {
    project_id: String,
    name: String,
    scopes: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct RevokeKeyRequest {
    key_id: String,
}

#[derive(Debug, Deserialize)]
struct UpdatePlanRequest {
    plan_tier: String, // "Free", "Pro", "Enterprise"
}

#[derive(Debug, Deserialize)]
struct MeteringEventRequest {
    project_id: String,
    op: String,
    count: Option<u64>,
}

pub struct AetherCloudServer {
    addr: SocketAddr,
    tenants: Arc<TenantManager>,
    keys: Arc<ApiKeyManager>,
    metering: Arc<MeteringEngine>,
}

impl AetherCloudServer {
    pub fn new(
        addr: SocketAddr,
        tenants: Arc<TenantManager>,
        keys: Arc<ApiKeyManager>,
        metering: Arc<MeteringEngine>,
    ) -> Self {
        Self {
            addr,
            tenants,
            keys,
            metering,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(self.addr)
            .await
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        info!(
            "☁️ AetherCloud Control Plane & SaaS Portal online at http://{}",
            self.addr
        );

        loop {
            let (socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!("AetherCloud connection accept error: {}", e);
                    continue;
                }
            };

            let tenants = self.tenants.clone();
            let keys = self.keys.clone();
            let metering = self.metering.clone();

            tokio::spawn(async move {
                if let Err(e) = Self::handle_client(socket, tenants, keys, metering).await {
                    error!("AetherCloud request error: {}", e);
                }
            });
        }
    }

    async fn handle_client(
        mut socket: TcpStream,
        tenants: Arc<TenantManager>,
        keys: Arc<ApiKeyManager>,
        metering: Arc<MeteringEngine>,
    ) -> Result<()> {
        let req = match aether_network::httpio::read_http_request(&mut socket).await {
            Ok(Some(r)) => r,
            _ => return Ok(()),
        };
        if req.method.is_empty() || req.path.is_empty() {
            return Ok(());
        }
        let method = req.method.as_str();
        let path = req.path.as_str();
        let body = req.body.as_str();

        let default_tenant_id = "org_default";
        let default_project_id = "proj_live_01";

        let (status_code, content_type, response_body) = match (method, path) {
            ("GET", "/health") => (
                200,
                "application/json",
                r#"{"status":"healthy","service":"aethercloud-control-plane","version":"0.1.0"}"#
                    .to_string(),
            ),

            ("GET", "/") | ("GET", "/dashboard") => {
                (200, "text/html; charset=utf-8", DASHBOARD_HTML.to_string())
            }

            ("GET", "/cloud/v1/projects") => {
                let list = tenants.list_projects(default_tenant_id);
                (
                    200,
                    "application/json",
                    serde_json::to_string(&list).unwrap(),
                )
            }

            ("POST", "/cloud/v1/projects") => {
                match serde_json::from_str::<CreateProjectRequest>(body) {
                    Ok(req) => {
                        let region = req.region.unwrap_or_else(|| "us-east-1".to_string());
                        let proj = tenants.create_project(default_tenant_id, &req.name, &region);
                        (
                            200,
                            "application/json",
                            serde_json::to_string(&proj).unwrap(),
                        )
                    }
                    Err(_) => (
                        400,
                        "application/json",
                        r#"{"error":"Invalid project request"}"#.to_string(),
                    ),
                }
            }

            ("GET", "/cloud/v1/keys") => {
                let key_list = keys.list_keys(default_project_id);
                (
                    200,
                    "application/json",
                    serde_json::to_string(&key_list).unwrap(),
                )
            }

            ("POST", "/cloud/v1/keys") => match serde_json::from_str::<CreateKeyRequest>(body) {
                Ok(req) => {
                    let scopes = req
                        .scopes
                        .unwrap_or_else(|| vec!["read".to_string(), "write".to_string()]);
                    let (api_key, raw_token) =
                        keys.create_key(default_tenant_id, &req.project_id, &req.name, scopes);
                    #[derive(Serialize)]
                    struct KeyCreatedResp {
                        key: crate::apikey::ApiKey,
                        raw_token: String,
                    }
                    (
                        200,
                        "application/json",
                        serde_json::to_string(&KeyCreatedResp {
                            key: api_key,
                            raw_token,
                        })
                        .unwrap(),
                    )
                }
                Err(_) => (
                    400,
                    "application/json",
                    r#"{"error":"Invalid key creation payload"}"#.to_string(),
                ),
            },

            ("POST", "/cloud/v1/keys/revoke") => {
                match serde_json::from_str::<RevokeKeyRequest>(body) {
                    Ok(req) => {
                        let success = keys.revoke_key(&req.key_id);
                        (
                            200,
                            "application/json",
                            format!(r#"{{"revoked":{}}}"#, success),
                        )
                    }
                    Err(_) => (
                        400,
                        "application/json",
                        r#"{"error":"Invalid revoke payload"}"#.to_string(),
                    ),
                }
            }

            ("GET", "/cloud/v1/usage") => {
                let org = tenants.get_org(default_tenant_id).unwrap();
                let report = metering.generate_report(
                    default_project_id,
                    org.plan_tier.max_monthly_ops(),
                    org.plan_tier.max_vectors(),
                    10 * 1024 * 1024 * 1024, // 10 GB limit
                );
                (
                    200,
                    "application/json",
                    serde_json::to_string(&report).unwrap(),
                )
            }

            ("GET", "/cloud/v1/billing") => {
                let org = tenants.get_org(default_tenant_id).unwrap();
                let usage = metering.get_usage(default_project_id);
                let invoice = BillingCalculator::calculate_invoice(org.plan_tier, &usage);
                (
                    200,
                    "application/json",
                    serde_json::to_string(&invoice).unwrap(),
                )
            }

            ("POST", "/cloud/v1/plan/update") => {
                match serde_json::from_str::<UpdatePlanRequest>(body) {
                    Ok(req) => {
                        let tier = match req.plan_tier.to_lowercase().as_str() {
                            "free" => PlanTier::Free,
                            "enterprise" => PlanTier::Enterprise,
                            _ => PlanTier::Pro,
                        };
                        tenants.update_plan(default_tenant_id, tier);
                        (
                            200,
                            "application/json",
                            r#"{"status":"updated"}"#.to_string(),
                        )
                    }
                    Err(_) => (
                        400,
                        "application/json",
                        r#"{"error":"Invalid plan payload"}"#.to_string(),
                    ),
                }
            }

            ("POST", "/cloud/v1/event") => {
                match serde_json::from_str::<MeteringEventRequest>(body) {
                    Ok(req) => {
                        let count = req.count.unwrap_or(1);
                        metering.record_operation(&req.project_id, &req.op, count);
                        (200, "application/json", r#"{"recorded":true}"#.to_string())
                    }
                    Err(_) => (
                        400,
                        "application/json",
                        r#"{"error":"Invalid event payload"}"#.to_string(),
                    ),
                }
            }

            _ => (
                404,
                "application/json",
                r#"{"error":"AetherCloud endpoint not found"}"#.to_string(),
            ),
        };

        let response = format!(
            "HTTP/1.1 {} OK\r\nContent-Type: {}\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            status_code,
            content_type,
            response_body.len(),
            response_body
        );

        socket
            .write_all(response.as_bytes())
            .await
            .map_err(|e| AetherError::IoError(e.to_string()))?;
        Ok(())
    }
}

const DASHBOARD_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>AetherCloud • Managed Database & AI Developer Platform</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; background-color: #070a11; color: #e2e8f0; }
    code, pre, .mono { font-family: 'JetBrains Mono', monospace; }
  </style>
</head>
<body class="min-h-screen flex flex-col antialiased">
  <!-- Cloud Header -->
  <header class="border-b border-slate-800 bg-slate-950/80 backdrop-blur px-8 py-4 flex items-center justify-between sticky top-0 z-50">
    <div class="flex items-center gap-3">
      <!-- Minimalist Distributed Lattice Mark -->
      <div class="h-9 w-9 rounded-xl bg-gradient-to-tr from-cyan-600 to-indigo-600 flex items-center justify-center p-1.5 shadow-lg shadow-cyan-950/50">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="w-full h-full text-white">
          <path stroke-linecap="round" stroke-linejoin="round" d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" />
        </svg>
      </div>
      <div>
        <div class="flex items-center gap-2">
          <h1 class="font-bold text-white tracking-tight text-base">AetherCloud</h1>
          <span class="text-[10px] px-2 py-0.5 rounded bg-cyan-950 text-cyan-400 border border-cyan-800 mono font-semibold">MANAGED CONTROL PLANE</span>
        </div>
        <p class="text-xs text-slate-400">Serverless State & Vector Platform for Autonomous AI Agents</p>
      </div>
    </div>
    
    <div class="flex items-center gap-4 text-xs">
      <div class="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-slate-900 border border-slate-800 text-slate-300">
        <span class="h-2 w-2 rounded-full bg-emerald-400 animate-pulse"></span>
        <span>Region: <b class="text-white">us-east-1</b></span>
      </div>
      <div class="flex items-center gap-2 px-3.5 py-1.5 rounded-lg bg-indigo-950/80 border border-indigo-700/80 text-indigo-300 font-medium">
        <span>Plan: <b id="headerPlanBadge" class="text-white">Pro Plan ($29/mo)</b></span>
      </div>
    </div>
  </header>

  <!-- Main SaaS Container -->
  <main class="flex-1 max-w-7xl w-full mx-auto p-8 space-y-8">
    
    <!-- Top Row: Project Info & Quick Connect Banner -->
    <div class="bg-slate-900/80 border border-slate-800 rounded-2xl p-6 shadow-xl relative overflow-hidden">
      <div class="absolute -right-10 -bottom-10 w-64 h-64 bg-cyan-500/5 rounded-full blur-3xl pointer-events-none"></div>
      
      <div class="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-6">
        <div>
          <div class="flex items-center gap-3">
            <h2 class="text-lg font-bold text-white" id="currentProjectTitle">Autonomous Agent Memory Fleet</h2>
            <span class="px-2 py-0.5 rounded bg-emerald-950 text-emerald-400 border border-emerald-800 text-xs font-medium">Active • 3-Node Raft Cluster</span>
          </div>
          <p class="text-xs text-slate-400 mt-1">Project ID: <span class="mono text-slate-300">proj_live_01</span> • Dedicated Tenant Namespace: <span class="mono text-cyan-400">t:org_default:*</span></p>
        </div>
        <div class="flex gap-2">
          <button onclick="copyConnectionString()" class="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-white rounded-xl text-xs font-semibold border border-slate-700 transition flex items-center gap-2">
            <span>📋 Copy Connection URI</span>
          </button>
        </div>
      </div>

      <!-- Quickstart Code Tabs -->
      <div class="bg-slate-950 border border-slate-800 rounded-xl p-4">
        <div class="flex items-center justify-between mb-2">
          <span class="text-xs text-slate-400 font-medium uppercase tracking-wider">SDK Connection Snippet:</span>
          <div class="flex gap-2 text-xs">
            <span class="text-cyan-400 font-semibold cursor-pointer">Node.js (TypeScript)</span>
            <span class="text-slate-500">|</span>
            <span class="text-slate-400 hover:text-white cursor-pointer">Python</span>
          </div>
        </div>
        <pre class="mono text-xs text-slate-200 overflow-x-auto p-1 leading-relaxed">
<span class="text-cyan-400">import</span> { AetherDB } <span class="text-cyan-400">from</span> <span class="text-emerald-400">"@aetherdb/sdk"</span>;

<span class="text-cyan-400">const</span> db = <span class="text-cyan-400">new</span> AetherDB({
  url: <span class="text-emerald-400">"http://localhost:8301"</span>,
  apiKey: <span id="codeApiKeyPreview" class="text-amber-300">"aether_sk_live_org_default_..."</span>
});

<span class="text-slate-500">// Instant transactional agent memory & semantic search</span>
<span class="text-cyan-400">await</span> db.set(<span class="text-emerald-400">"agent:session:ctx"</span>, { activeGoal: <span class="text-emerald-400">"Research Distributed Storage"</span> });
<span class="text-cyan-400">const</span> memories = <span class="text-cyan-400">await</span> db.vector.search(queryVector, 5);</pre>
      </div>
    </div>

    <!-- Middle Row: 4 Metric Quota Gauges -->
    <div class="grid grid-cols-1 md:grid-cols-4 gap-6">
      
      <!-- Card 1: Monthly Operations -->
      <div class="bg-slate-900/80 border border-slate-800 rounded-2xl p-5 shadow-lg space-y-3">
        <div class="flex justify-between items-center text-xs text-slate-400">
          <span>Monthly Operations</span>
          <span id="opsPctBadge" class="mono text-cyan-400 font-semibold">0.9%</span>
        </div>
        <div class="text-2xl font-bold text-white mono" id="opsCount">91,580</div>
        <div class="w-full bg-slate-950 rounded-full h-2 border border-slate-800 overflow-hidden">
          <div id="opsProgressBar" class="bg-cyan-500 h-full rounded-full transition-all duration-500" style="width: 0.9%"></div>
        </div>
        <div class="text-[11px] text-slate-500 flex justify-between">
          <span>Limit: <span id="opsLimit">10,000,000</span></span>
          <span class="text-slate-400">Pay-as-you-go</span>
        </div>
      </div>

      <!-- Card 2: Memory Vectors -->
      <div class="bg-slate-900/80 border border-slate-800 rounded-2xl p-5 shadow-lg space-y-3">
        <div class="flex justify-between items-center text-xs text-slate-400">
          <span>Vector Embeddings</span>
          <span id="vecsPctBadge" class="mono text-indigo-400 font-semibold">0.1%</span>
        </div>
        <div class="text-2xl font-bold text-white mono" id="vecsCount">1,250</div>
        <div class="w-full bg-slate-950 rounded-full h-2 border border-slate-800 overflow-hidden">
          <div id="vecsProgressBar" class="bg-indigo-500 h-full rounded-full transition-all duration-500" style="width: 0.1%"></div>
        </div>
        <div class="text-[11px] text-slate-500 flex justify-between">
          <span>Capacity: <span id="vecsLimit">1,000,000</span></span>
          <span class="text-slate-400">SIMD Ready</span>
        </div>
      </div>

      <!-- Card 3: Persistent Storage -->
      <div class="bg-slate-900/80 border border-slate-800 rounded-2xl p-5 shadow-lg space-y-3">
        <div class="flex justify-between items-center text-xs text-slate-400">
          <span>Persistent Storage</span>
          <span id="storagePctBadge" class="mono text-emerald-400 font-semibold">0.5%</span>
        </div>
        <div class="text-2xl font-bold text-white mono" id="storageCount">48.5 MB</div>
        <div class="w-full bg-slate-950 rounded-full h-2 border border-slate-800 overflow-hidden">
          <div id="storageProgressBar" class="bg-emerald-500 h-full rounded-full transition-all duration-500" style="width: 0.5%"></div>
        </div>
        <div class="text-[11px] text-slate-500 flex justify-between">
          <span>Quota: 10.0 GB</span>
          <span class="text-slate-400">SSD LSM-Tree</span>
        </div>
      </div>

      <!-- Card 4: Rate-Limit Token Ops -->
      <div class="bg-slate-900/80 border border-slate-800 rounded-2xl p-5 shadow-lg space-y-3">
        <div class="flex justify-between items-center text-xs text-slate-400">
          <span>Atomic Token Quota Ops</span>
          <span class="mono text-purple-400 font-semibold">Live</span>
        </div>
        <div class="text-2xl font-bold text-white mono" id="tokensCount">14,300</div>
        <div class="w-full bg-slate-950 rounded-full h-2 border border-slate-800 overflow-hidden">
          <div class="bg-purple-500 h-full rounded-full w-1/3"></div>
        </div>
        <div class="text-[11px] text-slate-500 flex justify-between">
          <span>Throughput Limit</span>
          <span class="text-purple-300 font-semibold">2,000 req/s</span>
        </div>
      </div>

    </div>

    <!-- Bottom Row: API Key Management & Real-Time Billing Breakdown -->
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-8">
      
      <!-- API Key Management (2 Cols) -->
      <div class="lg:col-span-2 bg-slate-900/80 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-4">
        <div class="flex items-center justify-between">
          <div>
            <h3 class="text-sm font-bold text-white uppercase tracking-wider flex items-center gap-2">
              <span class="text-cyan-400">🔑</span> Production API Keys
            </h3>
            <p class="text-xs text-slate-400 mt-0.5">Use secret keys to authenticate your agent backend securely.</p>
          </div>
          <button onclick="createNewApiKey()" class="px-3.5 py-2 bg-cyan-600 hover:bg-cyan-500 text-white rounded-xl text-xs font-semibold transition flex items-center gap-1.5 shadow-lg shadow-cyan-950">
            <span>+ Generate API Key</span>
          </button>
        </div>

        <div class="overflow-x-auto">
          <table class="w-full text-left text-xs mono">
            <thead>
              <tr class="border-b border-slate-800 text-slate-500 text-[11px] uppercase">
                <th class="py-2.5 px-3">Name</th>
                <th class="py-2.5 px-3">Token Prefix</th>
                <th class="py-2.5 px-3">Scopes</th>
                <th class="py-2.5 px-3">Status</th>
                <th class="py-2.5 px-3 text-right">Action</th>
              </tr>
            </thead>
            <tbody id="apiKeysTableBody" class="divide-y divide-slate-800/60 text-xs">
              <!-- Dynamically rendered keys -->
            </tbody>
          </table>
        </div>
      </div>

      <!-- Real-Time Billing & Invoice Preview (1 Col) -->
      <div class="bg-slate-900/80 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-4">
        <div class="flex items-center justify-between">
          <h3 class="text-sm font-bold text-white uppercase tracking-wider flex items-center gap-2">
            <span class="text-emerald-400">💳</span> Live Billing Estimate
          </h3>
          <span class="text-[10px] px-2 py-0.5 rounded bg-emerald-950 text-emerald-400 border border-emerald-800 mono">PRO-RATED</span>
        </div>

        <div class="space-y-2.5 text-xs">
          <div class="flex justify-between py-1.5 border-b border-slate-800/80">
            <span class="text-slate-400">Base Subscription (Pro):</span>
            <span class="mono text-white font-semibold" id="billBase">$29.00</span>
          </div>
          <div class="flex justify-between py-1.5 border-b border-slate-800/80">
            <span class="text-slate-400">KV Operations (67.6K ops):</span>
            <span class="mono text-slate-300" id="billKv">$0.14</span>
          </div>
          <div class="flex justify-between py-1.5 border-b border-slate-800/80">
            <span class="text-slate-400">Vector Searches (8.4K queries):</span>
            <span class="mono text-slate-300" id="billVec">$0.03</span>
          </div>
          <div class="flex justify-between py-1.5 border-b border-slate-800/80">
            <span class="text-slate-400">Token Operations (14.3K ops):</span>
            <span class="mono text-slate-300" id="billToken">$0.02</span>
          </div>
          <div class="flex justify-between py-1.5 border-b border-slate-800/80">
            <span class="text-slate-400">Storage (48.5 MB):</span>
            <span class="mono text-slate-300" id="billStorage">$0.01</span>
          </div>

          <div class="flex justify-between pt-2 text-sm font-bold text-white">
            <span>Projected Monthly Total:</span>
            <span class="mono text-emerald-400 text-base" id="billTotal">$29.20</span>
          </div>
        </div>

        <!-- Plan Tier Selector -->
        <div class="pt-3 border-t border-slate-800">
          <label class="text-[11px] text-slate-400 block mb-2 font-medium">Change Subscription Tier:</label>
          <div class="grid grid-cols-2 gap-2">
            <button onclick="updateTier('Free')" class="py-2 px-3 bg-slate-950 hover:bg-slate-800 border border-slate-800 text-slate-300 rounded-xl text-xs font-semibold transition">Free ($0)</button>
            <button onclick="updateTier('Pro')" class="py-2 px-3 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-xs font-semibold transition shadow-md shadow-indigo-950">Pro ($29)</button>
          </div>
        </div>

      </div>

    </div>

  </main>

  <script>
    async function loadCloudState() {
      try {
        // 1. Load Usage & Quotas
        const usageRes = await fetch('/cloud/v1/usage');
        if (usageRes.ok) {
          const u = await usageRes.json();
          document.getElementById('opsCount').textContent = Number(u.usage.kv_reads + u.usage.kv_writes + u.usage.vector_upserts + u.usage.vector_searches + u.usage.token_operations).toLocaleString();
          document.getElementById('opsLimit').textContent = Number(u.max_operations).toLocaleString();
          document.getElementById('opsPctBadge').textContent = u.ops_percentage + '%';
          document.getElementById('opsProgressBar').style.width = Math.max(u.ops_percentage, 1) + '%';

          document.getElementById('vecsCount').textContent = Number(u.usage.vector_upserts).toLocaleString();
          document.getElementById('vecsLimit').textContent = Number(u.max_vectors).toLocaleString();
          document.getElementById('vecsPctBadge').textContent = u.vectors_percentage + '%';
          document.getElementById('vecsProgressBar').style.width = Math.max(u.vectors_percentage, 1) + '%';

          const mb = (u.usage.storage_bytes / 1048576).toFixed(1);
          document.getElementById('storageCount').textContent = mb + ' MB';
          document.getElementById('storagePctBadge').textContent = u.storage_percentage + '%';
          document.getElementById('storageProgressBar').style.width = Math.max(u.storage_percentage, 1) + '%';

          document.getElementById('tokensCount').textContent = Number(u.usage.token_operations).toLocaleString();
        }

        // 2. Load API Keys
        const keysRes = await fetch('/cloud/v1/keys');
        if (keysRes.ok) {
          const keys = await keysRes.json();
          const tbody = document.getElementById('apiKeysTableBody');
          tbody.innerHTML = '';
          keys.forEach(k => {
            const tr = document.createElement('tr');
            const isRevoked = !!k.revoked_at;
            const statusBadge = isRevoked 
              ? '<span class="px-2 py-0.5 rounded bg-rose-950 text-rose-400 border border-rose-800 text-[10px]">Revoked</span>'
              : '<span class="px-2 py-0.5 rounded bg-emerald-950 text-emerald-400 border border-emerald-800 text-[10px]">Active</span>';

            tr.innerHTML = `
              <td class="py-3 px-3 font-semibold text-white">${k.name}</td>
              <td class="py-3 px-3 text-cyan-300 font-mono text-[11px]">${k.key_preview}</td>
              <td class="py-3 px-3 text-slate-400 text-[11px]">${k.scopes.join(', ')}</td>
              <td class="py-3 px-3">${statusBadge}</td>
              <td class="py-3 px-3 text-right">
                ${isRevoked ? '<span class="text-slate-600 text-xs">Revoked</span>' : `<button onclick="revokeApiKey('${k.id}')" class="text-rose-400 hover:text-rose-300 text-xs underline">Revoke</button>`}
              </td>
            `;
            tbody.appendChild(tr);
          });
          if (keys.length > 0) {
            document.getElementById('codeApiKeyPreview').textContent = `"${keys[0].key_preview}"`;
          }
        }

        // 3. Load Billing
        const billRes = await fetch('/cloud/v1/billing');
        if (billRes.ok) {
          const b = await billRes.json();
          document.getElementById('billBase').textContent = `$${b.base_subscription_usd.toFixed(2)}`;
          document.getElementById('billTotal').textContent = `$${b.estimated_total_usd.toFixed(2)}`;
          document.getElementById('headerPlanBadge').textContent = `${b.plan_tier} Plan ($${b.base_subscription_usd.toFixed(0)}/mo)`;
        }
      } catch (err) {
        console.error('Failed to load cloud state:', err);
      }
    }

    async function createNewApiKey() {
      const name = prompt('Enter a name for the new API Key:', 'Autonomous Agent Fleet ' + new Date().toLocaleDateString());
      if (!name) return;
      const res = await fetch('/cloud/v1/keys', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ project_id: 'proj_live_01', name })
      });
      const data = await res.json();
      alert('Secret API Key Generated:\n\n' + data.raw_token + '\n\nSave this key securely now. It will not be shown in full again.');
      loadCloudState();
    }

    async function revokeApiKey(keyId) {
      if (!confirm('Are you sure you want to revoke this API key? Applications using this key will immediately lose access.')) return;
      await fetch('/cloud/v1/keys/revoke', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key_id: keyId })
      });
      loadCloudState();
    }

    async function updateTier(plan_tier) {
      await fetch('/cloud/v1/plan/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ plan_tier })
      });
      alert('Subscription plan updated to: ' + plan_tier);
      loadCloudState();
    }

    function copyConnectionString() {
      navigator.clipboard.writeText('aether://org_default:proj_live_01@localhost:8301');
      alert('Copied AetherCloud Connection URI to clipboard!');
    }

    loadCloudState();
  </script>
</body>
</html>"#;
