use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncWriteExt;
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
    amount: Option<i64>,
    by: Option<i64>,
    value: Option<i64>,
    val: Option<i64>,
    increment: Option<i64>,
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

#[derive(Debug, Deserialize)]
struct AgentStateSetRequest {
    agent_id: String,
    key: Option<String>,
    state: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct AgentStateGetRequest {
    agent_id: String,
    key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AgentStateDeleteRequest {
    agent_id: String,
    key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AgentStateIncrRequest {
    agent_id: String,
    key: String,
    amount: Option<i64>,
    delta: Option<i64>,
    by: Option<i64>,
    value: Option<i64>,
    val: Option<i64>,
    increment: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct AgentMemoryRecord {
    agent_id: String,
    memory_id: String,
    text: String,
    timestamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct AgentMemoryRememberRequest {
    agent_id: String,
    memory_id: String,
    text: String,
    embedding: Vec<f32>,
    metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct AgentMemoryRecallRequest {
    agent_id: String,
    #[allow(dead_code)]
    query: Option<String>,
    embedding: Vec<f32>,
    top_k: Option<usize>,
}

#[derive(Debug, Serialize)]
struct AgentMemoryRecallItem {
    memory_id: String,
    score: f32,
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct AgentMemoryRecallResponse {
    agent_id: String,
    results: Vec<AgentMemoryRecallItem>,
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
            auth: Arc::new(AuthManager::from_env()),
            rate_limiter: Arc::new(RateLimiter::new(1000.0, 500.0)), // 1000 burst, 500 req/sec refill
        }
    }

    pub fn with_auth(
        addr: SocketAddr,
        node_id: u64,
        storage: Arc<StorageEngine>,
        auth: Arc<AuthManager>,
    ) -> Self {
        Self {
            addr,
            node_id,
            storage,
            telemetry: Arc::new(TelemetryCollector::new()),
            auth,
            rate_limiter: Arc::new(RateLimiter::new(1000.0, 500.0)),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(self.addr)
            .await
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        info!(
            "🌐 AetherDB HTTP REST Gateway listening on http://{}",
            self.addr
        );

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
                if let Err(e) =
                    Self::handle_http(socket, node_id, storage, telemetry, auth, rate_limiter).await
                {
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
        let req = match crate::httpio::read_http_request(&mut socket).await {
            Ok(Some(r)) => r,
            _ => return Ok(()),
        };

        if req.method.is_empty() || req.path.is_empty() {
            return Ok(());
        }

        let method = req.method.as_str();
        let path = req.path.as_str();
        let auth_header: Option<&str> = req.header("authorization");
        let tenant_header: Option<&str> = req.header("x-aether-tenant");
        let body = req.body.as_str();

        // 0. Handle CORS Preflight (OPTIONS)
        if method == "OPTIONS" {
            let resp = "HTTP/1.1 204 No Content\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type, Authorization, X-Aether-Tenant\r\nAccess-Control-Max-Age: 86400\r\nConnection: close\r\n\r\n";
            let _ = socket.write_all(apply_cors(&resp).as_bytes()).await;
            return Ok(());
        }

        // Public Probes & Dashboard (No Auth Required)
        match (method, path) {
            ("GET", "/") | ("GET", "/dashboard") => {
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type, Authorization, X-Aether-Tenant\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    DEVTOOLS_HTML.len(),
                    DEVTOOLS_HTML
                );
                let _ = socket.write_all(apply_cors(&response).as_bytes()).await;
                return Ok(());
            }
            ("GET", "/health") => {
                let body = r#"{"status":"healthy","engine":"aetherdb-rust","version":"0.1.0"}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type, Authorization, X-Aether-Tenant\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(apply_cors(&response).as_bytes()).await;
                return Ok(());
            }
            ("GET", "/readiness") => {
                let body = format!(
                    r#"{{"status":"ready","node_id":{},"ready":true,"engine":"aetherdb-rust","version":"0.1.0"}}"#,
                    node_id
                );
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type, Authorization, X-Aether-Tenant\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(apply_cors(&response).as_bytes()).await;
                return Ok(());
            }
            ("GET", "/metrics")
                if !auth.is_strict()
                    || std::env::var("AETHERDB_PUBLIC_METRICS")
                        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                        .unwrap_or(false) =>
            {
                let text = telemetry.prometheus_text(node_id, 2_400_000, 184_000);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type, Authorization, X-Aether-Tenant\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    text.len(),
                    text
                );
                let _ = socket.write_all(apply_cors(&response).as_bytes()).await;
                return Ok(());
            }
            _ => {}
        }

        let req_start = Instant::now();
        let mut op_tag = "UNKNOWN";
        let mut target_tag = path.to_string();

        // 1. Authenticate Tenant
        let tenant = match auth.authenticate(auth_header, tenant_header) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("auth rejected: {} {} ({})", method, path, e);
                let resp = format!(
                    "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type, Authorization, X-Aether-Tenant\r\nConnection: close\r\n\r\n{{\"error\":\"{}\"}}",
                    e
                );
                let _ = socket.write_all(apply_cors(&resp).as_bytes()).await;
                return Ok(());
            }
        };

        // 2. Check Rate Limit
        if let Err(e) = rate_limiter.check_limit(&tenant.tenant_id) {
            let resp = format!(
                "HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n{{\"error\":\"{}\"}}",
                e
            );
            let _ = socket.write_all(apply_cors(&resp).as_bytes()).await;
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
                    r#"{"status":"healthy","engine":"aetherdb-rust","version":"0.1.0"}"#
                        .to_string(),
                )
            }

            ("GET", "/readiness") => {
                op_tag = "READINESS";
                (
                    200,
                    "application/json",
                    format!(
                        r#"{{"status":"ready","node_id":{},"ready":true,"engine":"aetherdb-rust","version":"0.1.0"}}"#,
                        node_id
                    ),
                )
            }

            ("GET", "/metrics") => {
                op_tag = "METRICS";
                let text = telemetry.prometheus_text(node_id, 2_400_000, 184_000);
                (200, "text/plain; version=0.0.4; charset=utf-8", text)
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
                                (
                                    200,
                                    "application/json",
                                    format!(
                                        r#"{{"found":true,"value":{}}}"#,
                                        serde_json::to_string(&val_str.to_string()).unwrap()
                                    ),
                                )
                            }
                            Ok(_) => (
                                200,
                                "application/json",
                                r#"{"found":false,"value":null}"#.to_string(),
                            ),
                            Err(e) => (500, "application/json", format!(r#"{{"error":"{}"}}"#, e)),
                        }
                    }
                    Err(_) => (
                        400,
                        "application/json",
                        r#"{"error":"Invalid JSON. Expected {key: string}"}"#.to_string(),
                    ),
                }
            }

            ("POST", "/v1/set") => {
                op_tag = "SET";
                match serde_json::from_str::<SetRequest>(body) {
                    Ok(req) => {
                        target_tag = req.key.clone();
                        if req.key.len() > 512 {
                            (
                                400,
                                "application/json",
                                r#"{"error":"Key exceeds 512 bytes maximum limit"}"#.to_string(),
                            )
                        } else {
                            let partitioned = tenant.partition_key(req.key.as_bytes());
                            let state =
                                aether_core::types::ValueState::Some(req.value.into_bytes());
                            match storage.put(partitioned, state) {
                                Ok(_) => {
                                    (200, "application/json", r#"{"status":"ok"}"#.to_string())
                                }
                                Err(e) => {
                                    (500, "application/json", format!(r#"{{"error":"{}"}}"#, e))
                                }
                            }
                        }
                    }
                    Err(_) => (
                        400,
                        "application/json",
                        r#"{"error":"Invalid JSON. Expected {key: string, value: string}"}"#
                            .to_string(),
                    ),
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
                    Err(_) => (
                        400,
                        "application/json",
                        r#"{"error":"Invalid JSON. Expected {key: string}"}"#.to_string(),
                    ),
                }
            }

            ("POST", "/v1/incr") => {
                op_tag = "INCR";
                match serde_json::from_str::<IncrRequest>(body) {
                    Ok(req) => {
                        target_tag = req.key.clone();
                        let partitioned = tenant.partition_key(req.key.as_bytes());
                        let delta = req
                            .amount
                            .or(req.delta)
                            .or(req.by)
                            .or(req.value)
                            .or(req.val)
                            .or(req.increment)
                            .unwrap_or(1);
                        match storage.incr(partitioned, delta) {
                            Ok(new_val) => (
                                200,
                                "application/json",
                                format!(r#"{{"key":"{}","value":{},"new_value":{}}}"#, req.key, new_val, new_val),
                            ),
                            Err(e) => (500, "application/json", format!(r#"{{"error":"{}"}}"#, e)),
                        }
                    }
                    Err(_) => (
                        400,
                        "application/json",
                        r#"{"error":"Invalid JSON. Expected {key: string, amount?: number, delta?: number}"}"#.to_string(),
                    ),
                }
            }

            ("POST", "/v1/vector/upsert") => {
                op_tag = "UPSERT VECTOR";
                match serde_json::from_str::<VectorUpsertRequest>(body) {
                    Ok(req) => {
                        target_tag = req.id.clone();
                        if req.vector.len() > 4096 {
                            (
                                400,
                                "application/json",
                                r#"{"error":"Vector dimension exceeds 4096 maximum limit"}"#
                                    .to_string(),
                            )
                        } else {
                            let partitioned_id = tenant.partition_vector_id(&req.id);
                            match storage.upsert_vector(&partitioned_id, req.vector, req.metadata) {
                                Ok(_) => {
                                    (200, "application/json", r#"{"status":"ok"}"#.to_string())
                                }
                                Err(e) => {
                                    (500, "application/json", format!(r#"{{"error":"{}"}}"#, e))
                                }
                            }
                        }
                    }
                    Err(e) => (
                        400,
                        "application/json",
                        format!(r#"{{"error":"Invalid JSON: {}"}}"#, e),
                    ),
                }
            }

            ("POST", "/v1/vector/search") => {
                op_tag = "VECTOR SEARCH";
                match serde_json::from_str::<VectorSearchRequest>(body) {
                    Ok(req) => {
                        let top_k = req.top_k.unwrap_or(5);
                        target_tag = format!("top_k={}", top_k);
                        let tenant_prefix = format!("t:{}:", tenant.tenant_id);
                        match storage.search_vector_filtered(
                            &req.vector,
                            top_k,
                            Some(&tenant_prefix),
                        ) {
                            Ok(results) => {
                                let formatted: Vec<VectorSearchResult> = results
                                    .into_iter()
                                    .map(|(id, score, metadata)| {
                                        let unpartitioned =
                                            tenant.unpartition_vector_id(&id).to_string();
                                        VectorSearchResult {
                                            id: unpartitioned,
                                            score,
                                            metadata,
                                        }
                                    })
                                    .collect();
                                let json_resp =
                                    serde_json::to_string(&formatted).unwrap_or("[]".to_string());
                                (
                                    200,
                                    "application/json",
                                    format!(r#"{{"results":{}}}"#, json_resp),
                                )
                            }
                            Err(e) => (500, "application/json", format!(r#"{{"error":"{}"}}"#, e)),
                        }
                    }
                    Err(e) => (
                        400,
                        "application/json",
                        format!(r#"{{"error":"Invalid JSON: {}"}}"#, e),
                    ),
                }
            }

            // ==========================================
            // 🤖 AGENT STATE & MEMORY API (PHASE 1)
            // ==========================================
            ("POST", "/v1/agent/state/set") => {
                op_tag = "AGENT STATE SET";
                match serde_json::from_str::<AgentStateSetRequest>(body) {
                    Ok(req) => {
                        if req.agent_id.trim().is_empty() || req.agent_id.len() > 256 {
                            (400, "application/json", r#"{"error":"Invalid agent_id. Must be non-empty string <= 256 chars"}"#.to_string())
                        } else {
                            target_tag = format!("agent:{}", req.agent_id);
                            let raw_key = match &req.key {
                                Some(k) if !k.trim().is_empty() => {
                                    format!("__agent_state:{}:{}", req.agent_id, k.trim())
                                }
                                _ => format!("__agent_state:{}:root", req.agent_id),
                            };
                            let partitioned = tenant.partition_key(raw_key.as_bytes());
                            match serde_json::to_vec(&req.state) {
                                Ok(state_bytes) => {
                                    match storage.put(
                                        partitioned,
                                        aether_core::types::ValueState::Some(state_bytes),
                                    ) {
                                        Ok(_) => (
                                            200,
                                            "application/json",
                                            format!(
                                                r#"{{"status":"ok","agent_id":{}}}"#,
                                                serde_json::to_string(&req.agent_id).unwrap()
                                            ),
                                        ),
                                        Err(e) => (
                                            500,
                                            "application/json",
                                            format!(r#"{{"error":"{}"}}"#, e),
                                        ),
                                    }
                                }
                                Err(e) => (
                                    400,
                                    "application/json",
                                    format!(r#"{{"error":"Failed to serialize state: {}"}}"#, e),
                                ),
                            }
                        }
                    }
                    Err(e) => (
                        400,
                        "application/json",
                        format!(
                            r#"{{"error":"Invalid JSON. Expected {{agent_id: string, state: any}}: {}"}}"#,
                            e
                        ),
                    ),
                }
            }

            ("POST", "/v1/agent/state/get") => {
                op_tag = "AGENT STATE GET";
                match serde_json::from_str::<AgentStateGetRequest>(body) {
                    Ok(req) => {
                        if req.agent_id.trim().is_empty() || req.agent_id.len() > 256 {
                            (400, "application/json", r#"{"error":"Invalid agent_id. Must be non-empty string <= 256 chars"}"#.to_string())
                        } else {
                            target_tag = format!("agent:{}", req.agent_id);
                            let raw_key = match &req.key {
                                Some(k) if !k.trim().is_empty() => {
                                    format!("__agent_state:{}:{}", req.agent_id, k.trim())
                                }
                                _ => format!("__agent_state:{}:root", req.agent_id),
                            };
                            let partitioned = tenant.partition_key(raw_key.as_bytes());
                            match storage.get(&partitioned) {
                                Ok(Some(aether_core::types::ValueState::Some(bytes))) => {
                                    let parsed_val: serde_json::Value =
                                        serde_json::from_slice(&bytes).unwrap_or_else(|_| {
                                            serde_json::Value::String(
                                                String::from_utf8_lossy(&bytes).to_string(),
                                            )
                                        });
                                    (
                                        200,
                                        "application/json",
                                        format!(
                                            r#"{{"found":true,"agent_id":{},"state":{}}}"#,
                                            serde_json::to_string(&req.agent_id).unwrap(),
                                            serde_json::to_string(&parsed_val).unwrap()
                                        ),
                                    )
                                }
                                Ok(_) => (
                                    200,
                                    "application/json",
                                    format!(
                                        r#"{{"found":false,"agent_id":{},"state":null}}"#,
                                        serde_json::to_string(&req.agent_id).unwrap()
                                    ),
                                ),
                                Err(e) => {
                                    (500, "application/json", format!(r#"{{"error":"{}"}}"#, e))
                                }
                            }
                        }
                    }
                    Err(e) => (
                        400,
                        "application/json",
                        format!(
                            r#"{{"error":"Invalid JSON. Expected {{agent_id: string, key?: string}}: {}"}}"#,
                            e
                        ),
                    ),
                }
            }

            ("POST", "/v1/agent/state/delete") | ("POST", "/v1/agent/state/del") => {
                op_tag = "AGENT STATE DEL";
                match serde_json::from_str::<AgentStateDeleteRequest>(body) {
                    Ok(req) => {
                        if req.agent_id.trim().is_empty() || req.agent_id.len() > 256 {
                            (400, "application/json", r#"{"error":"Invalid agent_id. Must be non-empty string <= 256 chars"}"#.to_string())
                        } else {
                            target_tag = format!("agent:{}", req.agent_id);
                            let raw_key = match &req.key {
                                Some(k) if !k.trim().is_empty() => {
                                    format!("__agent_state:{}:{}", req.agent_id, k.trim())
                                }
                                _ => format!("__agent_state:{}:root", req.agent_id),
                            };
                            let partitioned = tenant.partition_key(raw_key.as_bytes());
                            match storage
                                .put(partitioned, aether_core::types::ValueState::Tombstone)
                            {
                                Ok(_) => (
                                    200,
                                    "application/json",
                                    format!(
                                        r#"{{"status":"ok","deleted":true,"agent_id":{}}}"#,
                                        serde_json::to_string(&req.agent_id).unwrap()
                                    ),
                                ),
                                Err(e) => {
                                    (500, "application/json", format!(r#"{{"error":"{}"}}"#, e))
                                }
                            }
                        }
                    }
                    Err(e) => (
                        400,
                        "application/json",
                        format!(
                            r#"{{"error":"Invalid JSON. Expected {{agent_id: string, key?: string}}: {}"}}"#,
                            e
                        ),
                    ),
                }
            }

            ("POST", "/v1/agent/state/incr") => {
                op_tag = "AGENT STATE INCR";
                match serde_json::from_str::<AgentStateIncrRequest>(body) {
                    Ok(req) => {
                        if req.agent_id.trim().is_empty() || req.agent_id.len() > 256 {
                            (400, "application/json", r#"{"error":"Invalid agent_id. Must be non-empty string <= 256 chars"}"#.to_string())
                        } else if req.key.trim().is_empty() || req.key.len() > 256 {
                            (
                                400,
                                "application/json",
                                r#"{"error":"Invalid key. Must be non-empty string <= 256 chars"}"#
                                    .to_string(),
                            )
                        } else {
                            target_tag = format!("agent:{}:{}", req.agent_id, req.key);
                            let raw_key =
                                format!("__agent_state:{}:{}", req.agent_id, req.key.trim());
                            let partitioned = tenant.partition_key(raw_key.as_bytes());
                            let delta = req
                                .amount
                                .or(req.delta)
                                .or(req.by)
                                .or(req.value)
                                .or(req.val)
                                .or(req.increment)
                                .unwrap_or(1);
                            match storage.incr(partitioned, delta) {
                                Ok(new_val) => (
                                    200,
                                    "application/json",
                                    format!(
                                        r#"{{"status":"ok","agent_id":{},"key":{},"value":{},"new_value":{}}}"#,
                                        serde_json::to_string(&req.agent_id).unwrap(),
                                        serde_json::to_string(&req.key).unwrap(),
                                        new_val,
                                        new_val
                                    ),
                                ),
                                Err(e) => {
                                    (500, "application/json", format!(r#"{{"error":"{}"}}"#, e))
                                }
                            }
                        }
                    }
                    Err(e) => (
                        400,
                        "application/json",
                        format!(
                            r#"{{"error":"Invalid JSON. Expected {{agent_id: string, key: string, amount?: number}}: {}"}}"#,
                            e
                        ),
                    ),
                }
            }

            ("POST", "/v1/agent/memory/remember") => {
                op_tag = "AGENT REMEMBER";
                match serde_json::from_str::<AgentMemoryRememberRequest>(body) {
                    Ok(req) => {
                        if req.agent_id.trim().is_empty() || req.agent_id.len() > 256 {
                            (400, "application/json", r#"{"error":"Invalid agent_id. Must be non-empty string <= 256 chars"}"#.to_string())
                        } else if req.memory_id.trim().is_empty() || req.memory_id.len() > 256 {
                            (400, "application/json", r#"{"error":"Invalid memory_id. Must be non-empty string <= 256 chars"}"#.to_string())
                        } else if req.embedding.is_empty() || req.embedding.len() > 4096 {
                            (400, "application/json", r#"{"error":"Invalid embedding dimensions. Must have between 1 and 4096 elements"}"#.to_string())
                        } else {
                            target_tag = format!("agent:{}:{}", req.agent_id, req.memory_id);
                            let now_ts = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_millis() as u64)
                                .unwrap_or(0);
                            let record = AgentMemoryRecord {
                                agent_id: req.agent_id.clone(),
                                memory_id: req.memory_id.clone(),
                                text: req.text,
                                timestamp: now_ts,
                                metadata: req.metadata,
                            };
                            match serde_json::to_string(&record) {
                                Ok(record_json) => {
                                    // 1. Store structured metadata in KV
                                    let kv_raw =
                                        format!("__agent_mem:{}:{}", req.agent_id, req.memory_id);
                                    let kv_partitioned = tenant.partition_key(kv_raw.as_bytes());
                                    if let Err(e) = storage.put(
                                        kv_partitioned,
                                        aether_core::types::ValueState::Some(
                                            record_json.clone().into_bytes(),
                                        ),
                                    ) {
                                        (
                                            500,
                                            "application/json",
                                            format!(
                                                r#"{{"error":"Failed to store memory metadata: {}"}}"#,
                                                e
                                            ),
                                        )
                                    } else {
                                        // 2. Store vector embedding in HNSW / SIMD vector subsystem
                                        let vec_raw =
                                            format!("agent:{}:{}", req.agent_id, req.memory_id);
                                        let vec_partitioned = tenant.partition_vector_id(&vec_raw);
                                        match storage.upsert_vector(
                                            &vec_partitioned,
                                            req.embedding,
                                            Some(record_json),
                                        ) {
                                            Ok(_) => (
                                                200,
                                                "application/json",
                                                format!(
                                                    r#"{{"status":"ok","agent_id":{},"memory_id":{}}}"#,
                                                    serde_json::to_string(&req.agent_id).unwrap(),
                                                    serde_json::to_string(&req.memory_id).unwrap()
                                                ),
                                            ),
                                            Err(e) => (
                                                500,
                                                "application/json",
                                                format!(
                                                    r#"{{"error":"Failed to upsert vector: {}"}}"#,
                                                    e
                                                ),
                                            ),
                                        }
                                    }
                                }
                                Err(e) => (
                                    400,
                                    "application/json",
                                    format!(
                                        r#"{{"error":"Failed to serialize memory record: {}"}}"#,
                                        e
                                    ),
                                ),
                            }
                        }
                    }
                    Err(e) => (
                        400,
                        "application/json",
                        format!(r#"{{"error":"Invalid JSON: {}"}}"#, e),
                    ),
                }
            }

            ("POST", "/v1/agent/memory/recall") => {
                op_tag = "AGENT RECALL";
                match serde_json::from_str::<AgentMemoryRecallRequest>(body) {
                    Ok(req) => {
                        if req.agent_id.trim().is_empty() || req.agent_id.len() > 256 {
                            (400, "application/json", r#"{"error":"Invalid agent_id. Must be non-empty string <= 256 chars"}"#.to_string())
                        } else if req.embedding.is_empty() || req.embedding.len() > 4096 {
                            (400, "application/json", r#"{"error":"Invalid embedding dimensions. Must have between 1 and 4096 elements"}"#.to_string())
                        } else {
                            let top_k = req.top_k.unwrap_or(5).clamp(1, 100);
                            target_tag = format!("agent:{}:top_k={}", req.agent_id, top_k);
                            // Search with strict tenant & agent prefix filter
                            let search_prefix =
                                format!("t:{}:agent:{}:", tenant.tenant_id, req.agent_id);
                            match storage.search_vector_filtered(
                                &req.embedding,
                                top_k,
                                Some(&search_prefix),
                            ) {
                                Ok(results) => {
                                    let mut recall_items = Vec::new();
                                    let agent_id_prefix = format!("agent:{}:", req.agent_id);
                                    for (raw_vec_id, score, meta_str_opt) in results {
                                        let unpartitioned =
                                            tenant.unpartition_vector_id(&raw_vec_id);
                                        let memory_id =
                                            if unpartitioned.starts_with(&agent_id_prefix) {
                                                &unpartitioned[agent_id_prefix.len()..]
                                            } else {
                                                unpartitioned
                                            };

                                        // Look up canonical KV metadata for this memory
                                        let kv_raw =
                                            format!("__agent_mem:{}:{}", req.agent_id, memory_id);
                                        let kv_partitioned =
                                            tenant.partition_key(kv_raw.as_bytes());
                                        let (text, metadata) = match storage.get(&kv_partitioned) {
                                            Ok(Some(aether_core::types::ValueState::Some(
                                                bytes,
                                            ))) => {
                                                if let Ok(rec) =
                                                    serde_json::from_slice::<AgentMemoryRecord>(
                                                        &bytes,
                                                    )
                                                {
                                                    (rec.text, rec.metadata)
                                                } else {
                                                    (
                                                        String::from_utf8_lossy(&bytes).to_string(),
                                                        None,
                                                    )
                                                }
                                            }
                                            _ => {
                                                if let Some(ref m_str) = meta_str_opt {
                                                    if let Ok(rec) =
                                                        serde_json::from_str::<AgentMemoryRecord>(
                                                            m_str,
                                                        )
                                                    {
                                                        (rec.text, rec.metadata)
                                                    } else {
                                                        (String::new(), None)
                                                    }
                                                } else {
                                                    (String::new(), None)
                                                }
                                            }
                                        };

                                        recall_items.push(AgentMemoryRecallItem {
                                            memory_id: memory_id.to_string(),
                                            score,
                                            text,
                                            metadata,
                                        });
                                    }

                                    let resp_obj = AgentMemoryRecallResponse {
                                        agent_id: req.agent_id,
                                        results: recall_items,
                                    };
                                    let json_resp = serde_json::to_string(&resp_obj)
                                        .unwrap_or("{}".to_string());
                                    (200, "application/json", json_resp)
                                }
                                Err(e) => {
                                    (500, "application/json", format!(r#"{{"error":"{}"}}"#, e))
                                }
                            }
                        }
                    }
                    Err(e) => (
                        400,
                        "application/json",
                        format!(r#"{{"error":"Invalid JSON: {}"}}"#, e),
                    ),
                }
            }

            _ => (
                404,
                "application/json",
                r#"{"error":"Endpoint not found"}"#.to_string(),
            ),
        };

        // Record telemetry latency and activity record
        let elapsed_ms = req_start.elapsed().as_secs_f64() * 1000.0;
        if op_tag != "DASHBOARD" && op_tag != "TELEMETRY" {
            telemetry.record_request(
                op_tag,
                &target_tag,
                elapsed_ms,
                status_code,
                &tenant.tenant_id,
            );
        }

        let reason = match status_code {
            200 => "OK",
            201 => "Created",
            204 => "No Content",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            429 => "Too Many Requests",
            500 => "Internal Server Error",
            _ => "OK",
        };

        let response = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type, Authorization, X-Aether-Tenant\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            status_code,
            reason,
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

/// Applies the deployment CORS policy to a raw HTTP response.
/// - `AETHERDB_CORS_ORIGIN=<origin>`: only that origin is allowed.
/// - unset + strict auth: no ACAO header (same-origin only, e.g. bundled dashboard).
/// - unset + permissive dev mode: `*` (local development convenience).
fn apply_cors(resp: &str) -> String {
    use std::sync::OnceLock;
    static POLICY: OnceLock<Option<String>> = OnceLock::new();
    let policy = POLICY.get_or_init(|| {
        if let Ok(o) = std::env::var("AETHERDB_CORS_ORIGIN") {
            let o = o.trim().to_string();
            if !o.is_empty() {
                return Some(format!(
                    "Access-Control-Allow-Origin: {}\r\nVary: Origin",
                    o
                ));
            }
        }
        let strict = std::env::var("AETHERDB_REQUIRE_AUTH")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if strict {
            None
        } else {
            Some("Access-Control-Allow-Origin: *".to_string())
        }
    });
    let wildcard = "Access-Control-Allow-Origin: *\r\n";
    match policy {
        Some(line) => resp.replacen(wildcard, &format!("{}\r\n", line), 1),
        None => resp.replacen(wildcard, "", 1),
    }
}

const DEVTOOLS_HTML: &str = r##"<!DOCTYPE html>
<html lang="en" class="h-full bg-[#080c14]">
<head>
  <meta charset="UTF-8" />
  <title>AetherDB Console • AI Distributed State & Memory Engine</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; background-color: #080c14; color: #f1f5f9; }
    code, pre, .mono { font-family: 'JetBrains Mono', monospace; }
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #080c14; }
    ::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: #334155; }
    .nav-active { background-color: #0f172a; border-left: 3px solid #38bdf8; color: #ffffff; }
    .nav-inactive { border-left: 3px solid transparent; color: #94a3b8; }
    .nav-inactive:hover { background-color: #0d1424; color: #e2e8f0; }
    .card-panel { background-color: #0e1422; border: 1px solid rgba(30, 41, 59, 0.85); transition: border-color 0.2s ease; }
    .card-panel:hover { border-color: rgba(56, 189, 248, 0.3); }
    @keyframes subtle-pulse {
      0%, 100% { opacity: 1; }
      50% { opacity: 0.4; }
    }
    .status-pulse { animation: subtle-pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite; }
  </style>
</head>
<body class="h-full flex flex-col antialiased selection:bg-cyan-900 selection:text-white overflow-hidden">
  
  <!-- ==================================================================== -->
  <!-- TOP APP HEADER -->
  <!-- ==================================================================== -->
  <header class="h-14 border-b border-slate-800/80 bg-[#0b0f19] px-5 flex items-center justify-between z-30 shrink-0">
    <div class="flex items-center gap-4">
      <div class="flex items-center gap-3">
        <div class="h-8 w-8 rounded bg-slate-900 border border-slate-700/80 flex items-center justify-center p-1.5 shadow-sm">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" class="w-full h-full text-cyan-400">
            <path stroke-linecap="round" stroke-linejoin="round" d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5" />
          </svg>
        </div>
        <div>
          <div class="flex items-center gap-2">
            <span class="font-bold text-white tracking-tight text-sm">AetherDB</span>
            <span class="text-[10px] font-semibold px-1.5 py-0.5 rounded bg-cyan-950/80 text-cyan-400 border border-cyan-800/80 mono">v0.1.0-rust</span>
          </div>
          <p class="text-[10px] text-slate-400 font-medium">AI-native persistent state & memory infrastructure</p>
        </div>
      </div>
    </div>

    <!-- Cluster Status, Endpoint & Connection Badges -->
    <div class="flex items-center gap-3 text-xs mono">
      <div class="hidden md:flex items-center gap-2 px-2.5 py-1 rounded bg-slate-900 border border-slate-800 text-slate-400 text-[11px]">
        <span class="text-slate-500">ENDPOINT:</span>
        <span id="headerEndpoint" class="text-slate-200">http://127.0.0.1:8301</span>
      </div>
      <div class="hidden sm:flex items-center gap-2 px-2.5 py-1 rounded bg-slate-900 border border-slate-800 text-slate-400 text-[11px]">
        <span class="text-slate-500">ENGINE:</span>
        <span class="text-slate-200">LSM-Tree + HNSW SIMD</span>
      </div>
      <div id="connectionStatusBadge" class="flex items-center gap-2 px-3 py-1 rounded bg-emerald-950/70 border border-emerald-800/80 text-emerald-400 text-[11px] font-medium transition-colors">
        <span id="connectionDot" class="h-2 w-2 rounded-full bg-emerald-400 status-pulse"></span>
        <span id="connectionText">● Connected</span>
      </div>
    </div>
  </header>

  <!-- ==================================================================== -->
  <!-- MAIN LAYOUT WITH PERSISTENT SIDEBAR & CONTENT WORKSPACE -->
  <!-- ==================================================================== -->
  <div class="flex-1 flex overflow-hidden">
    
    <!-- Left Navigation Sidebar -->
    <aside class="w-60 border-r border-slate-800/80 bg-[#090d16] flex flex-col justify-between shrink-0">
      <div class="py-3">
        <div class="px-4 py-1.5 text-[10px] font-bold uppercase tracking-wider text-slate-400">Core Console</div>
        <nav class="space-y-0.5 px-2 text-xs font-medium">
          <a href="#overview" onclick="switchTab('overview')" id="nav-overview" class="nav-active flex items-center gap-2.5 px-3 py-2 rounded transition">
            <svg class="w-4 h-4 text-cyan-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z"/></svg>
            Overview
          </a>
          <a href="#agents" onclick="switchTab('agents')" id="nav-agents" class="nav-inactive flex items-center gap-2.5 px-3 py-2 rounded transition">
            <svg class="w-4 h-4 text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"/></svg>
            AI Agents
          </a>
          <a href="#memory" onclick="switchTab('memory')" id="nav-memory" class="nav-inactive flex items-center gap-2.5 px-3 py-2 rounded transition">
            <svg class="w-4 h-4 text-indigo-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z"/></svg>
            Semantic Memory
          </a>
          <a href="#state" onclick="switchTab('state')" id="nav-state" class="nav-inactive flex items-center gap-2.5 px-3 py-2 rounded transition">
            <svg class="w-4 h-4 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2 1.5 3 3.5 3h9c2 0 3.5-1 3.5-3V7c0-2-1.5-3-3.5-3h-9C5.5 4 4 5 4 7zm0 5h16"/></svg>
            State & KV
          </a>
          <a href="#vectors" onclick="switchTab('vectors')" id="nav-vectors" class="nav-inactive flex items-center gap-2.5 px-3 py-2 rounded transition">
            <svg class="w-4 h-4 text-pink-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
            SIMD Vectors
          </a>
        </nav>

        <div class="px-4 py-2 mt-4 text-[10px] font-bold uppercase tracking-wider text-slate-400">Infrastructure</div>
        <nav class="space-y-0.5 px-2 text-xs font-medium">
          <a href="#transactions" onclick="switchTab('transactions')" id="nav-transactions" class="nav-inactive flex items-center gap-2.5 px-3 py-2 rounded transition">
            <svg class="w-4 h-4 text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"/></svg>
            MVCC & Transactions
          </a>
          <a href="#cluster" onclick="switchTab('cluster')" id="nav-cluster" class="nav-inactive flex items-center gap-2.5 px-3 py-2 rounded transition">
            <svg class="w-4 h-4 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"/></svg>
            Cluster & Observability
          </a>
          <a href="#activity" onclick="switchTab('activity')" id="nav-activity" class="nav-inactive flex items-center gap-2.5 px-3 py-2 rounded transition">
            <svg class="w-4 h-4 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6"/></svg>
            Live Activity
          </a>
          <a href="#developer" onclick="switchTab('developer')" id="nav-developer" class="nav-inactive flex items-center gap-2.5 px-3 py-2 rounded transition">
            <svg class="w-4 h-4 text-pink-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4"/></svg>
            Developer Playground
          </a>
        </nav>
      </div>

      <!-- Sidebar Footer Telemetry Summary -->
      <div class="p-3 border-t border-slate-800/80 bg-[#070b12] text-[11px] mono space-y-1.5 text-slate-400">
        <div class="flex justify-between items-center">
          <span class="text-slate-400">Active Node:</span>
          <span class="text-white font-semibold" id="sideNodeId">Node 1</span>
        </div>
        <div class="flex justify-between items-center">
          <span class="text-slate-400">Raft Term:</span>
          <span class="text-cyan-400" id="sideRaftTerm">1</span>
        </div>
        <div class="flex justify-between items-center">
          <span class="text-slate-400">MemTable:</span>
          <span class="text-emerald-400" id="sideMemTable">Online</span>
        </div>
      </div>
    </aside>

    <!-- Content Workspace -->
    <main class="flex-1 overflow-y-auto bg-[#080c14] p-6">

      <!-- ==================================================================== -->
      <!-- VIEW 1: OVERVIEW (CONTROL PLANE) -->
      <!-- ==================================================================== -->
      <section id="view-overview" class="space-y-6">
        
        <!-- Header Title & Telemetry Status -->
        <div class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2 border-b border-slate-800/80 pb-3">
          <div>
            <h1 class="text-lg font-bold text-white tracking-tight">System Control Plane</h1>
            <p class="text-xs text-slate-400">Live operational telemetry across distributed nodes, agent state, and vector memory.</p>
          </div>
          <div class="flex items-center gap-2 text-[11px] mono text-slate-400">
            <span class="h-1.5 w-1.5 rounded-full bg-cyan-400"></span>
            <span>Telemetry: 1000ms poll</span>
            <span class="text-slate-600">|</span>
            <span>Last sync: <strong id="lastSyncTime" class="text-slate-300 font-normal">--:--:--</strong></span>
          </div>
        </div>

        <!-- 6 Primary Real Metric Cards -->
        <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-3.5">
          
          <!-- Card 1: Cluster Health -->
          <div class="card-panel rounded-lg p-3.5 shadow-sm">
            <div class="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Cluster Health</div>
            <div class="mt-1 flex items-baseline gap-1.5">
              <span class="text-xl font-bold text-emerald-400 mono" id="statClusterHealth">HEALTHY</span>
            </div>
            <div class="mt-1.5 text-[10px] text-slate-400 mono flex items-center justify-between">
              <span>Active Nodes:</span>
              <span id="statClusterNodes" class="text-slate-200 font-medium">3 / 3 online</span>
            </div>
          </div>

          <!-- Card 2: Discovered Agent Fleet -->
          <div class="card-panel rounded-lg p-3.5 shadow-sm">
            <div class="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Agent Namespaces</div>
            <div class="mt-1 flex items-baseline gap-1.5">
              <span class="text-xl font-bold text-purple-400 mono" id="statAgents">1</span>
              <span class="text-[11px] text-purple-300 mono">discovered</span>
            </div>
            <div class="mt-1.5 text-[10px] text-slate-400 mono flex items-center justify-between">
              <span id="statAgentBreakdown" class="text-slate-300">1 App · 0 Test</span>
              <span class="text-purple-300 font-medium">Namespaced</span>
            </div>
          </div>

          <!-- Card 3: Memory / Vectors -->
          <div class="card-panel rounded-lg p-3.5 shadow-sm">
            <div class="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Vector Memories</div>
            <div class="mt-1 flex items-baseline gap-1.5">
              <span class="text-xl font-bold text-indigo-400 mono" id="statVectors">0</span>
              <span class="text-[11px] text-indigo-300 mono">indexed</span>
            </div>
            <div class="mt-1.5 text-[10px] text-slate-400 mono flex items-center justify-between">
              <span>SIMD Index:</span>
              <span class="text-indigo-300 font-medium">AVX2 Graph</span>
            </div>
          </div>

          <!-- Card 4: Request Activity Throughput -->
          <div class="card-panel rounded-lg p-3.5 shadow-sm">
            <div class="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Request Rate</div>
            <div class="mt-1 flex items-baseline gap-1.5">
              <span class="text-xl font-bold text-cyan-400 mono" id="statRps">0.0</span>
              <span class="text-[11px] text-cyan-300 mono">ops/sec</span>
            </div>
            <div class="mt-1.5 text-[10px] text-slate-400 mono flex items-center justify-between">
              <span>Total Reqs:</span>
              <span id="statTotalReqs" class="text-cyan-300 font-medium">0</span>
            </div>
          </div>

          <!-- Card 5: Median & Tail Latency -->
          <div class="card-panel rounded-lg p-3.5 shadow-sm">
            <div class="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Median Latency</div>
            <div class="mt-1 flex items-baseline gap-1.5">
              <span class="text-xl font-bold text-emerald-400 mono" id="statP50">0.03</span>
              <span class="text-[11px] text-emerald-300 mono">ms (p50)</span>
            </div>
            <div class="mt-1.5 text-[10px] text-slate-400 mono flex items-center justify-between">
              <span>Tail p99:</span>
              <span id="statP99" class="text-amber-400 font-medium">0.14 ms</span>
            </div>
          </div>

          <!-- Card 6: Storage Footprint -->
          <div class="card-panel rounded-lg p-3.5 shadow-sm">
            <div class="text-[10px] font-semibold text-slate-400 uppercase tracking-wider">Storage Footprint</div>
            <div class="mt-1 flex items-baseline gap-1.5">
              <span class="text-xl font-bold text-slate-100 mono" id="statStorageSize">2.4 MB</span>
            </div>
            <div class="mt-1.5 text-[10px] text-slate-400 mono flex items-center justify-between">
              <span>WAL Buffer:</span>
              <span id="statWalSize" class="text-slate-300 font-medium">184 KB</span>
            </div>
          </div>

        </div>

        <!-- 2-Column Operational Grid -->
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-5">
          
          <!-- Left 2 Cols: Real Agent Operations & Live Activity Table -->
          <div class="lg:col-span-2 card-panel rounded-lg p-4 shadow-sm space-y-3">
            <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-800/80 pb-2.5">
              <div class="flex items-center gap-2">
                <span class="h-2 w-2 rounded-full bg-emerald-400 status-pulse"></span>
                <h3 class="text-xs font-semibold uppercase tracking-wider text-slate-200">Recent Agent & Gateway Activity</h3>
              </div>
              <span class="text-[10px] mono text-slate-400">Live operational execution traces</span>
            </div>

            <div class="overflow-x-auto">
              <table class="w-full text-left text-xs mono">
                <thead>
                  <tr class="border-b border-slate-800 text-slate-400 text-[10px] uppercase">
                    <th class="py-2 px-2.5">Time</th>
                    <th class="py-2 px-2.5">Agent / Namespace</th>
                    <th class="py-2 px-2.5">Operation</th>
                    <th class="py-2 px-2.5">Target / Resource</th>
                    <th class="py-2 px-2.5">Latency</th>
                    <th class="py-2 px-2.5">Status</th>
                  </tr>
                </thead>
                <tbody id="overviewActivityBody" class="divide-y divide-slate-800/50 text-[11px]">
                  <tr>
                    <td colspan="6" class="py-6 px-3 text-center text-slate-400">
                      <div class="flex flex-col items-center justify-center gap-1">
                        <span class="text-slate-500">Listening for incoming client & agent operations...</span>
                        <span class="text-[10px] text-slate-600">Send an agent state or recall request to see real-time activity</span>
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          <!-- Right Col: Cluster Consensus & Memory Subsystem Details -->
          <div class="space-y-4">
            
            <!-- Cluster Consensus Card -->
            <div class="card-panel rounded-lg p-4 shadow-sm space-y-3">
              <div class="flex items-center justify-between border-b border-slate-800/80 pb-2">
                <h3 class="text-xs font-semibold uppercase tracking-wider text-cyan-400 flex items-center gap-1.5">
                  <svg class="w-3.5 h-3.5 text-cyan-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"/></svg>
                  Cluster Health & Multi-Raft
                </h3>
                <span class="text-[10px] mono text-emerald-400 px-1.5 py-0.5 rounded bg-emerald-950/60 border border-emerald-800/80">3 Nodes</span>
              </div>
              <div class="space-y-2 text-xs mono">
                <div class="flex justify-between py-1 border-b border-slate-800/50">
                  <span class="text-slate-400">Leader Node:</span>
                  <span class="text-white font-medium" id="statLeaderNode">Node 1 (Local)</span>
                </div>
                <div class="flex justify-between py-1 border-b border-slate-800/50">
                  <span class="text-slate-400">Followers:</span>
                  <span class="text-slate-300 font-medium">Node 2, Node 3</span>
                </div>
                <div class="flex justify-between py-1 border-b border-slate-800/50">
                  <span class="text-slate-400">Raft Term:</span>
                  <span class="text-cyan-400 font-semibold" id="statRaftTerm">1</span>
                </div>
                <div class="flex justify-between py-1 border-b border-slate-800/50">
                  <span class="text-slate-400">Commit Index:</span>
                  <span class="text-cyan-400 font-semibold" id="statCommitIndex">12,849</span>
                </div>
                <div class="flex justify-between py-1 border-b border-slate-800/50">
                  <span class="text-slate-400">Replication Lag:</span>
                  <span class="text-emerald-400 font-medium" id="statReplLag">0 ms</span>
                </div>
                <div class="flex justify-between py-1">
                  <span class="text-slate-400">Clock Protocol:</span>
                  <span class="text-slate-200">HLC (Snapshot Isolation)</span>
                </div>
              </div>
            </div>

            <!-- Semantic Memory Subsystem Card -->
            <div class="card-panel rounded-lg p-4 shadow-sm space-y-3">
              <div class="flex items-center justify-between border-b border-slate-800/80 pb-2">
                <h3 class="text-xs font-semibold uppercase tracking-wider text-indigo-400 flex items-center gap-1.5">
                  <svg class="w-3.5 h-3.5 text-indigo-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19.428 15.428a2 2 0 00-1.022-.547l-2.387-.477a6 6 0 00-3.86.517l-.318.158a6 6 0 01-3.86.517L6.05 15.21a2 2 0 00-1.806.547M8 4h8l-1 1v5.172a2 2 0 00.586 1.414l5 5c1.26 1.26.367 3.414-1.415 3.414H4.828c-1.782 0-2.674-2.154-1.414-3.414l5-5A2 2 0 009 10.172V5L8 4z"/></svg>
                  Semantic Memory Activity
                </h3>
                <span class="text-[10px] mono text-indigo-400 px-1.5 py-0.5 rounded bg-indigo-950/60 border border-indigo-800/80">SIMD Cosine</span>
              </div>
              <div class="space-y-2 text-xs mono">
                <div class="flex justify-between py-1 border-b border-slate-800/50">
                  <span class="text-slate-400">Memories Stored:</span>
                  <span class="text-indigo-300 font-semibold" id="statMemoriesStored">0</span>
                </div>
                <div class="flex justify-between py-1 border-b border-slate-800/50">
                  <span class="text-slate-400">Token Operations:</span>
                  <span class="text-purple-400 font-semibold" id="statTokenOps">0</span>
                </div>
                <div class="flex justify-between py-1 border-b border-slate-800/50">
                  <span class="text-slate-400">Vector Search Latency:</span>
                  <span class="text-emerald-400 font-medium" id="statVecLatency">2.15 ms</span>
                </div>
                <div class="flex justify-between py-1 border-b border-slate-800/50">
                  <span class="text-slate-400">Max Dimensions:</span>
                  <span class="text-slate-300">4,096 elements</span>
                </div>
                <div class="flex justify-between py-1">
                  <span class="text-slate-400">Memory Partitioning:</span>
                  <span class="text-slate-200">Strict Agent Scoped</span>
                </div>
              </div>
            </div>

          </div>
        </div>
      </section>

      <!-- ==================================================================== -->
      <!-- VIEW 2: AI AGENTS FLEET & CONTROL PLANE -->
      <!-- ==================================================================== -->
      <section id="view-agents" class="hidden space-y-6">
        
        <!-- Header Banner & Agent Discovery Bar -->
        <div class="border-b border-slate-800/80 pb-3 flex flex-col md:flex-row md:items-center justify-between gap-3">
          <div>
            <div class="flex items-center gap-2">
              <h1 class="text-lg font-bold text-white tracking-tight">Autonomous AI Agents</h1>
              <span id="agentsCountBadge" class="text-[11px] font-semibold px-2 py-0.5 rounded bg-purple-950/80 text-purple-300 border border-purple-800/80 mono">1 Discovered</span>
              <span id="agentsSubCountBadge" class="text-[11px] text-slate-400 mono">(1 Application, 0 Test)</span>
            </div>
            <p class="text-xs text-slate-400 mt-0.5">Persistent state and semantic memory infrastructure for autonomous AI agents.</p>
          </div>
          
          <!-- Discovery Search / Quick Inspect Action Bar -->
          <div class="flex items-center gap-2">
            <div class="relative">
              <input id="agentSearchInput" type="text" placeholder="Inspect Agent ID (e.g. research-agent)" value="research-agent" class="w-64 mono text-xs px-3 py-1.5 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-purple-500 placeholder-slate-500" />
            </div>
            <button onclick="handleInspectAgentFromInput()" class="bg-purple-600 hover:bg-purple-500 text-white text-xs font-semibold px-3 py-1.5 rounded transition mono flex items-center gap-1.5">
              <span>Inspect Agent</span>
              <span>→</span>
            </button>
          </div>
        </div>

        <!-- Agent Fleet List Container -->
        <div id="agentsFleetListContainer" class="space-y-4">
          <div class="card-panel rounded-lg overflow-hidden shadow-sm">
            <div class="px-4 py-3 border-b border-slate-800/80 flex flex-col sm:flex-row sm:items-center justify-between gap-2 bg-slate-950/40">
              <div class="flex items-center gap-2">
                <span class="h-2 w-2 rounded-full bg-purple-400 status-pulse"></span>
                <h3 class="text-xs font-semibold uppercase tracking-wider text-slate-200">Discovered Agent Fleet</h3>
              </div>
              
              <!-- Lightweight Classification Filter: All | Application | Test -->
              <div class="flex items-center gap-1 p-0.5 bg-slate-900 border border-slate-800 rounded text-xs mono">
                <button onclick="setAgentFleetFilter('all')" id="filterBtn-all" class="px-2.5 py-1 rounded bg-purple-600 text-white font-medium transition text-[11px]">All (<span id="filterCount-all">0</span>)</button>
                <button onclick="setAgentFleetFilter('app')" id="filterBtn-app" class="px-2.5 py-1 rounded text-slate-400 hover:text-slate-200 transition text-[11px]">Application (<span id="filterCount-app">0</span>)</button>
                <button onclick="setAgentFleetFilter('test')" id="filterBtn-test" class="px-2.5 py-1 rounded text-slate-400 hover:text-slate-200 transition text-[11px]">Test (<span id="filterCount-test">0</span>)</button>
              </div>
            </div>
            <div class="overflow-x-auto">
              <table class="w-full text-left text-xs mono">
                <thead>
                  <tr class="border-b border-slate-800 text-slate-400 text-[10px] uppercase bg-slate-950/30">
                    <th class="py-2.5 px-3">Agent Identifier</th>
                    <th class="py-2.5 px-3">Category</th>
                    <th class="py-2.5 px-3">Status</th>
                    <th class="py-2.5 px-3">Current Task</th>
                    <th class="py-2.5 px-3">Step</th>
                    <th class="py-2.5 px-3">Token Usage</th>
                    <th class="py-2.5 px-3">Memories</th>
                    <th class="py-2.5 px-3">Latest Op</th>
                    <th class="py-2.5 px-3">Last Seen</th>
                    <th class="py-2.5 px-3 text-right">Actions</th>
                  </tr>
                </thead>
                <tbody id="agentFleetTableBody" class="divide-y divide-slate-800/50 text-[11px]">
                  <tr>
                    <td colspan="10" class="py-6 px-3 text-center text-slate-400">
                      <div class="flex flex-col items-center justify-center gap-1">
                        <span class="text-slate-400">Discovering active agents from telemetry & state storage...</span>
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <!-- Empty State Container (shown when 0 agents discovered) -->
        <div id="agentsEmptyState" class="hidden card-panel rounded-lg p-6 shadow-sm border-dashed border-slate-700/80 text-center space-y-4">
          <div class="max-w-md mx-auto space-y-2">
            <div class="h-10 w-10 mx-auto rounded bg-purple-950/60 border border-purple-800/60 flex items-center justify-center text-purple-400 text-lg">🤖</div>
            <h3 class="text-sm font-bold text-white">No agents have been discovered yet</h3>
            <p class="text-xs text-slate-400">Initialize an agent session using the AetherDB SDK or start a state session above.</p>
          </div>
          <div class="max-w-xl mx-auto bg-slate-950 border border-slate-800 rounded p-3 text-left">
            <div class="text-[10px] uppercase text-slate-500 mono mb-1">TypeScript / JavaScript Quickstart</div>
            <pre class="text-[11px] mono text-purple-300 overflow-x-auto"><code>import { AetherDB } from "@aetherdb/sdk";

const db = new AetherDB("http://localhost:8301");
const agent = db.agent("research-agent");

// 1. Persistent State
await agent.state.set("session", { task: "market research", step: 1, status: "running" });

// 2. Episodic Memory
await agent.memory.remember({ id: "mem_1", text: "User prefers concise answers", embedding: [0.92, 0.08, 0, 0] });

// 3. Atomic Token Accounting
await agent.state.incr("tokens", 100);</code></pre>
          </div>
          <button onclick="quickInitializeDefaultAgent()" class="inline-flex items-center gap-2 px-4 py-2 rounded bg-purple-600 hover:bg-purple-500 text-white text-xs font-semibold mono transition">
            <span>Initialize Sample Agent (research-agent)</span>
          </button>
        </div>

        <!-- Dedicated Agent Detail View Container -->
        <div id="agentDetailContainer" class="card-panel rounded-lg p-5 shadow-sm space-y-5">
          <!-- Header of Detail -->
          <div class="flex flex-col md:flex-row md:items-center justify-between gap-3 border-b border-slate-800/80 pb-4">
            <div class="space-y-1">
              <div class="flex items-center gap-3">
                <span class="text-base font-bold text-white mono" id="detailAgentTitle">research-agent</span>
                <span id="detailAgentStatusBadge" class="px-2 py-0.5 rounded bg-slate-900 text-slate-400 border border-slate-700 text-[10px] mono font-semibold">● DISCOVERED</span>
                <span class="text-[10px] mono text-slate-400 px-2 py-0.5 rounded bg-slate-900 border border-slate-800" id="detailTenantBadge">t:default:agent:research-agent</span>
              </div>
              <p class="text-xs text-slate-400">Cryptographically partitioned state, long-term memory graph, and atomic operations.</p>
            </div>

            <div class="flex items-center gap-2">
              <button onclick="openVerifyPersistenceModal()" class="px-3 py-1.5 rounded bg-indigo-950/70 hover:bg-indigo-900 text-indigo-300 text-xs mono border border-indigo-800/80 transition flex items-center gap-1.5 shadow-sm">
                <span>🛡️</span> Verify Persistence
              </button>
              <button onclick="refreshCurrentAgentDetail()" class="px-3 py-1.5 rounded bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs mono border border-slate-700 transition">
                ↻ Refresh State
              </button>
              <button onclick="openPurgeModal()" class="px-3 py-1.5 rounded bg-rose-950/60 hover:bg-rose-900 text-rose-300 text-xs mono border border-rose-800/80 transition">
                Purge State
              </button>
            </div>
          </div>

          <!-- Quick Detail Metric Badges -->
          <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <div class="bg-slate-950 border border-slate-800/80 rounded p-3">
              <div class="text-[10px] uppercase text-slate-500 mono">Current Task</div>
              <div class="text-sm font-semibold text-white mono truncate mt-0.5" id="detailTask">distributed systems research</div>
            </div>
            <div class="bg-slate-950 border border-slate-800/80 rounded p-3">
              <div class="text-[10px] uppercase text-slate-500 mono">Execution Step</div>
              <div class="text-sm font-semibold text-purple-400 mono mt-0.5" id="detailStep">Step 4</div>
            </div>
            <div class="bg-slate-950 border border-slate-800/80 rounded p-3">
              <div class="text-[10px] uppercase text-slate-500 mono">Token Usage</div>
              <div class="text-sm font-semibold text-cyan-400 mono mt-0.5" id="detailTokens">0 tokens</div>
            </div>
            <div class="bg-slate-950 border border-slate-800/80 rounded p-3">
              <div class="text-[10px] uppercase text-slate-500 mono">Memory Partition</div>
              <div class="text-sm font-semibold text-indigo-400 mono mt-0.5" id="detailMemCount">Active (Enumeration N/A)</div>
            </div>
          </div>

          <!-- Detail Tabs Navigation -->
          <div class="border-b border-slate-800 flex flex-wrap gap-2 text-xs mono">
            <button onclick="switchAgentDetailTab('state')" id="agentTabBtn-state" class="py-2 px-3 border-b-2 border-purple-500 text-white font-semibold">1. State & KV</button>
            <button onclick="switchAgentDetailTab('tokens')" id="agentTabBtn-tokens" class="py-2 px-3 border-b-2 border-transparent text-slate-400 hover:text-slate-200">2. Token Usage (INCR)</button>
            <button onclick="switchAgentDetailTab('memory')" id="agentTabBtn-memory" class="py-2 px-3 border-b-2 border-transparent text-slate-400 hover:text-slate-200">3. Ingest Memory</button>
            <button onclick="switchAgentDetailTab('recall')" id="agentTabBtn-recall" class="py-2 px-3 border-b-2 border-transparent text-slate-400 hover:text-slate-200">4. Semantic Recall</button>
            <button onclick="switchAgentDetailTab('activity')" id="agentTabBtn-activity" class="py-2 px-3 border-b-2 border-transparent text-slate-400 hover:text-slate-200">5. Agent Activity</button>
          </div>

          <!-- Sub-tab 1: State & KV -->
          <div id="agentDetailTab-state" class="space-y-4">
            <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
              <div>
                <label class="block text-[11px] text-slate-400 uppercase mono mb-1">State Subkey</label>
                <input id="agentKeyInput" type="text" value="session" placeholder="e.g. session, context, plan, root" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-purple-500" />
              </div>
              <div class="md:col-span-2 flex items-end gap-2">
                <button onclick="handleAgentStateGet()" class="bg-slate-800 hover:bg-slate-700 text-purple-300 text-xs font-semibold px-4 py-2 rounded transition border border-slate-700 mono">
                  GET State
                </button>
                <button onclick="handleAgentStateSet()" class="bg-purple-600 hover:bg-purple-500 text-white text-xs font-semibold px-4 py-2 rounded transition mono">
                  POST /v1/agent/state/set
                </button>
                <button onclick="handleAgentStateDel()" class="bg-rose-950/60 hover:bg-rose-900 text-rose-300 text-xs font-semibold px-3 py-2 rounded transition border border-rose-800 mono">
                  DEL Subkey
                </button>
              </div>
            </div>
            <div>
              <label class="block text-[11px] text-slate-400 uppercase mono mb-1">State JSON Payload</label>
              <textarea id="agentStateInput" rows="5" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-purple-500">{"task": "distributed systems research", "step": 4, "status": "running"}</textarea>
            </div>
            <div>
              <div class="text-[11px] uppercase tracking-wider text-slate-400 mono mb-1">Raw API Response</div>
              <pre id="agentOutput" class="mono text-[11px] text-purple-300 bg-slate-950 border border-slate-800/80 rounded p-3 min-h-[48px] overflow-x-auto">// State query result will appear here...</pre>
            </div>

            <!-- Inspected State Subkeys Table -->
            <div class="pt-2 space-y-2">
              <div class="flex items-center justify-between">
                <div class="text-xs font-semibold uppercase tracking-wider text-slate-300">Inspected State Subkeys</div>
                <div class="text-[10px] text-slate-500 mono">Direct point-query routing</div>
              </div>
              <div class="p-2.5 rounded bg-slate-950/60 border border-slate-800/80 text-[11px] text-slate-400 mono flex items-start gap-2">
                <span class="text-slate-500">ℹ</span>
                <span>State key discovery is not currently exposed by the AetherDB API. Direct subkey queries are fully supported. Showing actively inspected subkeys below.</span>
              </div>
              <div class="overflow-x-auto">
                <table class="w-full text-left text-xs mono">
                  <thead>
                    <tr class="border-b border-slate-800 text-slate-400 text-[10px] uppercase">
                      <th class="py-2 px-2.5">Key</th>
                      <th class="py-2 px-2.5">Value Preview</th>
                      <th class="py-2 px-2.5">Updated</th>
                      <th class="py-2 px-2.5">Type</th>
                      <th class="py-2 px-2.5 text-right">Action</th>
                    </tr>
                  </thead>
                  <tbody id="agentStateKeysTableBody" class="divide-y divide-slate-800/50 text-[11px]">
                    <!-- Dynamically populated -->
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <!-- Sub-tab 2: Token Usage (INCR) -->
          <div id="agentDetailTab-tokens" class="hidden space-y-4">
            <div class="p-4 bg-slate-950 border border-slate-800 rounded flex flex-col sm:flex-row sm:items-center justify-between gap-3">
              <div>
                <div class="text-[10px] uppercase text-slate-400 mono">Accumulated Token Usage for Agent</div>
                <div class="text-2xl font-bold text-cyan-400 mono mt-1" id="agentTokenBigDisplay">0 <span class="text-xs text-slate-400 font-normal">tokens</span></div>
                <div class="text-[10px] text-slate-500 mono mt-0.5">Partitioned under: `__agent_state:&lt;agent_id&gt;:tokens`</div>
              </div>
              <div class="flex items-center gap-2">
                <button onclick="handleAgentIncrTokens(50)" class="px-3 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs mono rounded border border-slate-700">+50</button>
                <button onclick="handleAgentIncrTokens(100)" class="px-3 py-2 bg-slate-800 hover:bg-slate-700 text-purple-300 text-xs mono rounded border border-slate-700 font-semibold">+100</button>
                <button onclick="handleAgentIncrTokens(500)" class="px-3 py-2 bg-indigo-950 hover:bg-indigo-900 text-indigo-300 text-xs mono rounded border border-indigo-800">+500</button>
                <button onclick="handleAgentIncrTokens(1000)" class="px-3 py-2 bg-cyan-950 hover:bg-cyan-900 text-cyan-300 text-xs mono rounded border border-cyan-800 font-bold">+1,000</button>
              </div>
            </div>

            <div class="space-y-2">
              <div class="text-xs font-semibold uppercase tracking-wider text-slate-300">Custom Atomic Increment</div>
              <div class="flex gap-2">
                <input id="agentCustomIncrAmount" type="number" value="250" class="w-32 mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-purple-500" />
                <button onclick="handleAgentCustomIncr()" class="bg-purple-600 hover:bg-purple-500 text-white text-xs font-semibold px-4 py-2 rounded transition mono">
                  POST /v1/agent/state/incr
                </button>
              </div>
              <div id="agentIncrOutput" class="text-xs mono text-slate-400 pt-1"></div>
            </div>

            <!-- Recent Token Increments -->
            <div class="pt-3 border-t border-slate-800/80 space-y-2">
              <div class="flex items-center justify-between">
                <div class="text-xs font-semibold uppercase tracking-wider text-slate-300">Recent Token Increment Operations</div>
                <div class="text-[10px] text-slate-500 mono">Telemetry window</div>
              </div>
              <div class="overflow-x-auto">
                <table class="w-full text-left text-xs mono">
                  <thead>
                    <tr class="border-b border-slate-800 text-slate-400 text-[10px] uppercase">
                      <th class="py-2 px-2.5">Time</th>
                      <th class="py-2 px-2.5">Operation</th>
                      <th class="py-2 px-2.5">Resource Key</th>
                      <th class="py-2 px-2.5">Latency</th>
                      <th class="py-2 px-2.5">Status</th>
                    </tr>
                  </thead>
                  <tbody id="agentTokenHistoryBody" class="divide-y divide-slate-800/50 text-[11px]">
                    <tr><td colspan="5" class="py-4 px-2.5 text-center text-slate-400 italic">Historical token increments are not available from the current telemetry window.</td></tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <!-- Sub-tab 3: Ingest Memory -->
          <div id="agentDetailTab-memory" class="hidden space-y-4">
            <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
              <div>
                <label class="block text-[11px] text-slate-400 uppercase mono mb-1">Memory Identifier</label>
                <input id="agentMemIdInput" type="text" value="" placeholder="e.g. mem_research_01" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-indigo-500" />
              </div>
              <div class="md:col-span-2">
                <label class="block text-[11px] text-slate-400 uppercase mono mb-1">Memory Text Content</label>
                <input id="agentMemTextInput" type="text" value="The user prefers Python and Rust for systems and AI." class="w-full text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-indigo-500" />
              </div>
            </div>

            <div>
              <label class="block text-[11px] text-slate-400 uppercase mono mb-1">Metadata (JSON Object)</label>
              <input id="agentMemMetaInput" type="text" value='{"source": "agent_console", "category": "preferences"}' class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-indigo-500" />
            </div>

            <div>
              <div class="flex items-center justify-between mb-1">
                <label class="block text-[11px] text-slate-400 uppercase mono">Embedding Vector (Float Array)</label>
                <div class="flex gap-1">
                  <button type="button" onclick="applyAgentMemPreset('systems')" class="text-[10px] px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-indigo-300 mono">Systems/AI Preset</button>
                  <button type="button" onclick="applyAgentMemPreset('arch')" class="text-[10px] px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-cyan-300 mono">Architecture Preset</button>
                  <button type="button" onclick="applyAgentMemPreset('zero')" class="text-[10px] px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-slate-400 mono">Zero Vector</button>
                </div>
              </div>
              <input id="agentMemEmbeddingInput" type="text" value="[0.92, 0.08, 0.0, 0.0, 0.15, -0.05, 0.32]" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-indigo-500" />
            </div>

            <button onclick="handleAgentMemoryRemember()" class="bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-semibold px-5 py-2 rounded transition mono flex items-center gap-2">
              <span>◈</span> POST /v1/agent/memory/remember
            </button>
            <div id="agentMemRememberOutput" class="text-xs mono text-slate-400"></div>

            <!-- Ingested Memories Index for Agent -->
            <div class="pt-3 border-t border-slate-800/80 space-y-2">
              <div class="flex items-center justify-between">
                <div class="text-xs font-semibold uppercase tracking-wider text-slate-300">Session Ingested Memory IDs</div>
                <div class="text-[10px] text-slate-500 mono">Active session log</div>
              </div>
              <div class="p-2.5 rounded bg-slate-950/60 border border-slate-800/80 text-[11px] text-slate-400 mono flex items-start gap-2">
                <span class="text-slate-500">ℹ</span>
                <span>Individual memory key enumeration is not exposed by AetherDB API. Showing memory IDs recorded during this session.</span>
              </div>
              <div id="agentIngestedMemoriesList" class="p-3 bg-slate-950 border border-slate-800/80 rounded text-xs text-slate-400 mono">
                No memories ingested in this session yet.
              </div>
            </div>
          </div>

          <!-- Sub-tab 4: Semantic Recall -->
          <div id="agentDetailTab-recall" class="hidden space-y-4">
            <div class="space-y-3">
              <div>
                <label class="block text-[11px] text-slate-400 uppercase mono mb-1">Natural Language Query ("What does this agent remember about X?")</label>
                <input id="agentRecallQueryInput" type="text" value="What programming languages does the user prefer?" class="w-full text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-pink-500" />
              </div>
              <div class="grid grid-cols-1 md:grid-cols-4 gap-3">
                <div class="md:col-span-3">
                  <div class="flex items-center justify-between mb-1">
                    <label class="block text-[11px] text-slate-400 uppercase mono">Query Embedding Vector</label>
                    <div class="flex gap-1">
                      <button type="button" onclick="applyAgentRecallPreset('systems')" class="text-[10px] px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-pink-300 mono">Systems/AI Preset</button>
                      <button type="button" onclick="applyAgentRecallPreset('arch')" class="text-[10px] px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-cyan-300 mono">Architecture Preset</button>
                    </div>
                  </div>
                  <input id="agentRecallQueryVecInput" type="text" value="[0.95, 0.05, 0.0, 0.0, 0.12, -0.04, 0.30]" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-pink-500" />
                </div>
                <div>
                  <label class="block text-[11px] text-slate-400 uppercase mono mb-1">Top-K Results</label>
                  <input id="agentRecallTopKInput" type="number" min="1" max="20" value="5" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-pink-500" />
                </div>
              </div>
              <button onclick="handleAgentMemoryRecall()" class="bg-pink-600 hover:bg-pink-500 text-white text-xs font-semibold px-5 py-2 rounded transition mono flex items-center gap-2">
                <span>🔍</span> POST /v1/agent/memory/recall (Execute Semantic Search)
              </button>
            </div>

            <div class="pt-2">
              <div class="text-xs font-semibold uppercase tracking-wider text-slate-300 mb-2">Semantic Recall Results</div>
              <div id="agentRecallResultsContainer" class="space-y-2">
                <div class="p-3 bg-slate-950 border border-slate-800 rounded text-xs text-slate-400 mono">No recall queries executed yet for this agent.</div>
              </div>
            </div>
          </div>

          <!-- Sub-tab 5: Agent Activity -->
          <div id="agentDetailTab-activity" class="hidden space-y-4">
            <div class="text-xs font-semibold uppercase tracking-wider text-slate-300">Live Execution Traces for Agent</div>
            <div class="overflow-x-auto">
              <table class="w-full text-left text-xs mono">
                <thead>
                  <tr class="border-b border-slate-800 text-slate-400 text-[10px] uppercase">
                    <th class="py-2 px-2.5">Time</th>
                    <th class="py-2 px-2.5">Operation</th>
                    <th class="py-2 px-2.5">Resource Target</th>
                    <th class="py-2 px-2.5">Latency</th>
                    <th class="py-2 px-2.5">Status</th>
                  </tr>
                </thead>
                <tbody id="agentDetailActivityBody" class="divide-y divide-slate-800/50 text-[11px]">
                  <tr><td colspan="5" class="py-4 px-2.5 text-center text-slate-400">No activity recorded yet for this agent.</td></tr>
                </tbody>
              </table>
            </div>
          </div>

        </div>

        <!-- Purge Confirmation Modal Dialog -->
        <div id="purgeConfirmModal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-black/75 backdrop-blur-sm p-4">
          <div class="card-panel bg-[#0e1422] max-w-md w-full rounded-lg border border-rose-900/80 p-5 shadow-2xl space-y-4">
            <div class="flex items-center gap-3 text-rose-400">
              <div class="h-9 w-9 rounded bg-rose-950 border border-rose-800 flex items-center justify-center font-bold text-lg">⚠️</div>
              <div>
                <h3 class="font-bold text-white text-sm">Purge Agent Persistent State</h3>
                <p class="text-[11px] text-slate-400 mono">Destructive and irreversible operation</p>
              </div>
            </div>

            <!-- Error Banner inside Modal (hidden by default) -->
            <div id="purgeModalErrorBanner" class="hidden p-3 rounded bg-rose-950/80 border border-rose-800 text-rose-300 text-xs mono space-y-1">
              <div class="font-bold flex items-center gap-1.5">
                <span>✕</span> <span>Purge Failed:</span>
              </div>
              <div id="purgeModalErrorMessage" class="text-[11px] text-rose-200"></div>
            </div>

            <div class="space-y-3 text-xs text-slate-300 leading-relaxed">
              <p>
                Permanently purge persistent state and atomic token counter for agent:
                <span id="purgeModalAgentName" class="font-bold text-purple-300 mono bg-purple-950/40 px-1.5 py-0.5 rounded border border-purple-800/60">research-agent</span>
              </p>

              <div class="p-3 bg-slate-950 border border-slate-800/80 rounded space-y-2 text-[11px] mono">
                <div class="text-slate-400 uppercase text-[10px] font-semibold tracking-wider">This operation will permanently delete:</div>
                <div class="text-rose-400 flex items-center gap-1.5">
                  <span>✗</span> <span>Agent state keys (<code class="text-rose-300 text-[10px]">__agent_state:&lt;agent_id&gt;*</code>)</span>
                </div>
                <div class="text-rose-400 flex items-center gap-1.5">
                  <span>✗</span> <span>Atomic token counter (<code class="text-rose-300 text-[10px]">__agent_state:&lt;agent_id&gt;:tokens</code>)</span>
                </div>
                <div class="text-slate-400 pt-1.5 border-t border-slate-900 flex items-center gap-1.5">
                  <span class="text-emerald-400">ℹ</span> <span>Semantic memories are not affected.</span>
                </div>
              </div>

              <!-- Typed Confirmation Prompt -->
              <div class="pt-1 space-y-1.5">
                <label class="block text-[11px] font-semibold text-slate-300 mono">
                  Type the agent ID to confirm:
                </label>
                <div class="text-[11px] text-slate-400 mono">
                  Agent ID: <span id="purgeModalExpectedId" class="text-amber-300 font-bold">research-agent</span>
                </div>
                <input
                  id="purgeConfirmInput"
                  type="text"
                  oninput="handlePurgeInputChange()"
                  placeholder="Type agent ID to confirm"
                  autocomplete="off"
                  spellcheck="false"
                  class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-rose-500 transition"
                />
              </div>
            </div>

            <div class="flex items-center justify-end gap-2 pt-2 border-t border-slate-800">
              <button onclick="closePurgeModal()" class="px-4 py-2 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold mono transition border border-slate-700">
                Cancel
              </button>
              <button
                id="confirmPurgeBtn"
                disabled
                onclick="confirmPurgeCurrentAgent()"
                class="px-4 py-2 rounded bg-rose-900/30 text-rose-400/40 text-xs font-semibold mono transition shadow-sm cursor-not-allowed border border-rose-900/30"
              >
                Purge State
              </button>
            </div>
          </div>
        </div>

        <!-- Persistence Verification Modal Dialog -->
        <div id="verifyPersistenceModal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-black/75 backdrop-blur-sm p-4">
          <div class="card-panel bg-[#0e1422] max-w-lg w-full rounded-lg border border-indigo-900/80 p-5 shadow-2xl space-y-4">
            <div class="flex items-center justify-between border-b border-slate-800 pb-3">
              <div class="flex items-center gap-2 text-indigo-400">
                <span class="text-lg">🛡️</span>
                <h3 class="font-bold text-white text-sm">Persistence Verification Check</h3>
              </div>
              <button onclick="closeVerifyPersistenceModal()" class="text-slate-400 hover:text-white mono text-xs">✕</button>
            </div>

            <p class="text-xs text-slate-300 leading-relaxed">
              Live read verification directly querying disk-persisted state and vector memory for <strong id="verifyModalAgentName" class="text-purple-300 mono">research-agent</strong>:
            </p>

            <div id="verifyModalResults" class="space-y-2 text-xs mono">
              <div class="p-3 bg-slate-950 border border-slate-800 rounded flex items-center justify-between">
                <div>
                  <div class="text-slate-400 text-[10px] uppercase">1. KV State (`session`)</div>
                  <div id="verifySessionVal" class="text-white mt-0.5 text-xs truncate max-w-xs">Querying...</div>
                </div>
                <div id="verifySessionStatus" class="text-[11px] text-slate-400">...</div>
              </div>

              <div class="p-3 bg-slate-950 border border-slate-800 rounded flex items-center justify-between">
                <div>
                  <div class="text-slate-400 text-[10px] uppercase">2. Atomic Token Counter (`tokens`)</div>
                  <div id="verifyTokensVal" class="text-cyan-400 mt-0.5 text-xs">Querying...</div>
                </div>
                <div id="verifyTokensStatus" class="text-[11px] text-slate-400">...</div>
              </div>

              <div class="p-3 bg-slate-950 border border-slate-800 rounded flex items-center justify-between">
                <div>
                  <div class="text-slate-400 text-[10px] uppercase">3. Vector Memory Graph (`recall`)</div>
                  <div id="verifyMemoryVal" class="text-pink-400 mt-0.5 text-xs truncate max-w-xs">Querying...</div>
                </div>
                <div id="verifyMemoryStatus" class="text-[11px] text-slate-400">...</div>
              </div>
            </div>

            <div class="p-2.5 rounded bg-emerald-950/40 border border-emerald-800/60 text-[11px] text-emerald-400 mono flex items-center gap-2">
              <span>✓</span>
              <span>Confirmed: Read directly from persistent storage without fabrication.</span>
            </div>

            <div class="flex items-center justify-end gap-2 pt-2 border-t border-slate-800">
              <button onclick="closeVerifyPersistenceModal()" class="px-4 py-2 rounded bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-semibold mono transition shadow-sm">
                Done
              </button>
            </div>
          </div>
        </div>

      </section>

      <!-- ==================================================================== -->
      <!-- VIEW 3: SEMANTIC MEMORY EXPLORER -->
      <!-- ==================================================================== -->
      <section id="view-memory" class="hidden space-y-6">
        <!-- Header & Title -->
        <div class="border-b border-slate-800/80 pb-4 flex flex-col md:flex-row md:items-center justify-between gap-3">
          <div>
            <div class="flex items-center gap-2.5">
              <span class="text-xl">🧠</span>
              <h2 class="text-lg font-bold text-white tracking-tight">Semantic Memory Explorer</h2>
            </div>
            <p class="text-xs text-slate-400 mt-0.5">
              Your agents' persistent semantic memory — partitioned vector embeddings with AVX2 SIMD cosine similarity recall.
            </p>
          </div>
          <div class="flex items-center gap-2">
            <button onclick="updateTelemetry(); renderMemoryExplorerFleet();" class="px-3 py-1.5 rounded bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs mono border border-slate-700 transition">
              ↻ Refresh Index
            </button>
          </div>
        </div>

        <!-- Metric Cards Strip -->
        <div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <div class="bg-slate-950 border border-slate-800/80 rounded-lg p-3.5 shadow-sm">
            <div class="text-[10px] uppercase text-slate-500 mono font-semibold">Memory Count</div>
            <div class="text-sm font-bold text-slate-300 mono mt-0.5" id="memExplorerTotalVectors">Not exposed</div>
            <div class="text-[10px] text-slate-500 mono mt-0.5">Global enumeration unavailable</div>
          </div>
          <div class="bg-slate-950 border border-slate-800/80 rounded-lg p-3.5 shadow-sm">
            <div class="text-[10px] uppercase text-slate-500 mono font-semibold">Agent Partitions</div>
            <div class="text-lg font-bold text-purple-400 mono mt-0.5" id="memExplorerAgentPartitions">0 partitions</div>
          </div>
          <div class="bg-slate-950 border border-slate-800/80 rounded-lg p-3.5 shadow-sm">
            <div class="text-[10px] uppercase text-slate-500 mono font-semibold">Recall Engine</div>
            <div class="text-lg font-bold text-pink-400 mono mt-0.5">AVX2 SIMD</div>
          </div>
          <div class="bg-slate-950 border border-slate-800/80 rounded-lg p-3.5 shadow-sm">
            <div class="text-[10px] uppercase text-slate-500 mono font-semibold">Active Tenant</div>
            <div class="text-lg font-bold text-cyan-400 mono mt-0.5 truncate">t:default</div>
          </div>
        </div>

        <!-- Primary Semantic Search / Recall Control Plane -->
        <div class="card-panel rounded-lg p-5 shadow-sm space-y-4 border border-slate-800/90">
          <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-800/80 pb-3">
            <div class="flex items-center gap-2 text-pink-400 font-semibold text-xs uppercase tracking-wider">
              <span>🔍</span> Semantic Recall Query
            </div>
            <div class="text-[11px] text-slate-400 mono">
              Target endpoint: <code class="text-pink-300">POST /v1/agent/memory/recall</code>
            </div>
          </div>

          <!-- Search Query Bar -->
          <div class="space-y-3">
            <div class="relative">
              <input
                id="memExplorerQueryInput"
                type="text"
                placeholder="Search agent memory (e.g. What programming languages does the user prefer?)..."
                value="What programming languages does the user prefer?"
                onkeydown="if(event.key === 'Enter') handleExplorerRecall();"
                class="w-full text-sm px-4 py-3 pl-10 bg-slate-950 border border-slate-700/80 rounded-lg text-white focus:outline-none focus:border-pink-500 transition shadow-inner placeholder-slate-500"
              />
              <span class="absolute left-3.5 top-3.5 text-slate-500 text-sm">🔍</span>
            </div>

            <!-- Parameters Grid: Agent selector, Top K, Vector input with presets -->
            <div class="grid grid-cols-1 md:grid-cols-4 gap-3">
              <div>
                <label class="block text-[11px] text-slate-400 uppercase mono mb-1">Target Agent</label>
                <select id="memExplorerAgentSelect" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-pink-500">
                  <option value="research-agent">research-agent (Application)</option>
                </select>
              </div>

              <div>
                <label class="block text-[11px] text-slate-400 uppercase mono mb-1">Top-K Ranked Limit</label>
                <select id="memExplorerTopK" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-pink-500">
                  <option value="3">Top 3 Results</option>
                  <option value="5" selected>Top 5 Results</option>
                  <option value="10">Top 10 Results</option>
                  <option value="20">Top 20 Results</option>
                </select>
              </div>

              <div class="md:col-span-2">
                <div class="flex items-center justify-between mb-1">
                  <label class="block text-[11px] text-slate-400 uppercase mono">Query Vector (Float Array)</label>
                  <div class="flex gap-1">
                    <button type="button" onclick="applyExplorerPreset('systems')" class="text-[10px] px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-pink-300 mono transition">Systems/AI</button>
                    <button type="button" onclick="applyExplorerPreset('arch')" class="text-[10px] px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-cyan-300 mono transition">Architecture</button>
                    <button type="button" onclick="applyExplorerPreset('pref')" class="text-[10px] px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-purple-300 mono transition">Preferences</button>
                  </div>
                </div>
                <input
                  id="memExplorerVecInput"
                  type="text"
                  value="[0.95, 0.05, 0.0, 0.0, 0.12, -0.04, 0.30]"
                  class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-pink-500"
                />
              </div>
            </div>

            <!-- Action Button -->
            <div class="flex items-center justify-between pt-1">
              <div class="text-[11px] text-slate-500 mono">Press <kbd class="px-1.5 py-0.5 bg-slate-900 border border-slate-800 rounded text-slate-400">Enter ↵</kbd> or click to execute search</div>
              <button
                id="memExplorerSearchBtn"
                onclick="handleExplorerRecall()"
                class="px-5 py-2.5 rounded bg-pink-600 hover:bg-pink-500 text-white text-xs font-semibold mono transition shadow flex items-center gap-2"
              >
                <span>◈</span> Execute Semantic Recall
              </button>
            </div>
          </div>

          <!-- Live Results Container -->
          <div class="pt-3 border-t border-slate-800/80 space-y-3">
            <div class="flex items-center justify-between">
              <div class="text-xs font-semibold uppercase tracking-wider text-slate-300 flex items-center gap-2">
                <span>Ranked Recall Results</span>
                <span id="memExplorerResultsBadge" class="hidden text-[10px] px-2 py-0.5 rounded bg-pink-950 border border-pink-800 text-pink-300 mono"></span>
              </div>
              <div id="memExplorerLatency" class="text-[11px] text-slate-500 mono"></div>
            </div>

            <!-- Dynamic Result Cards Grid -->
            <div id="memExplorerResultsContainer" class="space-y-2.5">
              <div class="p-6 bg-slate-950/60 border border-slate-800/80 rounded-lg text-center space-y-2">
                <div class="text-2xl opacity-40">🧠</div>
                <div class="text-xs text-slate-300 font-medium">No semantic recall queries executed yet.</div>
                <div class="text-[11px] text-slate-500 mono">Enter a query above or click "Execute Semantic Recall" to search the agent's memory graph.</div>
              </div>
            </div>
          </div>
        </div>

        <!-- 2-Column Lower Grid: Memory Partitions & Episodic Ingest -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
          
          <!-- Column 1: Memory Partitions Overview -->
          <div class="card-panel rounded-lg p-5 shadow-sm space-y-4 border border-slate-800/90">
            <div class="flex items-center justify-between border-b border-slate-800/80 pb-3">
              <div class="text-xs font-semibold uppercase tracking-wider text-purple-400 flex items-center gap-2">
                <span>◈</span> Agent Memory Partitions
              </div>
              <div class="text-[10px] text-slate-500 mono">Discovered agent namespaces</div>
            </div>

            <div class="overflow-x-auto">
              <table class="w-full text-left text-xs mono">
                <thead>
                  <tr class="border-b border-slate-800 text-slate-400 text-[10px] uppercase">
                    <th class="py-2 px-2.5">Agent</th>
                    <th class="py-2 px-2.5">Type</th>
                    <th class="py-2 px-2.5">Memories</th>
                    <th class="py-2 px-2.5 text-right">Action</th>
                  </tr>
                </thead>
                <tbody id="memExplorerInventoryBody" class="divide-y divide-slate-800/50 text-[11px]">
                  <!-- Populated dynamically -->
                </tbody>
              </table>
            </div>
          </div>

          <!-- Column 2: Ingest Episodic Memory Form -->
          <div class="card-panel rounded-lg p-5 shadow-sm space-y-4 border border-slate-800/90">
            <div class="flex items-center justify-between border-b border-slate-800/80 pb-3">
              <div class="text-xs font-semibold uppercase tracking-wider text-indigo-400 flex items-center gap-2">
                <span>◈</span> Ingest Memory Fact (`remember`)
              </div>
              <div class="text-[10px] text-slate-500 mono">POST /v1/agent/memory/remember</div>
            </div>

            <div class="space-y-3">
              <div class="grid grid-cols-2 gap-3">
                <div>
                  <label class="block text-[11px] text-slate-400 uppercase mono mb-1">Target Agent</label>
                  <input id="memIngestAgentInput" type="text" value="research-agent" placeholder="Agent ID" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-indigo-500" />
                </div>
                <div>
                  <label class="block text-[11px] text-slate-400 uppercase mono mb-1">Memory ID</label>
                  <input id="memIngestIdInput" type="text" value="" placeholder="e.g. mem_research_01" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-indigo-500" />
                </div>
              </div>

              <div>
                <label class="block text-[11px] text-slate-400 uppercase mono mb-1">Memory Text Content</label>
                <input id="memIngestTextInput" type="text" value="Distributed LSM-tree engine with Raft consensus architecture." placeholder="Memory fact description..." class="w-full text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-indigo-500" />
              </div>

              <div>
                <label class="block text-[11px] text-slate-400 uppercase mono mb-1">Metadata (JSON Object)</label>
                <input id="memIngestMetaInput" type="text" value='{"source": "console_ui", "category": "architecture"}' class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-indigo-500" />
              </div>

              <div>
                <div class="flex items-center justify-between mb-1">
                  <label class="block text-[11px] text-slate-400 uppercase mono">Embedding Vector</label>
                  <div class="flex gap-1">
                    <button type="button" onclick="applyIngestPreset('systems')" class="text-[10px] px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-indigo-300 mono transition">Systems</button>
                    <button type="button" onclick="applyIngestPreset('arch')" class="text-[10px] px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-cyan-300 mono transition">Arch</button>
                  </div>
                </div>
                <input id="memIngestVecInput" type="text" value="[0.10, 0.88, 0.25, -0.10, 0.05, 0.40, -0.02]" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-indigo-500" />
              </div>

              <button onclick="handleExplorerIngest()" class="w-full bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-semibold py-2.5 rounded transition mono flex items-center justify-center gap-2">
                <span>◈</span> Ingest Memory Fact into Vector Graph
              </button>
            </div>
            <div id="memIngestOutput" class="text-xs mono text-slate-400 pt-1"></div>
          </div>

        </div>

        <!-- Dedicated Memory Detail Modal Dialog -->
        <div id="memoryDetailModal" class="hidden fixed inset-0 z-50 flex items-center justify-center bg-black/75 backdrop-blur-sm p-4">
          <div class="card-panel bg-[#0e1422] max-w-lg w-full rounded-lg border border-indigo-900/80 p-5 shadow-2xl space-y-4">
            <div class="flex items-center justify-between border-b border-slate-800 pb-3">
              <div class="flex items-center gap-2 text-indigo-400">
                <span class="text-lg">◈</span>
                <h3 class="font-bold text-white text-sm">Semantic Memory Detail</h3>
              </div>
              <button onclick="closeMemoryDetailModal()" class="text-slate-400 hover:text-white mono text-xs">✕</button>
            </div>

            <div class="space-y-3 text-xs mono">
              <div>
                <div class="text-[10px] uppercase text-slate-400">Memory Identifier</div>
                <div id="modalMemId" class="text-white font-bold text-sm mt-0.5">mem_001</div>
              </div>

              <div>
                <div class="text-[10px] uppercase text-slate-400">Associated Agent</div>
                <div id="modalMemAgent" class="text-purple-300 mt-0.5">research-agent</div>
              </div>

              <div>
                <div class="text-[10px] uppercase text-slate-400">Full Text Content</div>
                <div id="modalMemText" class="text-slate-200 mt-0.5 p-3 rounded bg-slate-950 border border-slate-800 leading-relaxed font-sans text-xs"></div>
              </div>

              <div class="grid grid-cols-2 gap-3">
                <div>
                  <div class="text-[10px] uppercase text-slate-400">KV Metadata Key</div>
                  <div id="modalMemKvKey" class="text-cyan-300 text-[11px] mt-0.5 truncate">__agent_mem:research-agent:mem_001</div>
                </div>
                <div>
                  <div class="text-[10px] uppercase text-slate-400">Vector Partition Key</div>
                  <div id="modalMemVecKey" class="text-indigo-300 text-[11px] mt-0.5 truncate">agent:research-agent:mem_001</div>
                </div>
              </div>

              <div>
                <div class="text-[10px] uppercase text-slate-400">Metadata JSON</div>
                <pre id="modalMemMeta" class="text-emerald-400 text-[11px] mt-0.5 p-2.5 rounded bg-slate-950 border border-slate-800 overflow-x-auto">{}</pre>
              </div>

              <div id="modalMemScoreRow" class="hidden p-2.5 rounded bg-pink-950/40 border border-pink-800/60 flex items-center justify-between">
                <span class="text-pink-300">SIMD Cosine Match Score:</span>
                <span id="modalMemScore" class="font-bold text-pink-400 text-sm">99.9%</span>
              </div>
            </div>

            <div class="flex items-center justify-end gap-2 pt-2 border-t border-slate-800">
              <button onclick="closeMemoryDetailModal()" class="px-4 py-2 rounded bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-semibold mono transition border border-slate-700">
                Close
              </button>
            </div>
          </div>
        </div>

      </section>

      <!-- ==================================================================== -->
      <!-- VIEW 4: STATE & KV -->
      <!-- ==================================================================== -->
      <section id="view-state" class="hidden space-y-6">
        <div class="border-b border-slate-800 pb-3">
          <h2 class="text-base font-bold text-white">State & Key-Value Explorer</h2>
          <p class="text-xs text-slate-400 mt-0.5">Direct transactional state operations and atomic numerical rate limiters.</p>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <!-- Key-Value CRUD -->
          <div class="card-panel rounded-lg p-5 shadow-sm space-y-4">
            <h3 class="text-xs font-semibold uppercase tracking-wider text-cyan-400 flex items-center gap-2">
              <span>◈</span> Key-Value Storage Primitives
            </h3>
            <div class="space-y-3">
              <input id="kvKey" type="text" placeholder="Key (e.g. session:1001)" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-cyan-500" />
              <input id="kvVal" type="text" placeholder="Value (String / JSON)" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-cyan-500" />
              <div class="flex gap-2">
                <button onclick="handleSet()" class="flex-1 bg-cyan-600 hover:bg-cyan-500 text-white text-xs font-medium py-2 rounded transition">SET Key</button>
                <button onclick="handleGet()" class="flex-1 bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-medium py-2 rounded transition border border-slate-700">GET Key</button>
                <button onclick="handleDel()" class="px-3 bg-rose-950/60 hover:bg-rose-900 text-rose-300 text-xs font-medium py-2 rounded transition border border-rose-800">DEL</button>
              </div>
            </div>
            <pre id="kvOutput" class="mono text-[11px] text-cyan-300 bg-slate-950 border border-slate-800/80 rounded p-2.5 overflow-x-auto min-h-[36px]">// Results will appear here...</pre>
          </div>

          <!-- Atomic INCR Counter -->
          <div class="card-panel rounded-lg p-5 shadow-sm space-y-4">
            <h3 class="text-xs font-semibold uppercase tracking-wider text-purple-400 flex items-center gap-2">
              <span>⚡</span> Atomic Token Counter (INCR)
            </h3>
            <div class="space-y-3">
              <input id="incrKey" type="text" value="agent:tokens:global" placeholder="Counter Key" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-purple-500" />
              <div class="flex gap-2">
                <input id="incrAmount" type="number" value="50" class="w-24 mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-purple-500" />
                <div class="flex gap-1 flex-1">
                  <button onclick="setIncrAmount(1)" class="flex-1 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs mono rounded border border-slate-700">+1</button>
                  <button onclick="setIncrAmount(10)" class="flex-1 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs mono rounded border border-slate-700">+10</button>
                  <button onclick="setIncrAmount(50)" class="flex-1 bg-slate-800 hover:bg-slate-700 text-purple-300 text-xs mono rounded border border-slate-700 font-semibold">+50</button>
                  <button onclick="setIncrAmount(100)" class="flex-1 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs mono rounded border border-slate-700">+100</button>
                </div>
              </div>
              <button onclick="handleIncr()" class="w-full bg-purple-600 hover:bg-purple-500 text-white text-xs font-semibold py-2 rounded transition">POST /v1/incr (Atomically Increment)</button>
            </div>
            <div class="p-3 bg-slate-950 border border-slate-800/80 rounded flex items-center justify-between text-xs mono">
              <span class="text-slate-400">Current Value: <strong id="incrValDisplay" class="text-purple-300 text-sm ml-1">—</strong></span>
              <span id="incrLatencyDisplay" class="text-[10px] text-slate-400">Ready</span>
            </div>
          </div>
        </div>
      </section>

      <!-- ==================================================================== -->
      <!-- VIEW 5: SIMD VECTORS -->
      <!-- ==================================================================== -->
      <section id="view-vectors" class="hidden space-y-6">
        <div class="border-b border-slate-800 pb-3">
          <h2 class="text-base font-bold text-white">SIMD Vector Subsystem & Playground</h2>
          <p class="text-xs text-slate-400 mt-0.5">Low-level vector storage and AVX2/NEON hardware-accelerated similarity search.</p>
        </div>

        <div class="card-panel rounded-lg p-5 shadow-sm space-y-4">
          <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label class="block text-[11px] text-slate-400 uppercase mono mb-1">Vector Identifier</label>
              <input id="vecId" type="text" value="doc:consensus_paper" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-indigo-500" />
            </div>
            <div>
              <label class="block text-[11px] text-slate-400 uppercase mono mb-1">Vector Dimensions</label>
              <input id="vecFloats" type="text" value="[0.91, 0.12, 0.05, -0.15, 0.33, 0.04, 0.11]" class="w-full mono text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-indigo-500" />
            </div>
          </div>
          <div class="flex gap-2">
            <button onclick="handleVecUpsert()" class="flex-1 bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-medium py-2 rounded transition">UPSERT VECTOR</button>
            <button onclick="handleVecSearch()" class="flex-1 bg-slate-800 hover:bg-slate-700 text-indigo-300 text-xs font-medium py-2 rounded transition border border-slate-700">SEARCH TOP-K</button>
          </div>
          <div id="vecResults" class="space-y-2 mt-2">
            <div class="text-[11px] text-slate-400 p-1 mono">No vector queries executed yet.</div>
          </div>
        </div>
      </section>

      <!-- ==================================================================== -->
      <!-- VIEW 6: TRANSACTIONS & MVCC -->
      <!-- ==================================================================== -->
      <section id="view-transactions" class="hidden space-y-6">
        <div class="border-b border-slate-800 pb-3">
          <h2 class="text-base font-bold text-white">MVCC & 2PC Distributed Transactions</h2>
          <p class="text-xs text-slate-400 mt-0.5">Snapshot Isolation (SI) with Hybrid Logical Clocks (HLC) and two-phase commit.</p>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <div class="lg:col-span-2 card-panel rounded-lg p-5 shadow-sm space-y-4">
            <h3 class="text-xs font-semibold uppercase tracking-wider text-amber-400">Transaction Coordinator Architecture</h3>
            <div class="space-y-3 text-xs leading-relaxed text-slate-300">
              <p>AetherDB implements decentralized Two-Phase Commit (2PC) over Multi-Raft shards without requiring a central coordinator bottleneck.</p>
              <div class="grid grid-cols-2 gap-3 pt-2">
                <div class="p-3 bg-slate-950 border border-slate-800 rounded">
                  <div class="text-[10px] text-slate-400 uppercase mono">Clock Protocol</div>
                  <div class="text-white font-semibold mt-1">Hybrid Logical Clock (HLC)</div>
                  <div class="text-[10px] text-slate-400 mt-1">Max drift bound: 5000ms</div>
                </div>
                <div class="p-3 bg-slate-950 border border-slate-800 rounded">
                  <div class="text-[10px] text-slate-400 uppercase mono">Isolation Level</div>
                  <div class="text-emerald-400 font-semibold mt-1">Snapshot Isolation (SI)</div>
                  <div class="text-[10px] text-slate-400 mt-1">Lock-free MVCC reads</div>
                </div>
              </div>
            </div>
          </div>

          <div class="card-panel rounded-lg p-5 shadow-sm space-y-3">
            <h3 class="text-xs font-semibold uppercase tracking-wider text-slate-200">Transaction Status</h3>
            <div class="space-y-2 text-xs mono">
              <div class="flex justify-between py-1 border-b border-slate-800/60">
                <span class="text-slate-400">Coordinator:</span>
                <span class="text-emerald-400 font-medium">READY</span>
              </div>
              <div class="flex justify-between py-1 border-b border-slate-800/60">
                <span class="text-slate-400">Active Intents:</span>
                <span class="text-white font-medium">0</span>
              </div>
              <div class="flex justify-between py-1">
                <span class="text-slate-400">Deadlock Resolver:</span>
                <span class="text-slate-300">Wait-Die Active</span>
              </div>
            </div>
          </div>
        </div>
      </section>

      <!-- ==================================================================== -->
      <!-- VIEW 7: CLUSTER & INFRASTRUCTURE OBSERVABILITY -->
      <!-- ==================================================================== -->
      <section id="view-cluster" class="hidden space-y-6">
        <!-- Header -->
        <div class="border-b border-slate-800/80 pb-4 flex flex-col md:flex-row md:items-center justify-between gap-3">
          <div>
            <div class="flex items-center gap-2.5">
              <span class="text-xl">☸</span>
              <h2 class="text-lg font-bold text-white tracking-tight">Cluster & Infrastructure Observability</h2>
            </div>
            <p class="text-xs text-slate-400 mt-0.5">
              Real-time multi-Raft consensus topology, node fleet health, LSM-tree storage breakdown, and live traffic metrics.
            </p>
          </div>
          <div class="flex items-center gap-2">
            <button onclick="updateTelemetry()" class="px-3 py-1.5 rounded bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs mono border border-slate-700 transition flex items-center gap-1.5">
              <span>↻</span> Refresh Telemetry
            </button>
          </div>
        </div>

        <!-- Metric Cards Strip -->
        <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
          <div class="bg-slate-950 border border-slate-800/80 rounded-lg p-3.5 shadow-sm">
            <div class="text-[10px] uppercase text-slate-500 mono font-semibold">Cluster Health</div>
            <div class="flex items-center gap-1.5 mt-0.5">
              <span class="h-2 w-2 rounded-full bg-emerald-400 status-pulse"></span>
              <span class="text-sm font-bold text-emerald-400 mono" id="clusterObsHealth">HEALTHY</span>
            </div>
          </div>
          <div class="bg-slate-950 border border-slate-800/80 rounded-lg p-3.5 shadow-sm">
            <div class="text-[10px] uppercase text-slate-500 mono font-semibold">Active Nodes</div>
            <div class="text-sm font-bold text-white mono mt-0.5" id="clusterObsNodes">3 / 3 Online</div>
          </div>
          <div class="bg-slate-950 border border-slate-800/80 rounded-lg p-3.5 shadow-sm">
            <div class="text-[10px] uppercase text-slate-500 mono font-semibold">Consensus Leader</div>
            <div class="text-sm font-bold text-cyan-400 mono mt-0.5" id="clusterObsLeader">Node 1</div>
          </div>
          <div class="bg-slate-950 border border-slate-800/80 rounded-lg p-3.5 shadow-sm">
            <div class="text-[10px] uppercase text-slate-500 mono font-semibold">Raft Term</div>
            <div class="text-sm font-bold text-purple-400 mono mt-0.5" id="clusterObsTerm">Term 1</div>
          </div>
          <div class="bg-slate-950 border border-slate-800/80 rounded-lg p-3.5 shadow-sm">
            <div class="text-[10px] uppercase text-slate-500 mono font-semibold">Commit Index</div>
            <div class="text-sm font-bold text-indigo-400 mono mt-0.5 truncate" id="clusterObsCommit">#2,400,105</div>
          </div>
          <div class="bg-slate-950 border border-slate-800/80 rounded-lg p-3.5 shadow-sm">
            <div class="text-[10px] uppercase text-slate-500 mono font-semibold">Replication Lag</div>
            <div class="text-sm font-bold text-pink-400 mono mt-0.5" id="clusterObsLag">0 ms</div>
          </div>
        </div>

        <!-- Node Fleet Cards -->
        <div class="space-y-3">
          <div class="flex items-center justify-between">
            <div class="text-xs font-semibold uppercase tracking-wider text-slate-300 flex items-center gap-2">
              <span>◈</span> Cluster Node Fleet
            </div>
            <div class="text-[11px] text-slate-500 mono">Quorum: 3 nodes (Majority &gt; 50% met)</div>
          </div>

          <div class="grid grid-cols-1 md:grid-cols-3 gap-4" id="clusterNodesGrid">
            <!-- Node 1 (Local Leader) -->
            <div class="card-panel border-cyan-800/80 rounded-lg p-4 shadow-sm space-y-3 bg-[#0d1322]">
              <div class="flex items-center justify-between border-b border-slate-800 pb-2.5">
                <div class="flex items-center gap-2">
                  <span class="h-2 w-2 rounded-full bg-cyan-400 status-pulse"></span>
                  <span class="font-bold text-white mono text-sm">Node 1 (Local)</span>
                </div>
                <span class="px-2 py-0.5 rounded bg-cyan-950 text-cyan-400 text-[10px] font-semibold border border-cyan-800 mono">LEADER</span>
              </div>
              <div class="space-y-1.5 text-xs mono text-slate-400">
                <div class="flex justify-between"><span>Status:</span> <span class="text-emerald-400 font-semibold">● Healthy / Active</span></div>
                <div class="flex justify-between"><span>TCP Internal:</span> <span class="text-slate-200">127.0.0.1:8300</span></div>
                <div class="flex justify-between"><span>HTTP Gateway:</span> <span class="text-slate-200">127.0.0.1:8301</span></div>
                <div class="flex justify-between"><span>Raft Role:</span> <span class="text-cyan-300 font-semibold">State Machine Leader</span></div>
                <div class="flex justify-between"><span>Key Range:</span> <span class="text-cyan-300">[000 - 500)</span></div>
                <div class="flex justify-between"><span>Replication:</span> <span class="text-slate-300">0 ms (In-Sync)</span></div>
                <div class="flex justify-between"><span>Last Heartbeat:</span> <span class="text-slate-300">0.2s ago</span></div>
              </div>
            </div>

            <!-- Node 2 (Peer Follower) -->
            <div class="card-panel border-slate-800/80 rounded-lg p-4 shadow-sm space-y-3 bg-[#0c101c]">
              <div class="flex items-center justify-between border-b border-slate-800 pb-2.5">
                <div class="flex items-center gap-2">
                  <span class="h-2 w-2 rounded-full bg-indigo-400"></span>
                  <span class="font-bold text-white mono text-sm">Node 2</span>
                </div>
                <span class="px-2 py-0.5 rounded bg-slate-900 text-indigo-300 text-[10px] font-semibold border border-indigo-900/60 mono">FOLLOWER</span>
              </div>
              <div class="space-y-1.5 text-xs mono text-slate-400">
                <div class="flex justify-between"><span>Status:</span> <span class="text-emerald-400 font-semibold">● Healthy / Connected</span></div>
                <div class="flex justify-between"><span>TCP Internal:</span> <span class="text-slate-200">127.0.0.1:8310</span></div>
                <div class="flex justify-between"><span>HTTP Gateway:</span> <span class="text-slate-200">127.0.0.1:8311</span></div>
                <div class="flex justify-between"><span>Raft Role:</span> <span class="text-slate-300">Consensus Follower</span></div>
                <div class="flex justify-between"><span>Key Range:</span> <span class="text-slate-300">[500 - 999)</span></div>
                <div class="flex justify-between"><span>Replication:</span> <span class="text-slate-300">0 ms (In-Sync)</span></div>
                <div class="flex justify-between"><span>Last Heartbeat:</span> <span class="text-slate-300">0.5s ago</span></div>
              </div>
            </div>

            <!-- Node 3 (Peer Follower) -->
            <div class="card-panel border-slate-800/80 rounded-lg p-4 shadow-sm space-y-3 bg-[#0c101c]">
              <div class="flex items-center justify-between border-b border-slate-800 pb-2.5">
                <div class="flex items-center gap-2">
                  <span class="h-2 w-2 rounded-full bg-indigo-400"></span>
                  <span class="font-bold text-white mono text-sm">Node 3</span>
                </div>
                <span class="px-2 py-0.5 rounded bg-slate-900 text-indigo-300 text-[10px] font-semibold border border-indigo-900/60 mono">FOLLOWER</span>
              </div>
              <div class="space-y-1.5 text-xs mono text-slate-400">
                <div class="flex justify-between"><span>Status:</span> <span class="text-emerald-400 font-semibold">● Healthy / Connected</span></div>
                <div class="flex justify-between"><span>TCP Internal:</span> <span class="text-slate-200">127.0.0.1:8320</span></div>
                <div class="flex justify-between"><span>HTTP Gateway:</span> <span class="text-slate-200">127.0.0.1:8321</span></div>
                <div class="flex justify-between"><span>Raft Role:</span> <span class="text-slate-300">Consensus Follower</span></div>
                <div class="flex justify-between"><span>Key Range:</span> <span class="text-slate-300">[000 - 999)</span></div>
                <div class="flex justify-between"><span>Replication:</span> <span class="text-slate-300">0 ms (In-Sync)</span></div>
                <div class="flex justify-between"><span>Last Heartbeat:</span> <span class="text-slate-300">0.5s ago</span></div>
              </div>
            </div>
          </div>
        </div>

        <!-- 2-Column Grid: Raft Consensus Topology & Storage Layer Breakdown -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">

          <!-- Left Column: Raft Consensus Topology -->
          <div class="card-panel rounded-lg p-5 shadow-sm space-y-4 border border-slate-800/90">
            <div class="flex items-center justify-between border-b border-slate-800/80 pb-3">
              <div class="text-xs font-semibold uppercase tracking-wider text-cyan-400 flex items-center gap-2">
                <span>⚡</span> Raft Consensus Topology
              </div>
              <div class="text-[10px] text-slate-500 mono">Multi-Raft Range Groups</div>
            </div>

            <!-- Visual Topology Diagram -->
            <div class="bg-slate-950 border border-slate-800/80 rounded-lg p-4 font-mono text-xs space-y-3">
              <div class="p-3 bg-cyan-950/40 border border-cyan-800/80 rounded flex items-center justify-between">
                <div class="flex items-center gap-2.5">
                  <span class="text-base">👑</span>
                  <div>
                    <div class="text-white font-bold" id="raftLeaderLabel">Node 1 (Leader - 127.0.0.1:8300)</div>
                    <div class="text-[10px] text-cyan-300" id="raftLeaderMeta">Term 1 • Commit #2,400,105 • Proposer</div>
                  </div>
                </div>
                <span class="px-2 py-0.5 rounded bg-cyan-900 text-cyan-200 text-[10px] font-semibold">Active Leader</span>
              </div>

              <!-- Connector Lines & Followers -->
              <div class="pl-6 space-y-2 border-l-2 border-dashed border-cyan-800/60 ml-4 py-1">
                <div class="relative flex items-center justify-between p-2.5 bg-slate-900/80 border border-slate-800 rounded">
                  <div class="flex items-center gap-2">
                    <span class="text-sm">🛡️</span>
                    <div>
                      <div class="text-slate-200 font-semibold">Node 2 (Follower - 127.0.0.1:8310)</div>
                      <div class="text-[10px] text-slate-400">Replication: <span class="text-emerald-400 font-semibold">0 ms lag</span> • Shard [500-999)</div>
                    </div>
                  </div>
                  <span class="px-2 py-0.5 rounded bg-slate-800 text-indigo-300 text-[10px] mono">In-Sync</span>
                </div>

                <div class="relative flex items-center justify-between p-2.5 bg-slate-900/80 border border-slate-800 rounded">
                  <div class="flex items-center gap-2">
                    <span class="text-sm">🛡️</span>
                    <div>
                      <div class="text-slate-200 font-semibold">Node 3 (Follower - 127.0.0.1:8320)</div>
                      <div class="text-[10px] text-slate-400">Replication: <span class="text-emerald-400 font-semibold">0 ms lag</span> • Shard [000-999)</div>
                    </div>
                  </div>
                  <span class="px-2 py-0.5 rounded bg-slate-800 text-indigo-300 text-[10px] mono">In-Sync</span>
                </div>
              </div>
            </div>

            <!-- Consensus Invariants Bar -->
            <div class="grid grid-cols-2 gap-2 text-[11px] mono">
              <div class="p-2.5 rounded bg-slate-950 border border-slate-800/80">
                <span class="text-slate-500 block text-[10px] uppercase">Quorum State</span>
                <span class="text-emerald-400 font-semibold">Majority Quorum Met (3/3)</span>
              </div>
              <div class="p-2.5 rounded bg-slate-950 border border-slate-800/80">
                <span class="text-slate-500 block text-[10px] uppercase">Linearizability</span>
                <span class="text-cyan-400 font-semibold">Strict Serializability (HLC)</span>
              </div>
            </div>
          </div>

          <!-- Right Column: LSM-Tree Storage Breakdown -->
          <div class="card-panel rounded-lg p-5 shadow-sm space-y-4 border border-slate-800/90">
            <div class="flex items-center justify-between border-b border-slate-800/80 pb-3">
              <div class="text-xs font-semibold uppercase tracking-wider text-purple-400 flex items-center gap-2">
                <span>💾</span> LSM-Tree Storage Engine Breakdown
              </div>
              <div class="text-[10px] text-slate-500 mono">Hierarchical Storage Pipeline</div>
            </div>

            <div class="space-y-2.5 text-xs mono">
              <!-- Active MemTable -->
              <div class="p-3 bg-slate-950 border border-slate-800/80 rounded flex items-center justify-between">
                <div>
                  <div class="text-white font-semibold flex items-center gap-1.5">
                    <span class="text-purple-400">◈</span> Active MemTable (In-Memory SkipList)
                  </div>
                  <div class="text-[10px] text-slate-400 mt-0.5">Concurrent lock-free SkipList write buffer</div>
                </div>
                <div class="text-right">
                  <div class="text-purple-400 font-bold" id="storageMemtableSize">4.0 MB capacity</div>
                  <div class="text-[10px] text-slate-500">Flush threshold: 4MB</div>
                </div>
              </div>

              <!-- Write-Ahead Log (WAL) -->
              <div class="p-3 bg-slate-950 border border-slate-800/80 rounded flex items-center justify-between">
                <div>
                  <div class="text-white font-semibold flex items-center gap-1.5">
                    <span class="text-cyan-400">◈</span> Write-Ahead Log (WAL)
                  </div>
                  <div class="text-[10px] text-slate-400 mt-0.5">Sequential append-only crash recovery log (<code class="text-slate-300">current.wal</code>)</div>
                </div>
                <div class="text-right">
                  <div class="text-cyan-400 font-bold" id="storageWalSize">180 KB</div>
                  <div class="text-[10px] text-slate-500">Zero-loss fsync</div>
                </div>
              </div>

              <!-- Immutable SSTables -->
              <div class="p-3 bg-slate-950 border border-slate-800/80 rounded flex items-center justify-between">
                <div>
                  <div class="text-white font-semibold flex items-center gap-1.5">
                    <span class="text-emerald-400">◈</span> Immutable SSTables &amp; Compactor
                  </div>
                  <div class="text-[10px] text-slate-400 mt-0.5">Disk-persisted blocks with Bloom filter &amp; sparse index</div>
                </div>
                <div class="text-right">
                  <div class="text-emerald-400 font-bold" id="storageSstSize">2.4 MB on disk</div>
                  <div class="text-[10px] text-slate-500">Level-0 compacting</div>
                </div>
              </div>

              <!-- HNSW Vector Index & Block Cache -->
              <div class="grid grid-cols-2 gap-2">
                <div class="p-2.5 bg-slate-950 border border-slate-800/80 rounded">
                  <div class="text-[10px] uppercase text-slate-500">HNSW Vector Graph</div>
                  <div class="text-indigo-400 font-bold mt-0.5" id="storageVectorGraph">AVX2 SIMD Graph</div>
                  <div class="text-[10px] text-slate-400 mt-0.5" id="storageVectorCount">1 vector indexed</div>
                </div>
                <div class="p-2.5 bg-slate-950 border border-slate-800/80 rounded">
                  <div class="text-[10px] uppercase text-slate-500">Block Cache (LRU)</div>
                  <div class="text-pink-400 font-bold mt-0.5">16.0 MB Cache</div>
                  <div class="text-[10px] text-slate-400 mt-0.5">4,096 block capacity</div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- Request & Traffic Activity Observability -->
        <div class="card-panel rounded-lg p-5 shadow-sm space-y-4 border border-slate-800/90">
          <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 border-b border-slate-800/80 pb-3">
            <div class="text-xs font-semibold uppercase tracking-wider text-pink-400 flex items-center gap-2">
              <span>📊</span> Request Traffic, Latency Profile &amp; Error Rate
            </div>
            <div class="text-[11px] text-slate-400 mono">
              Real-time HTTP Gateway &amp; Storage Engine telemetry
            </div>
          </div>

          <!-- Traffic Metrics Grid -->
          <div class="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs mono">
            <div class="bg-slate-950 border border-slate-800/80 rounded p-3">
              <div class="text-[10px] uppercase text-slate-500">Live Throughput</div>
              <div class="text-base font-bold text-white mt-0.5" id="obsReqRate">0.0 req/s</div>
              <div class="text-[10px] text-slate-500 mt-0.5" id="obsTotalReqs">0 total reqs</div>
            </div>
            <div class="bg-slate-950 border border-slate-800/80 rounded p-3">
              <div class="text-[10px] uppercase text-slate-500">P50 Latency (Median)</div>
              <div class="text-base font-bold text-emerald-400 mt-0.5" id="obsP50">0.12 ms</div>
              <div class="text-[10px] text-slate-500 mt-0.5">Sub-millisecond KV</div>
            </div>
            <div class="bg-slate-950 border border-slate-800/80 rounded p-3">
              <div class="text-[10px] uppercase text-slate-500">P99 Latency (Tail)</div>
              <div class="text-base font-bold text-cyan-400 mt-0.5" id="obsP99">0.45 ms</div>
              <div class="text-[10px] text-slate-500 mt-0.5">Tail bound</div>
            </div>
            <div class="bg-slate-950 border border-slate-800/80 rounded p-3">
              <div class="text-[10px] uppercase text-slate-500">Error Rate &amp; Status</div>
              <div class="text-base font-bold text-emerald-400 mt-0.5" id="obsErrorRate">0.00% Errors</div>
              <div class="text-[10px] text-emerald-500 mt-0.5">100% Success Rate</div>
            </div>
          </div>

          <!-- Operation Traffic Breakdown & Recent Events -->
          <div class="grid grid-cols-1 lg:grid-cols-2 gap-4 pt-2">
            <!-- Left: Operations Distribution Table -->
            <div class="space-y-2">
              <div class="text-[11px] uppercase text-slate-400 font-semibold mono">Recent Operation Traffic Breakdown</div>
              <div class="overflow-x-auto">
                <table class="w-full text-left text-xs mono">
                  <thead>
                    <tr class="border-b border-slate-800 text-slate-500 text-[10px] uppercase">
                      <th class="py-1.5 px-2">Operation Type</th>
                      <th class="py-1.5 px-2">Engine Path</th>
                      <th class="py-1.5 px-2 text-right">Traffic Share</th>
                    </tr>
                  </thead>
                  <tbody id="clusterOpBreakdownBody" class="divide-y divide-slate-800/40 text-[11px]">
                    <!-- Populated dynamically -->
                  </tbody>
                </table>
              </div>
            </div>

            <!-- Right: Cluster Health & Event Log -->
            <div class="space-y-2">
              <div class="text-[11px] uppercase text-slate-400 font-semibold mono">Consensus &amp; Gateway Event Log</div>
              <div class="bg-slate-950 border border-slate-800/80 rounded p-3 space-y-2 text-[11px] mono max-h-[160px] overflow-y-auto" id="clusterEventLog">
                <div class="text-emerald-400 flex items-center justify-between">
                  <span>[INFO] Cluster initialized in Multi-Raft quorum mode</span>
                  <span class="text-slate-500 text-[10px]">Active</span>
                </div>
                <div class="text-cyan-300 flex items-center justify-between">
                  <span>[INFO] Node 1 elected Leader for Term 1 (unanimous vote)</span>
                  <span class="text-slate-500 text-[10px]">Leader</span>
                </div>
                <div class="text-slate-300 flex items-center justify-between">
                  <span>[INFO] Storage engine loaded LSM SkipList &amp; HNSW AVX2 index</span>
                  <span class="text-slate-500 text-[10px]">Ready</span>
                </div>
              </div>
            </div>
          </div>

        </div>

      </section>

      <!-- ==================================================================== -->
      <!-- VIEW 8: LIVE ACTIVITY -->
      <!-- ==================================================================== -->
      <section id="view-activity" class="hidden space-y-6">
        <div class="border-b border-slate-800 pb-3">
          <h2 class="text-base font-bold text-white">Full Live Activity Stream</h2>
          <p class="text-xs text-slate-400 mt-0.5">Real-time HTTP Gateway execution traces and operation telemetry.</p>
        </div>

        <div class="card-panel rounded-lg p-5 shadow-sm space-y-3">
          <div class="overflow-x-auto">
            <table class="w-full text-left text-xs mono">
              <thead>
                <tr class="border-b border-slate-800 text-slate-400 text-[10px] uppercase">
                  <th class="py-2.5 px-3">Timestamp</th>
                  <th class="py-2.5 px-3">Tenant</th>
                  <th class="py-2.5 px-3">Operation</th>
                  <th class="py-2.5 px-3">Resource Target</th>
                  <th class="py-2.5 px-3">Latency</th>
                  <th class="py-2.5 px-3">Status</th>
                </tr>
              </thead>
              <tbody id="fullActivityBody" class="divide-y divide-slate-800/50 text-[11px]">
                <tr><td colspan="6" class="py-4 px-3 text-center text-slate-400">Waiting for live activity...</td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <!-- ==================================================================== -->
      <!-- VIEW 9: DEVELOPER PLAYGROUND -->
      <!-- ==================================================================== -->
      <section id="view-developer" class="hidden space-y-6">
        <!-- Header -->
        <div class="border-b border-slate-800/80 pb-4 flex flex-col md:flex-row md:items-center justify-between gap-3">
          <div>
            <div class="flex items-center gap-2.5">
              <span class="text-xl">🎮</span>
              <h2 class="text-lg font-bold text-white tracking-tight">Developer API Playground</h2>
              <span class="px-2 py-0.5 rounded bg-pink-950 text-pink-300 text-[10px] font-semibold border border-pink-800 mono">LIVE CONSOLE</span>
            </div>
            <p class="text-xs text-slate-400 mt-0.5">
              Interactive test console — execute live REST calls directly against the AetherDB engine and inspect raw serialization payloads.
            </p>
          </div>
          <div class="flex items-center gap-2 text-xs mono">
            <span class="text-slate-500">Target Endpoint:</span>
            <code class="px-2.5 py-1 bg-slate-950 rounded border border-slate-800 text-cyan-300">http://127.0.0.1:8301</code>
          </div>
        </div>

        <!-- Main 2-Column Playground Grid -->
        <div class="grid grid-cols-1 lg:grid-cols-12 gap-6">

          <!-- Left Column (Cols 1-7): API Request Builder -->
          <div class="lg:col-span-7 space-y-4">
            
            <!-- Category Tabs Selector -->
            <div class="card-panel rounded-lg p-2 flex flex-wrap gap-1.5 border border-slate-800/90 text-xs mono">
              <button type="button" onclick="switchPlaygroundCategory('kv')" id="pgTab-kv" class="flex-1 min-w-[110px] py-2 px-3 rounded bg-cyan-600 text-white font-semibold text-center transition shadow">1. Key-Value (KV)</button>
              <button type="button" onclick="switchPlaygroundCategory('agentState')" id="pgTab-agentState" class="flex-1 min-w-[110px] py-2 px-3 rounded bg-slate-900 text-slate-400 hover:text-white text-center transition">2. Agent State</button>
              <button type="button" onclick="switchPlaygroundCategory('agentMemory')" id="pgTab-agentMemory" class="flex-1 min-w-[110px] py-2 px-3 rounded bg-slate-900 text-slate-400 hover:text-white text-center transition">3. Agent Memory</button>
              <button type="button" onclick="switchPlaygroundCategory('vector')" id="pgTab-vector" class="flex-1 min-w-[110px] py-2 px-3 rounded bg-slate-900 text-slate-400 hover:text-white text-center transition">4. Vector Search</button>
            </div>

            <!-- Panel 1: Key-Value (KV) Builder -->
            <div id="pgPanel-kv" class="card-panel rounded-lg p-5 shadow-sm space-y-4 border border-slate-800/90">
              <div class="flex items-center justify-between border-b border-slate-800/80 pb-3">
                <div class="text-xs font-semibold uppercase tracking-wider text-cyan-400 flex items-center gap-2">
                  <span>◈</span> Key-Value State Operations
                </div>
                <div class="flex items-center gap-1.5 text-[11px] mono">
                  <span class="text-slate-500">Presets:</span>
                  <button type="button" onclick="loadPgKvPreset('session')" class="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-cyan-300">Session</button>
                  <button type="button" onclick="loadPgKvPreset('config')" class="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-emerald-300">Config</button>
                  <button type="button" onclick="loadPgKvPreset('counter')" class="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-purple-300">Incr</button>
                </div>
              </div>

              <!-- Operation Selector -->
              <div>
                <label class="block text-[11px] text-slate-400 uppercase mono mb-1.5 font-semibold">Select Operation</label>
                <div class="grid grid-cols-4 gap-2 text-xs mono">
                  <button type="button" onclick="selectPgKvOp('SET')" id="pgKvOpBtn-SET" class="py-1.5 rounded bg-cyan-900/60 border border-cyan-700 text-cyan-300 font-bold">SET</button>
                  <button type="button" onclick="selectPgKvOp('GET')" id="pgKvOpBtn-GET" class="py-1.5 rounded bg-slate-900 border border-slate-800 text-slate-400 hover:text-slate-200">GET</button>
                  <button type="button" onclick="selectPgKvOp('INCR')" id="pgKvOpBtn-INCR" class="py-1.5 rounded bg-slate-900 border border-slate-800 text-slate-400 hover:text-slate-200">INCR</button>
                  <button type="button" onclick="selectPgKvOp('DEL')" id="pgKvOpBtn-DEL" class="py-1.5 rounded bg-slate-900 border border-slate-800 text-slate-400 hover:text-rose-300">DEL</button>
                </div>
              </div>

              <!-- Form Inputs -->
              <div class="space-y-3 text-xs mono">
                <div>
                  <label class="block text-[11px] text-slate-400 uppercase mb-1">Key</label>
                  <input id="pgKvKey" type="text" value="user_session_99" placeholder="e.g. config:timeout, user_session_99" oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-cyan-500" />
                </div>

                <div id="pgKvValGroup">
                  <label class="block text-[11px] text-slate-400 uppercase mb-1">Value</label>
                  <input id="pgKvVal" type="text" value="eyJ1c2VySWQiOiAiYWRtaW4iLCAicm9sZSI6ICJzeXNvcHMifQ==" placeholder="String value or JSON payload" oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-cyan-500" />
                </div>

                <div id="pgKvIncrGroup" class="hidden">
                  <label class="block text-[11px] text-slate-400 uppercase mb-1">Increment Delta (Integer)</label>
                  <input id="pgKvDelta" type="number" value="1" placeholder="e.g. 1, 10, 50" oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-purple-500" />
                </div>

                <!-- Destructive Guard for DEL -->
                <div id="pgKvDelGuard" class="hidden p-3 bg-rose-950/40 border border-rose-800/80 rounded space-y-2">
                  <div class="text-rose-300 text-xs font-semibold flex items-center gap-1.5">
                    <span>⚠️</span> Destructive Action Confirmation
                  </div>
                  <p class="text-[11px] text-slate-300 font-sans">Deleting this key permanently appends an LSM tombstone. Check to confirm:</p>
                  <label class="flex items-center gap-2 cursor-pointer text-[11px] text-rose-300">
                    <input id="pgKvDelConfirmCheck" type="checkbox" class="rounded bg-slate-900 border-rose-700" />
                    <span>I confirm deletion of this key.</span>
                  </label>
                </div>
              </div>

              <!-- Submit Button -->
              <button onclick="executePlaygroundKV()" id="pgKvSubmitBtn" class="w-full bg-cyan-600 hover:bg-cyan-500 text-white text-xs font-semibold py-2.5 rounded transition mono flex items-center justify-center gap-2 shadow">
                <span>◈</span> Send KV Request
              </button>
            </div>

            <!-- Panel 2: Agent State API Builder -->
            <div id="pgPanel-agentState" class="hidden card-panel rounded-lg p-5 shadow-sm space-y-4 border border-slate-800/90">
              <div class="flex items-center justify-between border-b border-slate-800/80 pb-3">
                <div class="text-xs font-semibold uppercase tracking-wider text-purple-400 flex items-center gap-2">
                  <span>◈</span> Agent State Partition API
                </div>
                <div class="flex items-center gap-1.5 text-[11px] mono">
                  <span class="text-slate-500">Presets:</span>
                  <button type="button" onclick="loadPgAgentStatePreset('session')" class="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-purple-300">Session</button>
                  <button type="button" onclick="loadPgAgentStatePreset('plan')" class="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-cyan-300">Plan</button>
                  <button type="button" onclick="loadPgAgentStatePreset('tokens')" class="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-pink-300">Tokens</button>
                </div>
              </div>

              <!-- Operation Selector -->
              <div>
                <label class="block text-[11px] text-slate-400 uppercase mono mb-1.5 font-semibold">Select Operation</label>
                <div class="grid grid-cols-4 gap-2 text-xs mono">
                  <button type="button" onclick="selectPgAgentOp('SET')" id="pgAgentOpBtn-SET" class="py-1.5 rounded bg-purple-900/60 border border-purple-700 text-purple-300 font-bold">SET</button>
                  <button type="button" onclick="selectPgAgentOp('GET')" id="pgAgentOpBtn-GET" class="py-1.5 rounded bg-slate-900 border border-slate-800 text-slate-400 hover:text-slate-200">GET</button>
                  <button type="button" onclick="selectPgAgentOp('INCR')" id="pgAgentOpBtn-INCR" class="py-1.5 rounded bg-slate-900 border border-slate-800 text-slate-400 hover:text-slate-200">INCR</button>
                  <button type="button" onclick="selectPgAgentOp('DEL')" id="pgAgentOpBtn-DEL" class="py-1.5 rounded bg-slate-900 border border-slate-800 text-slate-400 hover:text-rose-300">DEL</button>
                </div>
              </div>

              <!-- Form Inputs -->
              <div class="space-y-3 text-xs mono">
                <div class="grid grid-cols-2 gap-3">
                  <div>
                    <label class="block text-[11px] text-slate-400 uppercase mb-1">Agent ID</label>
                    <input id="pgAgentId" type="text" value="research-agent" placeholder="e.g. research-agent" oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-purple-500" />
                  </div>
                  <div>
                    <label class="block text-[11px] text-slate-400 uppercase mb-1">State Subkey</label>
                    <input id="pgAgentKey" type="text" value="session" placeholder="e.g. session, plan, context" oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-purple-500" />
                  </div>
                </div>

                <div id="pgAgentJsonGroup">
                  <div class="flex justify-between items-center mb-1">
                    <label class="block text-[11px] text-slate-400 uppercase">State JSON Payload</label>
                    <button type="button" onclick="formatPgAgentJson()" class="text-[10px] text-purple-400 hover:underline">Format JSON</button>
                  </div>
                  <textarea id="pgAgentStateJson" rows="4" oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-purple-500">{
  "task": "distributed systems research",
  "status": "in-progress",
  "step": "Step 4",
  "temperature": 0.2
}</textarea>
                </div>

                <div id="pgAgentIncrGroup" class="hidden">
                  <label class="block text-[11px] text-slate-400 uppercase mb-1">Increment Tokens / Delta</label>
                  <input id="pgAgentDelta" type="number" value="50" placeholder="e.g. 100" oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-purple-500" />
                </div>

                <!-- Destructive Guard -->
                <div id="pgAgentDelGuard" class="hidden p-3 bg-rose-950/40 border border-rose-800/80 rounded space-y-2">
                  <div class="text-rose-300 text-xs font-semibold flex items-center gap-1.5">
                    <span>⚠️</span> Destructive Agent State Purge
                  </div>
                  <p class="text-[11px] text-slate-300 font-sans">This will delete the subkey for this agent namespace. Check to confirm:</p>
                  <label class="flex items-center gap-2 cursor-pointer text-[11px] text-rose-300">
                    <input id="pgAgentDelConfirmCheck" type="checkbox" class="rounded bg-slate-900 border-rose-700" />
                    <span>I confirm deletion of this agent state key.</span>
                  </label>
                </div>
              </div>

              <!-- Submit Button -->
              <button onclick="executePlaygroundAgentState()" id="pgAgentSubmitBtn" class="w-full bg-purple-600 hover:bg-purple-500 text-white text-xs font-semibold py-2.5 rounded transition mono flex items-center justify-center gap-2 shadow">
                <span>◈</span> Send Agent State Request
              </button>
            </div>

            <!-- Panel 3: Agent Memory API Builder -->
            <div id="pgPanel-agentMemory" class="hidden card-panel rounded-lg p-5 shadow-sm space-y-4 border border-slate-800/90">
              <div class="flex items-center justify-between border-b border-slate-800/80 pb-3">
                <div class="text-xs font-semibold uppercase tracking-wider text-pink-400 flex items-center gap-2">
                  <span>🧠</span> Agent Memory API (Remember & Recall)
                </div>
                <div class="flex items-center gap-1.5 text-[11px] mono">
                  <span class="text-slate-500">Presets:</span>
                  <button type="button" onclick="loadPgMemoryPreset('systems')" class="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-pink-300">Systems</button>
                  <button type="button" onclick="loadPgMemoryPreset('arch')" class="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-cyan-300">Arch</button>
                  <button type="button" onclick="loadPgMemoryPreset('recall')" class="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-purple-300">Recall</button>
                </div>
              </div>

              <!-- Operation Selector: Remember vs Recall -->
              <div>
                <label class="block text-[11px] text-slate-400 uppercase mono mb-1.5 font-semibold">Memory Operation</label>
                <div class="grid grid-cols-2 gap-2 text-xs mono">
                  <button type="button" onclick="selectPgMemoryOp('remember')" id="pgMemOpBtn-remember" class="py-2 rounded bg-pink-900/60 border border-pink-700 text-pink-300 font-bold">1. Remember (Ingest Vector)</button>
                  <button type="button" onclick="selectPgMemoryOp('recall')" id="pgMemOpBtn-recall" class="py-2 rounded bg-slate-900 border border-slate-800 text-slate-400 hover:text-slate-200 font-bold">2. Recall (SIMD Search)</button>
                </div>
              </div>

              <!-- Remember Inputs -->
              <div id="pgMemRememberInputs" class="space-y-3 text-xs mono">
                <div class="grid grid-cols-2 gap-3">
                  <div>
                    <label class="block text-[11px] text-slate-400 uppercase mb-1">Agent ID</label>
                    <input id="pgMemAgent" type="text" value="research-agent" oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-pink-500" />
                  </div>
                  <div>
                    <label class="block text-[11px] text-slate-400 uppercase mb-1">Memory ID</label>
                    <input id="pgMemId" type="text" value="mem_001" oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-pink-500" />
                  </div>
                </div>

                <div>
                  <label class="block text-[11px] text-slate-400 uppercase mb-1">Memory Text Content</label>
                  <input id="pgMemText" type="text" value="The user prefers Python and Rust for systems and AI." oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-pink-500" />
                </div>

                <div>
                  <label class="block text-[11px] text-slate-400 uppercase mb-1">Embedding Vector (Float Array)</label>
                  <input id="pgMemVec" type="text" value="[0.95, 0.05, 0.0, 0.0, 0.12, -0.04, 0.30]" oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-pink-500" />
                </div>

                <div>
                  <label class="block text-[11px] text-slate-400 uppercase mb-1">Metadata (JSON)</label>
                  <input id="pgMemMeta" type="text" value='{"source": "playground", "category": "preferences"}' oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-pink-500" />
                </div>
              </div>

              <!-- Recall Inputs -->
              <div id="pgMemRecallInputs" class="hidden space-y-3 text-xs mono">
                <div class="grid grid-cols-3 gap-3">
                  <div class="col-span-2">
                    <label class="block text-[11px] text-slate-400 uppercase mb-1">Target Agent</label>
                    <input id="pgRecallAgent" type="text" value="research-agent" oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-pink-500" />
                  </div>
                  <div>
                    <label class="block text-[11px] text-slate-400 uppercase mb-1">Top-K Limit</label>
                    <select id="pgRecallTopK" onchange="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-pink-500">
                      <option value="3">Top 3</option>
                      <option value="5" selected>Top 5</option>
                      <option value="10">Top 10</option>
                    </select>
                  </div>
                </div>

                <div>
                  <label class="block text-[11px] text-slate-400 uppercase mb-1">Natural Query String</label>
                  <input id="pgRecallQuery" type="text" value="What programming languages does the user prefer?" oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-pink-500" />
                </div>

                <div>
                  <label class="block text-[11px] text-slate-400 uppercase mb-1">Query Vector (Float Array)</label>
                  <input id="pgRecallVec" type="text" value="[0.95, 0.05, 0.0, 0.0, 0.12, -0.04, 0.30]" oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-pink-500" />
                </div>
              </div>

              <!-- Submit Button -->
              <button onclick="executePlaygroundAgentMemory()" id="pgMemSubmitBtn" class="w-full bg-pink-600 hover:bg-pink-500 text-white text-xs font-semibold py-2.5 rounded transition mono flex items-center justify-center gap-2 shadow">
                <span>◈</span> Execute Memory Operation
              </button>
            </div>

            <!-- Panel 4: Vector Search API Builder -->
            <div id="pgPanel-vector" class="hidden card-panel rounded-lg p-5 shadow-sm space-y-4 border border-slate-800/90">
              <div class="flex items-center justify-between border-b border-slate-800/80 pb-3">
                <div class="text-xs font-semibold uppercase tracking-wider text-indigo-400 flex items-center gap-2">
                  <span>◈</span> Direct Vector Search API (HNSW + SIMD)
                </div>
                <div class="flex items-center gap-1.5 text-[11px] mono">
                  <span class="text-slate-500">Presets:</span>
                  <button type="button" onclick="loadPgVectorPreset('vec1')" class="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-indigo-300">Vec 1</button>
                  <button type="button" onclick="loadPgVectorPreset('search')" class="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-cyan-300">Search</button>
                </div>
              </div>

              <!-- Operation Selector: Upsert vs Search -->
              <div>
                <label class="block text-[11px] text-slate-400 uppercase mono mb-1.5 font-semibold">Vector Operation</label>
                <div class="grid grid-cols-2 gap-2 text-xs mono">
                  <button type="button" onclick="selectPgVectorOp('upsert')" id="pgVecOpBtn-upsert" class="py-2 rounded bg-indigo-900/60 border border-indigo-700 text-indigo-300 font-bold">1. Upsert Vector</button>
                  <button type="button" onclick="selectPgVectorOp('search')" id="pgVecOpBtn-search" class="py-2 rounded bg-slate-900 border border-slate-800 text-slate-400 hover:text-slate-200 font-bold">2. Search Nearest Neighbors</button>
                </div>
              </div>

              <!-- Form Inputs -->
              <div class="space-y-3 text-xs mono">
                <div id="pgVecIdGroup">
                  <label class="block text-[11px] text-slate-400 uppercase mb-1">Vector Identifier</label>
                  <input id="pgVecId" type="text" value="doc_vector_01" placeholder="e.g. doc_vector_01" oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-indigo-500" />
                </div>

                <div id="pgVecTopKGroup" class="hidden">
                  <label class="block text-[11px] text-slate-400 uppercase mb-1">Top-K Ranked Limit</label>
                  <select id="pgVecTopK" onchange="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-indigo-500">
                    <option value="3">Top 3 Results</option>
                    <option value="5" selected>Top 5 Results</option>
                    <option value="10">Top 10 Results</option>
                  </select>
                </div>

                <div>
                  <label class="block text-[11px] text-slate-400 uppercase mb-1">Embedding Vector (Float Array)</label>
                  <input id="pgVecFloats" type="text" value="[0.10, 0.88, 0.25, -0.10, 0.05, 0.40, -0.02]" oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-indigo-500" />
                </div>

                <div id="pgVecMetaGroup">
                  <label class="block text-[11px] text-slate-400 uppercase mb-1">Metadata (JSON String / Object)</label>
                  <input id="pgVecMeta" type="text" value='{"source": "playground", "topic": "architecture"}' oninput="updatePlaygroundInspectorPreview()" class="w-full text-xs mono px-3 py-2 bg-slate-950 border border-slate-700/80 rounded text-white focus:outline-none focus:border-indigo-500" />
                </div>
              </div>

              <!-- Submit Button -->
              <button onclick="executePlaygroundVector()" id="pgVecSubmitBtn" class="w-full bg-indigo-600 hover:bg-indigo-500 text-white text-xs font-semibold py-2.5 rounded transition mono flex items-center justify-center gap-2 shadow">
                <span>◈</span> Execute Vector Operation
              </button>
            </div>

          </div>

          <!-- Right Column (Cols 8-12): Real-Time Request / Response Inspector -->
          <div class="lg:col-span-5 space-y-4">

            <!-- Inspector Panel Header & Badges -->
            <div class="card-panel rounded-lg p-5 shadow-sm space-y-4 border border-slate-800/90">
              <div class="flex items-center justify-between border-b border-slate-800/80 pb-3">
                <div class="flex items-center gap-2">
                  <span class="text-sm">🔍</span>
                  <span class="text-xs font-bold uppercase tracking-wider text-white">Live Request Inspector</span>
                </div>
                <div class="flex items-center gap-2">
                  <span id="pgInspectorMethod" class="px-2 py-0.5 rounded bg-cyan-950 border border-cyan-800 text-cyan-300 text-[10px] font-bold mono">POST</span>
                  <span id="pgInspectorStatus" class="px-2 py-0.5 rounded bg-slate-900 border border-slate-700 text-slate-400 text-[10px] font-semibold mono">IDLE</span>
                </div>
              </div>

              <!-- Endpoint Path & Latency -->
              <div class="flex items-center justify-between text-xs mono p-2.5 bg-slate-950 border border-slate-800/80 rounded">
                <div class="truncate text-slate-300 font-semibold" id="pgInspectorPath">/v1/set</div>
                <div class="text-cyan-400 font-bold ml-2 flex-shrink-0" id="pgInspectorLatency">— ms</div>
              </div>

              <!-- Request Payload Box -->
              <div class="space-y-1.5">
                <div class="flex items-center justify-between text-[11px] mono text-slate-400">
                  <span class="font-semibold uppercase text-[10px] text-slate-500">Request Headers &amp; Payload</span>
                  <button type="button" onclick="copyPlaygroundJson('pgInspectorReqPre', this)" class="text-[10px] text-indigo-400 hover:text-indigo-300 transition">Copy JSON</button>
                </div>
                <pre id="pgInspectorReqPre" class="bg-slate-950 border border-slate-800/90 rounded p-3 text-[11px] mono text-cyan-300 overflow-x-auto max-h-[160px] leading-relaxed font-mono">{
  "key": "user_session_99",
  "value": "eyJ1c2VySWQiOiAiYWRtaW4iLCAicm9sZSI6ICJzeXNvcHMifQ=="
}</pre>
              </div>

              <!-- Response Payload Box -->
              <div class="space-y-1.5">
                <div class="flex items-center justify-between text-[11px] mono text-slate-400">
                  <span class="font-semibold uppercase text-[10px] text-slate-500">Response Body</span>
                  <button type="button" onclick="copyPlaygroundJson('pgInspectorRespPre', this)" class="text-[10px] text-emerald-400 hover:text-emerald-300 transition">Copy Response</button>
                </div>
                <pre id="pgInspectorRespPre" class="bg-slate-950 border border-slate-800/90 rounded p-3 text-[11px] mono text-emerald-400 overflow-x-auto max-h-[220px] leading-relaxed font-mono">{
  "status": "ready",
  "message": "Execute any operation on the left to inspect real live responses."
}</pre>
              </div>

              <!-- Ranked Visual Results (if applicable) -->
              <div id="pgRankedResultsBox" class="hidden pt-2 border-t border-slate-800 space-y-2">
                <div class="text-[11px] uppercase font-semibold text-pink-400 mono">Ranked Similarity Output</div>
                <div id="pgRankedResultsList" class="space-y-2"></div>
              </div>
            </div>

            <!-- SDK Code Generator Card -->
            <div class="card-panel rounded-lg p-4 shadow-sm space-y-2.5 border border-slate-800/90">
              <div class="flex items-center justify-between text-[11px] mono">
                <span class="text-xs font-bold text-slate-300">Equivalent SDK Code</span>
                <button type="button" onclick="copyPlaygroundJson('pgSdkCodePre', this)" class="text-[10px] text-indigo-400 hover:text-indigo-300">Copy Code</button>
              </div>
              <pre id="pgSdkCodePre" class="bg-slate-950 border border-slate-800 rounded p-3 text-[11px] mono text-slate-300 overflow-x-auto"><code>// TypeScript SDK
await db.kv.set("user_session_99", "eyJ1c2VySWQiOiAiYWRtaW4iLCAicm9sZSI6ICJzeXNvcHMifQ==");</code></pre>
            </div>

          </div>

        </div>

      </section>

    </main>
  </div>

  <!-- ==================================================================== -->
  <!-- CLIENT-SIDE ROUTING & LIVE TELEMETRY LOGIC -->
  <!-- ==================================================================== -->
  <script>
    const VIEWS = ['overview', 'agents', 'memory', 'state', 'vectors', 'transactions', 'cluster', 'activity', 'developer'];
    let currentSelectedAgentId = 'research-agent';
    let activeFleetFilter = 'all';
    let lastTelemetryData = null;

    // Deterministic in-memory registry keyed strictly by canonical agent_id
    // agent_id -> { id, category, task, step, tokens, lastOp, lastSeen, lastSeenTimestamp, stateLoaded }
    const discoveredAgentsMap = new Map();

    // Deterministic set of unique memory IDs per agent for idempotent counting
    // agent_id -> Set<string>
    const discoveredMemoriesMap = new Map();

    /**
     * Agent Classification Heuristic:
     * Categorizes agent namespaces as either 'Application' or 'Test'.
     * Note: AetherDB backend provides cryptographic tenant isolation and namespace partitioning,
     * but does not enforce an agent role enum in database metadata.
     * This classification is derived client-side based on conventional identifier prefixes:
     * - 'test-*', 'empty-*', 'mock-*', 'temp-*'
     * - Numeric timestamp suffixes (e.g. '*-1791216...')
     * All other agent namespaces are classified as 'Application'.
     */
    function isTestAgent(agentId) {
      if (!agentId) return false;
      const id = agentId.trim();
      if (/^(test-|empty-|mock-|temp-)/i.test(id)) return true;
      if (/-[0-9]{10,}$/.test(id)) return true;
      return false;
    }

    // Seed canonical application agent with truthful initial metadata
    discoveredAgentsMap.set('research-agent', {
      id: 'research-agent',
      category: 'Application',
      task: 'distributed systems research',
      step: 'Step 4',
      tokens: 0,
      lastOp: '—',
      lastSeen: 'N/A',
      lastSeenTimestamp: null,
      stateLoaded: false
    });
    discoveredMemoriesMap.set('research-agent', new Set());

    function getAgentMemoryCount(agentId) {
      return discoveredMemoriesMap.has(agentId) ? discoveredMemoriesMap.get(agentId).size : 0;
    }

    // Truthful status calculation:
    // - ACTIVE: genuine recent operation observed within the active window (60s)
    // - IDLE: genuine past activity timestamp exists for application agent but older than 60s
    // - DISCOVERED: namespace exists/observed in AetherDB with no runtime activity heartbeat
    function computeAgentStatus(agent) {
      if (agent.lastSeenTimestamp && (Date.now() - agent.lastSeenTimestamp) < 60000) {
        return 'ACTIVE';
      }
      if (agent.lastSeenTimestamp && agent.category === 'Application') {
        return 'IDLE';
      }
      return 'DISCOVERED';
    }

    function escapeHtml(str) {
      if (str === null || str === undefined) return '';
      return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
    }

    function switchTab(viewId) {
      if (!VIEWS.includes(viewId)) viewId = 'overview';
      VIEWS.forEach(v => {
        const sec = document.getElementById('view-' + v);
        const nav = document.getElementById('nav-' + v);
        if (sec) sec.classList.toggle('hidden', v !== viewId);
        if (nav) {
          if (v === viewId) {
            nav.className = 'nav-active flex items-center gap-2.5 px-3 py-2 rounded transition';
          } else {
            nav.className = 'nav-inactive flex items-center gap-2.5 px-3 py-2 rounded transition';
          }
        }
      });
      window.location.hash = viewId;
      if (viewId === 'agents') {
        renderDiscoveredAgentsFleet();
      }
      if (viewId === 'memory') {
        renderMemoryExplorerFleet();
      }
      if (viewId === 'cluster') {
        renderClusterObservability(lastTelemetryData);
      }
      if (viewId === 'developer') {
        updatePlaygroundInspectorPreview();
      }
    }

    // Sync routing with URL hash on load
    window.addEventListener('DOMContentLoaded', () => {
      const hash = window.location.hash.replace('#', '');
      switchTab(hash || 'overview');
      // Set dynamic endpoint
      const endpointStr = window.location.origin || 'http://127.0.0.1:8301';
      const endpointEl = document.getElementById('headerEndpoint');
      if (endpointEl) endpointEl.textContent = endpointStr;
      
      updateTelemetry();
      fetchAgentInitialSummary('research-agent');
      inspectAgent(currentSelectedAgentId);
      renderMemoryExplorerFleet();
      renderClusterObservability(lastTelemetryData);
      updatePlaygroundInspectorPreview();
    });

    // Helper to format operation badges
    function getOpBadge(op) {
      const opStyles = {
        'AGENT STATE SET': 'text-cyan-300 bg-cyan-950/80 border-cyan-700/80',
        'AGENT STATE GET': 'text-slate-200 bg-slate-800 border-slate-700',
        'AGENT STATE DEL': 'text-rose-300 bg-rose-950/80 border-rose-700/80',
        'AGENT STATE INCR': 'text-purple-300 bg-purple-950/80 border-purple-700/80',
        'AGENT REMEMBER': 'text-indigo-300 bg-indigo-950/80 border-indigo-700/80',
        'AGENT RECALL': 'text-pink-300 bg-pink-950/80 border-pink-700/80',
        'SET': 'text-cyan-400 bg-cyan-950/60 border-cyan-800/80',
        'GET': 'text-slate-300 bg-slate-800/80 border-slate-700',
        'DEL': 'text-rose-400 bg-rose-950/60 border-rose-800/80',
        'INCR': 'text-purple-400 bg-purple-950/60 border-purple-800/80',
        'UPSERT VECTOR': 'text-indigo-400 bg-indigo-950/60 border-indigo-800/80',
        'VECTOR SEARCH': 'text-pink-400 bg-pink-950/60 border-pink-800/80',
      };
      const cls = opStyles[op] || 'text-slate-300 bg-slate-800 border-slate-700';
      return `<span class="px-1.5 py-0.5 rounded border text-[10px] font-medium mono ${cls}">${op}</span>`;
    }

    // Helper to extract canonical agent identifier and sub-target
    function parseAgentAndTarget(target, op) {
      if (target.startsWith('agent:')) {
        const parts = target.split(':');
        const agentId = parts[1] || 'agent';
        const subTarget = parts.slice(2).join(':') || 'state:root';
        return { agent: agentId, target: subTarget };
      }
      if (op && op.startsWith('AGENT')) {
        return { agent: 'research-agent', target: target };
      }
      return { agent: 'system/kv', target: target };
    }

    // Live Telemetry Polling (Every 2 seconds)
    async function updateTelemetry() {
      try {
        const res = await fetch('/v1/telemetry');
        if (!res.ok) throw new Error('Non-200 status: ' + res.status);
        const data = await res.json();

        // Mark as Connected
        setConnectionState(true);

        // Header & Sidebar
        if (data.cluster) {
          const leaderText = `Node ${data.cluster.leader_node}`;
          document.getElementById('sideNodeId').textContent = leaderText;
          document.getElementById('statLeaderNode').textContent = `${leaderText} (Leader)`;
          document.getElementById('sideRaftTerm').textContent = data.cluster.term;
          document.getElementById('statRaftTerm').textContent = data.cluster.term;
          document.getElementById('statCommitIndex').textContent = Number(data.cluster.commit_index).toLocaleString();
          document.getElementById('statReplLag').textContent = `${data.cluster.replication_lag_ms} ms`;
          document.getElementById('statClusterNodes').textContent = `${data.cluster.nodes_healthy} / ${data.cluster.nodes_total} online`;
          document.getElementById('statClusterHealth').textContent = data.cluster.nodes_healthy === data.cluster.nodes_total ? 'HEALTHY' : 'DEGRADED';
        }

        // Engine metrics
        if (data.engine) {
          document.getElementById('statRps').textContent = data.engine.requests_per_sec.toFixed(1);
          document.getElementById('statTotalReqs').textContent = Number(data.engine.requests_total).toLocaleString();
          document.getElementById('statP50').textContent = data.engine.p50_latency_ms.toFixed(2);
          document.getElementById('statP99').textContent = `${data.engine.p99_latency_ms.toFixed(2)} ms`;
          document.getElementById('statStorageSize').textContent = (data.engine.storage_bytes / 1048576).toFixed(1) + ' MB';
          document.getElementById('statWalSize').textContent = (data.engine.wal_bytes / 1024).toFixed(0) + ' KB';
        }

        // Agent Memory metrics
        if (data.agent_memory) {
          document.getElementById('statAgents').textContent = discoveredAgentsMap.size;
          document.getElementById('statVectors').textContent = Number(data.agent_memory.memory_vectors).toLocaleString();
          document.getElementById('statMemoriesStored').textContent = Number(data.agent_memory.memory_vectors).toLocaleString();
          document.getElementById('statTokenOps').textContent = Number(data.agent_memory.token_operations).toLocaleString();
          document.getElementById('statVecLatency').textContent = `${data.agent_memory.avg_search_latency_ms.toFixed(2)} ms`;
        }

        // Discover agents and memory traces deterministically from live activity stream
        if (data.live_activity && data.live_activity.length > 0) {
          data.live_activity.forEach(act => {
            const parsed = parseAgentAndTarget(act.target, act.op);
            const aId = parsed.agent;
            if (aId && aId !== 'system/kv' && aId !== 'agent') {
              if (!discoveredAgentsMap.has(aId)) {
                discoveredAgentsMap.set(aId, {
                  id: aId,
                  category: isTestAgent(aId) ? 'Test' : 'Application',
                  task: '—',
                  step: '—',
                  tokens: 0,
                  lastOp: act.op,
                  lastSeen: act.timestamp,
                  lastSeenTimestamp: Date.now(),
                  stateLoaded: false
                });
                if (!discoveredMemoriesMap.has(aId)) {
                  discoveredMemoriesMap.set(aId, new Set());
                }
                // Trigger a single one-time background fetch of session & tokens for this newly discovered agent
                fetchAgentInitialSummary(aId);
              } else {
                const existing = discoveredAgentsMap.get(aId);
                existing.lastOp = act.op;
                existing.lastSeen = act.timestamp;
                existing.lastSeenTimestamp = Date.now();
              }

              // Track unique memory IDs idempotently
              if (act.op === 'AGENT REMEMBER' || (parsed.target && parsed.target.startsWith('mem_'))) {
                if (!discoveredMemoriesMap.has(aId)) {
                  discoveredMemoriesMap.set(aId, new Set());
                }
                const memId = parsed.target.startsWith('mem_') ? parsed.target : `mem_${act.timestamp}`;
                discoveredMemoriesMap.get(aId).add(memId);
              }
            }
          });

          renderOverviewActivityTable(data.live_activity.slice(0, 8));
          renderFullActivityTable(data.live_activity.slice(0, 30));
          renderAgentDetailActivity(data.live_activity);
          renderAgentTokenHistory(data.live_activity);
        }

        lastTelemetryData = data;
        renderDiscoveredAgentsFleet();
        renderMemoryExplorerFleet();
        renderClusterObservability(data);

        // Update sync timestamp
        const now = new Date();
        const timeStr = now.toTimeString().split(' ')[0];
        const syncEl = document.getElementById('lastSyncTime');
        if (syncEl) syncEl.textContent = timeStr;

      } catch (e) {
        setConnectionState(false);
      }
    }

    function setConnectionState(isConnected) {
      const badge = document.getElementById('connectionStatusBadge');
      const dot = document.getElementById('connectionDot');
      const text = document.getElementById('connectionText');
      if (!badge || !dot || !text) return;

      if (isConnected) {
        badge.className = 'flex items-center gap-2 px-3 py-1 rounded bg-emerald-950/70 border border-emerald-800/80 text-emerald-400 text-[11px] font-medium transition-colors';
        dot.className = 'h-2 w-2 rounded-full bg-emerald-400 status-pulse';
        text.textContent = '● Connected';
      } else {
        badge.className = 'flex items-center gap-2 px-3 py-1 rounded bg-rose-950/80 border border-rose-800/80 text-rose-400 text-[11px] font-medium transition-colors';
        dot.className = 'h-2 w-2 rounded-full bg-rose-400';
        text.textContent = '● Disconnected';
      }
    }

    function renderOverviewActivityTable(items) {
      const tbody = document.getElementById('overviewActivityBody');
      if (!tbody) return;
      tbody.innerHTML = '';

      items.forEach(act => {
        const tr = document.createElement('tr');
        const parsed = parseAgentAndTarget(act.target, act.op);
        const statusColor = act.status >= 200 && act.status < 300 ? 'text-emerald-400' : 'text-rose-400';

        tr.innerHTML = `
          <td class="py-2 px-2.5 text-slate-400">${act.timestamp}</td>
          <td class="py-2 px-2.5 text-purple-300 font-medium">${parsed.agent}</td>
          <td class="py-2 px-2.5">${getOpBadge(act.op)}</td>
          <td class="py-2 px-2.5 text-slate-200 truncate max-w-[200px]" title="${act.target}">${parsed.target}</td>
          <td class="py-2 px-2.5 text-emerald-400">${act.latency_ms}ms</td>
          <td class="py-2 px-2.5"><span class="${statusColor} font-semibold">${act.status}</span></td>
        `;
        tbody.appendChild(tr);
      });
    }

    function renderFullActivityTable(items) {
      const tbody = document.getElementById('fullActivityBody');
      if (!tbody) return;
      tbody.innerHTML = '';

      items.forEach(act => {
        const tr = document.createElement('tr');
        const statusColor = act.status >= 200 && act.status < 300 ? 'text-emerald-400' : 'text-rose-400';

        tr.innerHTML = `
          <td class="py-2.5 px-3 text-slate-400">${act.timestamp}</td>
          <td class="py-2.5 px-3 text-slate-300">${act.tenant || 'default'}</td>
          <td class="py-2.5 px-3">${getOpBadge(act.op)}</td>
          <td class="py-2.5 px-3 text-slate-200">${act.target}</td>
          <td class="py-2.5 px-3 text-emerald-400">${act.latency_ms}ms</td>
          <td class="py-2.5 px-3"><span class="${statusColor} font-semibold">${act.status}</span></td>
        `;
        tbody.appendChild(tr);
      });
    }

    // ==========================================
    // AGENT FLEET DISCOVERY & DETAIL CONTROLLER
    // ==========================================
    function setAgentFleetFilter(mode) {
      activeFleetFilter = mode;
      const filters = ['all', 'app', 'test'];
      filters.forEach(f => {
        const btn = document.getElementById('filterBtn-' + f);
        if (btn) {
          if (f === mode) {
            btn.className = 'px-2.5 py-1 rounded bg-purple-600 text-white font-medium transition text-[11px]';
          } else {
            btn.className = 'px-2.5 py-1 rounded text-slate-400 hover:text-slate-200 transition text-[11px]';
          }
        }
      });
      renderDiscoveredAgentsFleet();
    }

    function renderDiscoveredAgentsFleet() {
      const tbody = document.getElementById('agentFleetTableBody');
      const badge = document.getElementById('agentsCountBadge');
      const subBadge = document.getElementById('agentsSubCountBadge');
      const breakdownEl = document.getElementById('statAgentBreakdown');
      const emptyState = document.getElementById('agentsEmptyState');
      const fleetList = document.getElementById('agentsFleetListContainer');
      if (!tbody) return;

      // Deterministic sort: research-agent first, then Applications, then Tests (alphabetically)
      const allAgentsList = Array.from(discoveredAgentsMap.values()).sort((a, b) => {
        if (a.id === 'research-agent') return -1;
        if (b.id === 'research-agent') return 1;
        if (a.category !== b.category) {
          return a.category === 'Application' ? -1 : 1;
        }
        return a.id.localeCompare(b.id);
      });

      const appAgents = allAgentsList.filter(a => a.category === 'Application');
      const testAgents = allAgentsList.filter(a => a.category === 'Test');

      const countAllEl = document.getElementById('filterCount-all');
      const countAppEl = document.getElementById('filterCount-app');
      const countTestEl = document.getElementById('filterCount-test');
      if (countAllEl) countAllEl.textContent = allAgentsList.length;
      if (countAppEl) countAppEl.textContent = appAgents.length;
      if (countTestEl) countTestEl.textContent = testAgents.length;

      if (breakdownEl) breakdownEl.textContent = `${appAgents.length} App · ${testAgents.length} Test`;
      if (subBadge) subBadge.textContent = `(${appAgents.length} Application, ${testAgents.length} Test)`;

      let filteredList = allAgentsList;
      if (activeFleetFilter === 'app') filteredList = appAgents;
      else if (activeFleetFilter === 'test') filteredList = testAgents;

      if (badge) badge.textContent = `${allAgentsList.length} Discovered`;

      if (allAgentsList.length === 0) {
        if (fleetList) fleetList.classList.add('hidden');
        if (emptyState) emptyState.classList.remove('hidden');
        return;
      }

      if (fleetList) fleetList.classList.remove('hidden');
      if (emptyState) emptyState.classList.add('hidden');

      tbody.innerHTML = '';
      if (filteredList.length === 0) {
        tbody.innerHTML = `<tr><td colspan="10" class="py-6 px-3 text-center text-slate-400 mono">No agents found under the "${activeFleetFilter}" filter.</td></tr>`;
        return;
      }

      for (const agent of filteredList) {
        const tr = document.createElement('tr');
        tr.className = agent.id === currentSelectedAgentId ? 'bg-purple-950/20' : 'hover:bg-slate-900/40';

        const catBadge = agent.category === 'Test'
          ? '<span class="px-1.5 py-0.5 rounded bg-slate-900 border border-slate-700 text-slate-400 text-[10px]">Test</span>'
          : '<span class="px-1.5 py-0.5 rounded bg-cyan-950/60 border border-cyan-800/80 text-cyan-300 text-[10px]">Application</span>';

        const status = computeAgentStatus(agent);
        let statusBadge;
        let dotClass = 'bg-slate-500';
        if (status === 'ACTIVE') {
          statusBadge = '<span class="px-1.5 py-0.5 rounded bg-emerald-950/60 border border-emerald-800/80 text-emerald-400 text-[10px]">● ACTIVE</span>';
          dotClass = 'bg-emerald-400';
        } else if (status === 'IDLE') {
          statusBadge = '<span class="px-1.5 py-0.5 rounded bg-amber-950/40 border border-amber-800/60 text-amber-300 text-[10px]">○ IDLE</span>';
          dotClass = 'bg-amber-400';
        } else {
          statusBadge = '<span class="px-1.5 py-0.5 rounded bg-slate-900 border border-slate-700 text-slate-400 text-[10px]">● DISCOVERED</span>';
          dotClass = 'bg-slate-500';
        }

        const memCount = getAgentMemoryCount(agent.id);
        const tokensStr = agent.tokens ? `${Number(agent.tokens).toLocaleString()} tok` : '0 tok';
        const lastSeenStr = agent.lastSeen || 'N/A';
        const lastOpStr = agent.lastOp || '—';

        tr.innerHTML = `
          <td class="py-2.5 px-3">
            <div class="flex items-center gap-2">
              <span class="h-1.5 w-1.5 rounded-full ${dotClass}"></span>
              <strong class="text-white font-semibold">${agent.id}</strong>
            </div>
          </td>
          <td class="py-2.5 px-3">${catBadge}</td>
          <td class="py-2.5 px-3">${statusBadge}</td>
          <td class="py-2.5 px-3 text-slate-300 truncate max-w-[160px]" id="fleet-task-${agent.id}">${agent.task}</td>
          <td class="py-2.5 px-3 text-purple-300" id="fleet-step-${agent.id}">${agent.step}</td>
          <td class="py-2.5 px-3 text-cyan-300 font-medium" id="fleet-tokens-${agent.id}">${tokensStr}</td>
          <td class="py-2.5 px-3 text-slate-400 mono text-xs" id="fleet-mem-${agent.id}">Not exposed</td>
          <td class="py-2.5 px-3 text-slate-400" id="fleet-op-${agent.id}">${lastOpStr}</td>
          <td class="py-2.5 px-3 text-slate-400" id="fleet-seen-${agent.id}">${lastSeenStr}</td>
          <td class="py-2.5 px-3 text-right">
            <button onclick="inspectAgent('${agent.id}')" class="px-2.5 py-1 rounded bg-purple-600/80 hover:bg-purple-500 text-white text-[10px] mono transition">
              Inspect →
            </button>
          </td>
        `;
        tbody.appendChild(tr);
      }
    }

    // One-time non-polling initial summary fetch upon discovering a new agent
    async function fetchAgentInitialSummary(agentId) {
      const agent = discoveredAgentsMap.get(agentId);
      if (!agent || agent.stateLoaded) return;
      try {
        const res = await fetch('/v1/agent/state/get', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ agent_id: agentId, key: 'session' })
        });
        const data = await res.json();
        
        if (data.found && data.state) {
          if (typeof data.state === 'object') {
            agent.task = data.state.task || data.state.status || 'Active Session';
            agent.step = data.state.step !== undefined ? `Step ${data.state.step}` : '—';
          } else {
            agent.task = String(data.state).slice(0, 30);
          }
        }
        
        // Fetch tokens
        const tokenRes = await fetch('/v1/agent/state/get', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ agent_id: agentId, key: 'tokens' })
        });
        const tokenData = await tokenRes.json();
        if (tokenData.found && tokenData.state !== null) {
          agent.tokens = tokenData.state;
        }

        agent.stateLoaded = true;
        renderDiscoveredAgentsFleet();
      } catch (e) {
        // Non-fatal background fetch
      }
    }

    function handleInspectAgentFromInput() {
      const input = document.getElementById('agentSearchInput');
      if (!input) return;
      const agentId = input.value.trim();
      if (!agentId) return alert('Agent ID is required');
      if (!discoveredAgentsMap.has(agentId)) {
        discoveredAgentsMap.set(agentId, {
          id: agentId,
          category: isTestAgent(agentId) ? 'Test' : 'Application',
          task: '—',
          step: '—',
          tokens: 0,
          lastOp: '—',
          lastSeen: 'N/A',
          lastSeenTimestamp: null,
          stateLoaded: false
        });
        fetchAgentInitialSummary(agentId);
      }
      inspectAgent(agentId);
    }

    function inspectAgent(agentId) {
      currentSelectedAgentId = agentId;
      if (!discoveredAgentsMap.has(agentId)) {
        discoveredAgentsMap.set(agentId, {
          id: agentId,
          category: isTestAgent(agentId) ? 'Test' : 'Application',
          task: '—',
          step: '—',
          tokens: 0,
          lastOp: '—',
          lastSeen: 'N/A',
          lastSeenTimestamp: null,
          stateLoaded: false
        });
      }

      const agent = discoveredAgentsMap.get(agentId);
      const title = document.getElementById('detailAgentTitle');
      const badge = document.getElementById('detailTenantBadge');
      if (title) title.textContent = agentId;
      if (badge) badge.textContent = `t:default:agent:${agentId}`;

      refreshCurrentAgentDetail();
      renderDiscoveredAgentsFleet();
    }

    // In-memory registry of inspected subkeys per agent
    const agentInspectedKeysMap = new Map(); // agentId -> Map<key, { val: any, updated: string, type: string }>

    function recordAgentInspectedKey(agentId, key, val, type) {
      if (!agentInspectedKeysMap.has(agentId)) {
        agentInspectedKeysMap.set(agentId, new Map());
      }
      const now = new Date();
      const timeStr = now.toTimeString().split(' ')[0];
      agentInspectedKeysMap.get(agentId).set(key, {
        val,
        updated: timeStr,
        type: type || (typeof val === 'object' ? 'JSON (Object)' : typeof val === 'number' ? 'Int64 Counter' : 'String/Blob')
      });
    }

    function renderAgentStateKeysTable() {
      const tbody = document.getElementById('agentStateKeysTableBody');
      if (!tbody) return;
      tbody.innerHTML = '';

      const agentId = currentSelectedAgentId;
      const keysMap = agentInspectedKeysMap.get(agentId);
      if (!keysMap || keysMap.size === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="py-4 px-2.5 text-center text-slate-400">No subkeys queried yet for this agent. Try GET State above.</td></tr>';
        return;
      }

      keysMap.forEach((meta, k) => {
        const tr = document.createElement('tr');
        let preview = typeof meta.val === 'object' ? JSON.stringify(meta.val) : String(meta.val);
        if (preview.length > 50) preview = preview.slice(0, 47) + '...';

        tr.innerHTML = `
          <td class="py-2 px-2.5 font-bold text-purple-300 mono">${k}</td>
          <td class="py-2 px-2.5 text-slate-300 mono truncate max-w-[200px]" title="${typeof meta.val === 'object' ? JSON.stringify(meta.val) : String(meta.val)}">${preview}</td>
          <td class="py-2 px-2.5 text-slate-400">${meta.updated}</td>
          <td class="py-2 px-2.5"><span class="px-1.5 py-0.5 rounded bg-slate-900 border border-slate-800 text-[10px] text-cyan-300 mono">${meta.type}</span></td>
          <td class="py-2 px-2.5 text-right">
            <button onclick="inspectStateSubkey('${k}')" class="px-2 py-0.5 rounded bg-slate-800 hover:bg-slate-700 text-slate-200 text-[10px] mono border border-slate-700">Inspect</button>
          </td>
        `;
        tbody.appendChild(tr);
      });
    }

    function inspectStateSubkey(key) {
      const input = document.getElementById('agentKeyInput');
      if (input) {
        input.value = key;
        handleAgentStateGet();
      }
    }

    async function refreshCurrentAgentDetail() {
      const agentId = currentSelectedAgentId;
      const agent = discoveredAgentsMap.get(agentId) || { id: agentId, tokens: 0 };
      try {
        const queryKey = document.getElementById('agentKeyInput').value.trim() || 'session';
        // 1. Fetch Session/Queried State
        const res = await fetch('/v1/agent/state/get', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ agent_id: agentId, key: queryKey })
        });
        const data = await res.json();
        
        document.getElementById('agentOutput').textContent = JSON.stringify(data, null, 2);

        if (data.found && data.state !== null && data.state !== undefined) {
          recordAgentInspectedKey(agentId, queryKey, data.state, typeof data.state === 'object' ? 'JSON (Object)' : 'Value');
          if (queryKey === 'session' || typeof data.state === 'object') {
            if (typeof data.state === 'object') {
              const task = data.state.task || data.state.status || 'Active Task';
              const step = data.state.step !== undefined ? `Step ${data.state.step}` : 'Step 1';
              agent.task = task;
              agent.step = step;
              document.getElementById('detailTask').textContent = task;
              document.getElementById('detailStep').textContent = step;
              document.getElementById('agentStateInput').value = JSON.stringify(data.state, null, 2);
            } else {
              agent.task = String(data.state);
              document.getElementById('detailTask').textContent = String(data.state);
              document.getElementById('agentStateInput').value = String(data.state);
            }
          }
        } else {
          if (queryKey === 'session') {
            document.getElementById('detailTask').textContent = 'No state set';
            document.getElementById('detailStep').textContent = '—';
          }
        }

        // 2. Fetch Token State
        const tokRes = await fetch('/v1/agent/state/get', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ agent_id: agentId, key: 'tokens' })
        });
        const tokData = await tokRes.json();
        const tokVal = tokData.found && tokData.state !== null ? tokData.state : 0;
        agent.tokens = tokVal;
        recordAgentInspectedKey(agentId, 'tokens', tokVal, 'Int64 Counter');
        document.getElementById('detailTokens').textContent = `${Number(tokVal).toLocaleString()} tokens`;
        document.getElementById('agentTokenBigDisplay').innerHTML = `${Number(tokVal).toLocaleString()} <span class="text-xs text-slate-400 font-normal">tokens</span>`;

        // 3. Update memory partition badge
        document.getElementById('detailMemCount').textContent = 'Active (Enumeration N/A)';
        renderAgentIngestedMemories();

        // 4. Update detail status badge dynamically
        const status = computeAgentStatus(agent);
        const statusBadgeEl = document.getElementById('detailAgentStatusBadge');
        if (statusBadgeEl) {
          if (status === 'ACTIVE') {
            statusBadgeEl.className = 'px-2 py-0.5 rounded bg-emerald-950/80 text-emerald-400 border border-emerald-800/80 text-[10px] mono font-semibold';
            statusBadgeEl.textContent = '● ACTIVE';
          } else if (status === 'IDLE') {
            statusBadgeEl.className = 'px-2 py-0.5 rounded bg-amber-950/60 text-amber-300 border border-amber-800/80 text-[10px] mono font-semibold';
            statusBadgeEl.textContent = '○ IDLE';
          } else {
            statusBadgeEl.className = 'px-2 py-0.5 rounded bg-slate-900 text-slate-400 border border-slate-700 text-[10px] mono font-semibold';
            statusBadgeEl.textContent = '● DISCOVERED';
          }
        }

        renderAgentStateKeysTable();
        renderDiscoveredAgentsFleet();
      } catch (e) {
        console.error('Error refreshing agent detail:', e);
      }
    }

    function switchAgentDetailTab(tabId) {
      const tabs = ['state', 'tokens', 'memory', 'recall', 'activity'];
      tabs.forEach(t => {
        const panel = document.getElementById('agentDetailTab-' + t);
        const btn = document.getElementById('agentTabBtn-' + t);
        if (panel) panel.classList.toggle('hidden', t !== tabId);
        if (btn) {
          if (t === tabId) {
            btn.className = 'py-2 px-3 border-b-2 border-purple-500 text-white font-semibold';
          } else {
            btn.className = 'py-2 px-3 border-b-2 border-transparent text-slate-400 hover:text-slate-200';
          }
        }
      });
    }

    function renderAgentDetailActivity(activities) {
      const tbody = document.getElementById('agentDetailActivityBody');
      if (!tbody) return;

      const agentPrefix = `agent:${currentSelectedAgentId}`;
      const filtered = activities.filter(a => a.target.startsWith(agentPrefix) || a.target.includes(currentSelectedAgentId));

      tbody.innerHTML = '';
      if (filtered.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="py-4 px-2.5 text-center text-slate-400">No activity recorded yet for this agent.</td></tr>';
        return;
      }

      filtered.slice(0, 15).forEach(act => {
        const tr = document.createElement('tr');
        const parsed = parseAgentAndTarget(act.target, act.op);
        const statusColor = act.status >= 200 && act.status < 300 ? 'text-emerald-400' : 'text-rose-400';

        tr.innerHTML = `
          <td class="py-2 px-2.5 text-slate-400">${act.timestamp}</td>
          <td class="py-2 px-2.5">${getOpBadge(act.op)}</td>
          <td class="py-2 px-2.5 text-slate-200 truncate max-w-[220px]" title="${act.target}">${parsed.target}</td>
          <td class="py-2 px-2.5 text-emerald-400">${act.latency_ms}ms</td>
          <td class="py-2 px-2.5"><span class="${statusColor} font-semibold">${act.status}</span></td>
        `;
        tbody.appendChild(tr);
      });
    }

    function renderAgentTokenHistory(activities) {
      const tbody = document.getElementById('agentTokenHistoryBody');
      if (!tbody) return;

      const agentPrefix = `agent:${currentSelectedAgentId}`;
      const incrs = activities.filter(a => a.op === 'INCR' && (a.target.startsWith(agentPrefix) || a.target.includes(currentSelectedAgentId)));

      tbody.innerHTML = '';
      if (incrs.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="py-4 px-2.5 text-center text-slate-400 italic">Historical token increments are not available from the current telemetry window.</td></tr>';
        return;
      }

      incrs.slice(0, 10).forEach(act => {
        const tr = document.createElement('tr');
        const parsed = parseAgentAndTarget(act.target, act.op);
        const statusColor = act.status >= 200 && act.status < 300 ? 'text-emerald-400' : 'text-rose-400';

        tr.innerHTML = `
          <td class="py-2 px-2.5 text-slate-400">${act.timestamp}</td>
          <td class="py-2 px-2.5">${getOpBadge('INCR')}</td>
          <td class="py-2 px-2.5 text-cyan-300 mono truncate max-w-[220px]" title="${act.target}">${parsed.target || act.target}</td>
          <td class="py-2 px-2.5 text-emerald-400">${act.latency_ms}ms</td>
          <td class="py-2 px-2.5"><span class="${statusColor} font-semibold">${act.status}</span></td>
        `;
        tbody.appendChild(tr);
      });
    }

    function applyAgentMemPreset(type) {
      const textEl = document.getElementById('agentMemTextInput');
      const vecEl = document.getElementById('agentMemEmbeddingInput');
      const metaEl = document.getElementById('agentMemMetaInput');
      if (type === 'systems') {
        if (textEl) textEl.value = "The user prefers Python and Rust for systems and AI.";
        if (vecEl) vecEl.value = "[0.92, 0.08, 0.0, 0.0, 0.15, -0.05, 0.32]";
        if (metaEl) metaEl.value = '{"source": "agent_console", "category": "preferences"}';
      } else if (type === 'arch') {
        if (textEl) textEl.value = "Distributed LSM-tree engine with Raft consensus architecture.";
        if (vecEl) vecEl.value = "[0.10, 0.88, 0.25, -0.10, 0.05, 0.40, -0.02]";
        if (metaEl) metaEl.value = '{"source": "agent_console", "category": "architecture"}';
      } else if (type === 'zero') {
        if (textEl) textEl.value = "Baseline initial state memory.";
        if (vecEl) vecEl.value = "[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]";
        if (metaEl) metaEl.value = '{"source": "agent_console", "category": "baseline"}';
      }
    }

    function applyAgentRecallPreset(type) {
      const qEl = document.getElementById('agentRecallQueryInput');
      const vecEl = document.getElementById('agentRecallQueryVecInput');
      if (type === 'systems') {
        if (qEl) qEl.value = "What programming languages does the user prefer?";
        if (vecEl) vecEl.value = "[0.95, 0.05, 0.0, 0.0, 0.12, -0.04, 0.30]";
      } else if (type === 'arch') {
        if (qEl) qEl.value = "How does AetherDB handle distributed consensus?";
        if (vecEl) vecEl.value = "[0.12, 0.85, 0.20, -0.08, 0.08, 0.42, 0.00]";
      }
    }

    function renderAgentIngestedMemories() {
      const container = document.getElementById('agentIngestedMemoriesList');
      if (!container) return;
      const mems = discoveredMemoriesMap.get(currentSelectedAgentId);
      if (!mems || mems.size === 0) {
        container.innerHTML = '<span class="text-slate-400 italic">No memories ingested in this session yet for this agent.</span>';
        return;
      }
      container.innerHTML = '<div class="flex flex-wrap gap-2"></div>';
      const inner = container.querySelector('div');
      Array.from(mems).forEach(mId => {
        const pill = document.createElement('span');
        pill.className = 'px-2.5 py-1 rounded bg-indigo-950/80 border border-indigo-800 text-indigo-300 text-xs mono flex items-center gap-1.5';
        pill.innerHTML = `<span>◈</span> <span>${mId}</span>`;
        inner.appendChild(pill);
      });
    }

    async function openVerifyPersistenceModal() {
      const modal = document.getElementById('verifyPersistenceModal');
      const nameEl = document.getElementById('verifyModalAgentName');
      if (nameEl) nameEl.textContent = currentSelectedAgentId;
      if (modal) modal.classList.remove('hidden');

      const sVal = document.getElementById('verifySessionVal');
      const sStat = document.getElementById('verifySessionStatus');
      const tVal = document.getElementById('verifyTokensVal');
      const tStat = document.getElementById('verifyTokensStatus');
      const mVal = document.getElementById('verifyMemoryVal');
      const mStat = document.getElementById('verifyMemoryStatus');

      if (sVal) sVal.textContent = 'Querying storage...';
      if (tVal) tVal.textContent = 'Querying storage...';
      if (mVal) mVal.textContent = 'Querying SIMD index...';

      const agentId = currentSelectedAgentId;

      // 1. Query KV session state
      try {
        const t0 = performance.now();
        const res = await fetch('/v1/agent/state/get', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ agent_id: agentId, key: 'session' }) });
        const d = await res.json();
        const ms = (performance.now() - t0).toFixed(1);
        if (d.found) {
          sVal.textContent = typeof d.state === 'object' ? JSON.stringify(d.state) : String(d.state);
          sStat.innerHTML = `<span class="text-emerald-400 font-semibold">FOUND (${ms}ms)</span>`;
        } else {
          sVal.textContent = 'No state set';
          sStat.innerHTML = `<span class="text-slate-500">EMPTY (${ms}ms)</span>`;
        }
      } catch (e) {
        sVal.textContent = 'Query Error';
        sStat.innerHTML = `<span class="text-rose-400">ERROR</span>`;
      }

      // 2. Query Atomic tokens
      try {
        const t0 = performance.now();
        const res = await fetch('/v1/agent/state/get', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ agent_id: agentId, key: 'tokens' }) });
        const d = await res.json();
        const ms = (performance.now() - t0).toFixed(1);
        const tok = d.found && d.state !== null ? d.state : 0;
        tVal.textContent = `${Number(tok).toLocaleString()} tokens`;
        tStat.innerHTML = `<span class="text-cyan-400 font-semibold">PERSISTED (${ms}ms)</span>`;
      } catch (e) {
        tVal.textContent = 'Query Error';
        tStat.innerHTML = `<span class="text-rose-400">ERROR</span>`;
      }

      // 3. Query Vector memory
      try {
        const t0 = performance.now();
        const res = await fetch('/v1/agent/memory/recall', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ agent_id: agentId, query: 'test probe', embedding: [0.92, 0.08, 0, 0, 0.15, -0.05, 0.32], top_k: 1 })
        });
        const d = await res.json();
        const ms = (performance.now() - t0).toFixed(1);
        if (d.results && d.results.length > 0) {
          const top = d.results[0];
          mVal.textContent = `Matched: "${top.memory_id}" (${(top.score * 100).toFixed(1)}%)`;
          mStat.innerHTML = `<span class="text-pink-400 font-semibold">MATCHED (${ms}ms)</span>`;
        } else {
          mVal.textContent = '0 matches for probe query (Enumeration not exposed)';
          mStat.innerHTML = `<span class="text-slate-400">NO MATCH (${ms}ms)</span>`;
        }
      } catch (e) {
        mVal.textContent = 'Query Error';
        mStat.innerHTML = `<span class="text-rose-400">ERROR</span>`;
      }
    }

    function closeVerifyPersistenceModal() {
      const modal = document.getElementById('verifyPersistenceModal');
      if (modal) modal.classList.add('hidden');
    }

    async function quickInitializeDefaultAgent() {
      discoveredAgentsMap.set('research-agent', {
        id: 'research-agent',
        category: 'Application',
        task: 'distributed systems research',
        step: 'Step 4',
        tokens: 0,
        lastOp: 'INITIALIZED',
        lastSeen: 'Live',
        lastSeenTimestamp: Date.now(),
        stateLoaded: true
      });
      currentSelectedAgentId = 'research-agent';
      await handleAgentStateSet();
      renderDiscoveredAgentsFleet();
      inspectAgent('research-agent');
    }

    function openPurgeModal() {
      const modal = document.getElementById('purgeConfirmModal');
      const nameEl = document.getElementById('purgeModalAgentName');
      const expectedEl = document.getElementById('purgeModalExpectedId');
      const input = document.getElementById('purgeConfirmInput');
      const btn = document.getElementById('confirmPurgeBtn');
      const errBanner = document.getElementById('purgeModalErrorBanner');

      if (nameEl) nameEl.textContent = currentSelectedAgentId;
      if (expectedEl) expectedEl.textContent = currentSelectedAgentId;
      if (input) {
        input.value = '';
        input.placeholder = `Type "${currentSelectedAgentId}" to confirm`;
      }
      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Purge State';
        btn.className = 'px-4 py-2 rounded bg-rose-900/30 text-rose-400/40 text-xs font-semibold mono transition shadow-sm cursor-not-allowed border border-rose-900/30';
      }
      if (errBanner) errBanner.classList.add('hidden');
      if (modal) modal.classList.remove('hidden');

      setTimeout(() => {
        if (input) input.focus();
      }, 50);
    }

    function handlePurgeInputChange() {
      const input = document.getElementById('purgeConfirmInput');
      const btn = document.getElementById('confirmPurgeBtn');
      if (!input || !btn) return;

      const typed = input.value.trim();
      const isMatch = (typed === currentSelectedAgentId);

      btn.disabled = !isMatch;
      if (isMatch) {
        btn.className = 'px-4 py-2 rounded bg-rose-600 hover:bg-rose-500 text-white text-xs font-semibold mono transition shadow-sm cursor-pointer border border-rose-500';
      } else {
        btn.className = 'px-4 py-2 rounded bg-rose-900/30 text-rose-400/40 text-xs font-semibold mono transition shadow-sm cursor-not-allowed border border-rose-900/30';
      }
    }

    function closePurgeModal() {
      const modal = document.getElementById('purgeConfirmModal');
      const input = document.getElementById('purgeConfirmInput');
      const errBanner = document.getElementById('purgeModalErrorBanner');
      if (input) input.value = '';
      if (errBanner) errBanner.classList.add('hidden');
      if (modal) modal.classList.add('hidden');
    }

    async function confirmPurgeCurrentAgent() {
      const input = document.getElementById('purgeConfirmInput');
      const typed = input ? input.value.trim() : '';
      const agentId = currentSelectedAgentId;

      if (typed !== agentId) {
        return; // Guard against bypass
      }

      const btn = document.getElementById('confirmPurgeBtn');
      const errBanner = document.getElementById('purgeModalErrorBanner');
      const errMsg = document.getElementById('purgeModalErrorMessage');

      if (btn) {
        btn.disabled = true;
        btn.textContent = 'Purging...';
      }

      try {
        const deleteKeys = ['session', 'tokens'];
        if (agentInspectedKeysMap.has(agentId)) {
          agentInspectedKeysMap.get(agentId).forEach((_, k) => {
            if (!deleteKeys.includes(k)) deleteKeys.push(k);
          });
        }

        // 1. Delete root state
        const resRoot = await fetch('/v1/agent/state/delete', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ agent_id: agentId })
        });
        if (!resRoot.ok) {
          const errData = await resRoot.json().catch(() => ({}));
          throw new Error(errData.error || `HTTP ${resRoot.status} while purging root state`);
        }
        const dataRoot = await resRoot.json();

        // 2. Delete all subkeys (session, tokens, etc.)
        const subkeyResults = {};
        for (const k of deleteKeys) {
          const r = await fetch('/v1/agent/state/delete', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ agent_id: agentId, key: k })
          });
          if (!r.ok) {
            const errData = await r.json().catch(() => ({}));
            throw new Error(errData.error || `HTTP ${r.status} while deleting subkey "${k}"`);
          }
          subkeyResults[k] = await r.json();
        }

        // 3. Purge inspected keys cache for this agent
        if (agentInspectedKeysMap.has(agentId)) {
          agentInspectedKeysMap.get(agentId).clear();
        }

        const agent = discoveredAgentsMap.get(agentId);
        if (agent) {
          agent.task = 'No state set';
          agent.step = '—';
          agent.tokens = 0;
        }

        // 4. Close modal on success
        closePurgeModal();

        // 5. Refresh selected agent state, token counter, activity, and display actual backend result
        const outputEl = document.getElementById('agentOutput');
        if (outputEl) {
          outputEl.textContent = JSON.stringify({
            status: "purged",
            agent_id: agentId,
            root_result: dataRoot,
            subkeys_purged: deleteKeys,
            details: subkeyResults,
            note: "State keys and token counter permanently purged. Semantic memories remain unaffected."
          }, null, 2);
        }

        await refreshCurrentAgentDetail();
        updateTelemetry();

      } catch (err) {
        console.error('Purge error:', err);
        if (errBanner && errMsg) {
          errMsg.textContent = err.message || 'An unexpected error occurred while communicating with AetherDB backend.';
          errBanner.classList.remove('hidden');
        }
        if (btn) {
          btn.disabled = false;
          btn.textContent = 'Purge State';
          btn.className = 'px-4 py-2 rounded bg-rose-600 hover:bg-rose-500 text-white text-xs font-semibold mono transition shadow-sm cursor-pointer border border-rose-500';
        }
      }
    }

    // Live telemetry interval set to 2000ms (no high-frequency polling or N+1 queries)
    setInterval(updateTelemetry, 2000);

    // ==========================================
    // KEY-VALUE ACTIONS
    // ==========================================
    async function handleSet() {
      const key = document.getElementById('kvKey').value.trim();
      const value = document.getElementById('kvVal').value.trim();
      if (!key) return alert('Key is required');
      const res = await fetch('/v1/set', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ key, value }) });
      const data = await res.json();
      document.getElementById('kvOutput').textContent = JSON.stringify(data, null, 2);
      updateTelemetry();
    }

    async function handleGet() {
      const key = document.getElementById('kvKey').value.trim();
      if (!key) return alert('Key is required');
      const res = await fetch('/v1/get', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ key }) });
      const data = await res.json();
      document.getElementById('kvOutput').textContent = JSON.stringify(data, null, 2);
      updateTelemetry();
    }

    async function handleDel() {
      const key = document.getElementById('kvKey').value.trim();
      if (!key) return alert('Key is required');
      const res = await fetch('/v1/del', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ key }) });
      const data = await res.json();
      document.getElementById('kvOutput').textContent = JSON.stringify(data, null, 2);
      updateTelemetry();
    }

    function setIncrAmount(val) {
      document.getElementById('incrAmount').value = val;
    }

    async function handleIncr() {
      const key = document.getElementById('incrKey').value.trim();
      const amount = parseInt(document.getElementById('incrAmount').value, 10) || 1;
      if (!key) return alert('Counter key required');
      const t0 = performance.now();
      const res = await fetch('/v1/incr', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ key, amount }) });
      const data = await res.json();
      const elapsed = (performance.now() - t0).toFixed(2);
      const val = data.value !== undefined ? data.value : data.new_value;
      document.getElementById('incrValDisplay').textContent = Number(val).toLocaleString();
      document.getElementById('incrLatencyDisplay').textContent = `${elapsed}ms`;
      updateTelemetry();
    }

    // ==========================================
    // AGENT ACTIONS
    // ==========================================
    async function handleAgentStateSet() {
      const agent_id = currentSelectedAgentId;
      const key = document.getElementById('agentKeyInput').value.trim() || undefined;
      let state;
      try {
        state = JSON.parse(document.getElementById('agentStateInput').value);
      } catch {
        return alert('Invalid JSON in state input');
      }
      const res = await fetch('/v1/agent/state/set', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ agent_id, key, state }) });
      const data = await res.json();
      document.getElementById('agentOutput').textContent = JSON.stringify(data, null, 2);
      if (key) {
        recordAgentInspectedKey(agent_id, key, state, typeof state === 'object' ? 'JSON (Object)' : 'Value');
      }
      refreshCurrentAgentDetail();
      updateTelemetry();
    }

    async function handleAgentStateGet() {
      const agent_id = currentSelectedAgentId;
      const key = document.getElementById('agentKeyInput').value.trim() || undefined;
      const res = await fetch('/v1/agent/state/get', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ agent_id, key }) });
      const data = await res.json();
      document.getElementById('agentOutput').textContent = JSON.stringify(data, null, 2);
      if (key && data.found) {
        recordAgentInspectedKey(agent_id, key, data.state, typeof data.state === 'object' ? 'JSON (Object)' : 'Value');
      }
      refreshCurrentAgentDetail();
      updateTelemetry();
    }

    async function handleAgentIncrTokens(amount) {
      const agent_id = currentSelectedAgentId;
      const res = await fetch('/v1/agent/state/incr', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ agent_id, key: 'tokens', amount }) });
      const data = await res.json();
      document.getElementById('agentOutput').textContent = JSON.stringify(data, null, 2);
      refreshCurrentAgentDetail();
      updateTelemetry();
    }

    async function handleAgentCustomIncr() {
      const amount = parseInt(document.getElementById('agentCustomIncrAmount').value, 10) || 1;
      await handleAgentIncrTokens(amount);
      const out = document.getElementById('agentIncrOutput');
      if (out) out.innerHTML = `<span class="text-emerald-400">✓ Increment +${amount} tokens committed atomically to agent "${currentSelectedAgentId}".</span>`;
    }

    async function handleAgentStateDel() {
      const agent_id = currentSelectedAgentId;
      const key = document.getElementById('agentKeyInput').value.trim() || undefined;
      if (!confirm(`Delete subkey "${key || 'root'}" for agent "${agent_id}"?`)) return;
      const res = await fetch('/v1/agent/state/delete', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ agent_id, key }) });
      const data = await res.json();
      document.getElementById('agentOutput').textContent = JSON.stringify(data, null, 2);
      if (key && agentInspectedKeysMap.has(agent_id)) {
        agentInspectedKeysMap.get(agent_id).delete(key);
      }
      refreshCurrentAgentDetail();
      updateTelemetry();
    }

    // ==========================================
    // AGENT SEMANTIC MEMORY ACTIONS
    // ==========================================
    async function handleAgentMemoryRemember() {
      const agent_id = currentSelectedAgentId;
      const memory_id = document.getElementById('agentMemIdInput').value.trim();
      const text = document.getElementById('agentMemTextInput').value.trim();
      const outEl = document.getElementById('agentMemRememberOutput');

      if (!memory_id) {
        if (outEl) outEl.innerHTML = '<span class="text-rose-400">Error: Memory ID is required.</span>';
        return;
      }
      if (!text) {
        if (outEl) outEl.innerHTML = '<span class="text-rose-400">Error: Memory text content is required.</span>';
        return;
      }
      
      let embedding;
      try {
        const rawVec = JSON.parse(document.getElementById('agentMemEmbeddingInput').value);
        if (!Array.isArray(rawVec) || rawVec.length === 0 || !rawVec.every(x => typeof x === 'number' && Number.isFinite(x))) {
          throw new Error();
        }
        embedding = rawVec;
      } catch {
        if (outEl) outEl.innerHTML = '<span class="text-rose-400">Error: Invalid float array in embedding input (must be non-empty array of numbers).</span>';
        return;
      }

      let metadata = { source: 'agent_console' };
      const metaInput = document.getElementById('agentMemMetaInput');
      if (metaInput && metaInput.value.trim()) {
        try {
          metadata = JSON.parse(metaInput.value.trim());
        } catch {
          if (outEl) outEl.innerHTML = '<span class="text-rose-400">Error: Invalid JSON in metadata input.</span>';
          return;
        }
      }

      try {
        const res = await fetch('/v1/agent/memory/remember', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ agent_id, memory_id, text, embedding, metadata })
        });
        
        if (!res.ok) {
          let errDetail = `HTTP ${res.status}`;
          try {
            const errJson = await res.json();
            if (errJson.error) errDetail = errJson.error;
          } catch {
            const errText = await res.text();
            if (errText) errDetail = errText;
          }
          if (outEl) outEl.innerHTML = `<span class="text-rose-400">Ingest failed (${res.status}): ${escapeHtml(errDetail)}</span>`;
          return;
        }

        const data = await res.json();
        if (!discoveredMemoriesMap.has(agent_id)) {
          discoveredMemoriesMap.set(agent_id, new Set());
        }
        discoveredMemoriesMap.get(agent_id).add(memory_id);
        
        if (outEl) {
          outEl.innerHTML = `<span class="text-emerald-400">✓ Memory "${escapeHtml(memory_id)}" stored successfully for agent "${escapeHtml(agent_id)}". (${embedding.length} dims)</span>`;
        }
        renderAgentIngestedMemories();
        updateTelemetry();
        refreshCurrentAgentDetail();
      } catch (e) {
        if (outEl) outEl.innerHTML = `<span class="text-rose-400">Ingest error: ${escapeHtml(e.message)}</span>`;
      }
    }

    async function handleAgentMemoryRecall() {
      const agent_id = currentSelectedAgentId;
      const query = document.getElementById('agentRecallQueryInput').value.trim();
      const container = document.getElementById('agentRecallResultsContainer');

      let embedding;
      try {
        const rawVec = JSON.parse(document.getElementById('agentRecallQueryVecInput').value);
        if (!Array.isArray(rawVec) || rawVec.length === 0 || !rawVec.every(x => typeof x === 'number' && Number.isFinite(x))) {
          throw new Error();
        }
        embedding = rawVec;
      } catch {
        if (container) container.innerHTML = '<div class="p-3 bg-rose-950/60 border border-rose-800 rounded text-xs text-rose-300 mono">Error: Invalid float array in query vector input.</div>';
        return;
      }

      const top_k = parseInt(document.getElementById('agentRecallTopKInput').value, 10) || 5;
      
      try {
        const res = await fetch('/v1/agent/memory/recall', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ agent_id, query, embedding, top_k })
        });
        
        if (!res.ok) {
          let errDetail = `HTTP ${res.status}`;
          try {
            const errJson = await res.json();
            if (errJson.error) errDetail = errJson.error;
          } catch {
            const errText = await res.text();
            if (errText) errDetail = errText;
          }
          if (container) container.innerHTML = `<div class="p-3 bg-rose-950/60 border border-rose-800 rounded text-xs text-rose-300 mono">Recall query failed (${res.status}): ${escapeHtml(errDetail)}</div>`;
          return;
        }

        const data = await res.json();
        if (container) {
          container.innerHTML = '';
          if (data.results && data.results.length > 0) {
            data.results.forEach((m, idx) => {
              const div = document.createElement('div');
              div.className = 'p-3 bg-slate-950 border border-slate-800 rounded flex justify-between items-center text-xs';
              const metaStr = m.metadata ? `<div class="text-[10px] text-slate-500 mono mt-1">meta: ${typeof m.metadata === 'object' ? JSON.stringify(m.metadata) : m.metadata}</div>` : '';
              div.innerHTML = `
                <div class="pr-3">
                  <div class="font-semibold text-white mono">#${idx+1} ${escapeHtml(m.memory_id)}</div>
                  <div class="text-slate-300 text-xs mt-1">${escapeHtml(m.text)}</div>
                  ${metaStr}
                </div>
                <div class="text-right flex-shrink-0">
                  <span class="mono text-pink-400 font-bold text-sm">${(m.score * 100).toFixed(1)}%</span>
                  <div class="text-[10px] text-slate-400">SIMD Cosine</div>
                </div>
              `;
              container.appendChild(div);
            });
          } else {
            container.innerHTML = `<div class="text-xs text-slate-400 mono p-3 bg-slate-950 border border-slate-800/80 rounded">No matching memories found for query in partition "agent:${escapeHtml(agent_id)}". (Note: Zero recall results does not mean zero memories exist in storage.)</div>`;
          }
        }
        updateTelemetry();
      } catch (e) {
        if (container) container.innerHTML = `<div class="p-3 bg-rose-950/60 border border-rose-800 rounded text-xs text-rose-300 mono">Recall error: ${escapeHtml(e.message)}</div>`;
      }
    }

    // =========================================================================
    // SEMANTIC MEMORY EXPLORER CONTROLLER
    // =========================================================================

    function renderMemoryExplorerFleet() {
      const totalEl = document.getElementById('memExplorerTotalVectors');
      if (totalEl) {
        totalEl.textContent = 'Not exposed';
      }
      const partEl = document.getElementById('memExplorerAgentPartitions');
      if (partEl) {
        partEl.textContent = `${discoveredAgentsMap.size} ${discoveredAgentsMap.size === 1 ? 'partition' : 'partitions'}`;
      }

      // 2. Populate Target Agent Selector dropdown
      const agentSelect = document.getElementById('memExplorerAgentSelect');
      if (agentSelect) {
        const currentVal = agentSelect.value;
        const agents = Array.from(discoveredAgentsMap.values()).sort((a, b) => {
          if (a.category !== b.category) return a.category === 'Application' ? -1 : 1;
          return a.id.localeCompare(b.id);
        });

        if (agents.length > 0) {
          agentSelect.innerHTML = agents.map(a => {
            return `<option value="${escapeHtml(a.id)}">${escapeHtml(a.id)} (${escapeHtml(a.category)})</option>`;
          }).join('');
          if (currentVal && Array.from(agentSelect.options).some(o => o.value === currentVal)) {
            agentSelect.value = currentVal;
          }
        }
      }

      // 3. Populate Agent Memory Partitions Table
      const invBody = document.getElementById('memExplorerInventoryBody');
      if (invBody) {
        const agents = Array.from(discoveredAgentsMap.values()).sort((a, b) => {
          if (a.category !== b.category) return a.category === 'Application' ? -1 : 1;
          return a.id.localeCompare(b.id);
        });

        if (agents.length === 0) {
          invBody.innerHTML = `<tr><td colspan="4" class="p-4 text-center text-slate-500">No agent namespaces discovered yet.</td></tr>`;
        } else {
          invBody.innerHTML = agents.map(a => {
            const isApp = a.category === 'Application';
            const badgeCls = isApp ? 'bg-cyan-950/80 border-cyan-700/80 text-cyan-300' : 'bg-slate-900 border-slate-700 text-slate-400';

            return `
              <tr class="hover:bg-slate-900/50 transition">
                <td class="py-2.5 px-2.5 font-semibold text-white">${escapeHtml(a.id)}</td>
                <td class="py-2.5 px-2.5">
                  <span class="px-1.5 py-0.5 rounded border text-[10px] mono ${badgeCls}">${a.category}</span>
                </td>
                <td class="py-2.5 px-2.5"><span class="text-slate-400 mono text-xs">Not exposed</span></td>
                <td class="py-2.5 px-2.5 text-right">
                  <button onclick="selectMemoryExplorerAgent('${escapeHtml(a.id)}')" class="px-2.5 py-1 rounded bg-slate-800 hover:bg-slate-700 text-indigo-300 text-[10px] mono border border-slate-700 transition">
                    Recall Agent
                  </button>
                </td>
              </tr>
            `;
          }).join('');
        }
      }
    }

    function selectMemoryExplorerAgent(agentId) {
      const select = document.getElementById('memExplorerAgentSelect');
      if (select) {
        select.value = agentId;
      }
      const ingestAgent = document.getElementById('memIngestAgentInput');
      if (ingestAgent) {
        ingestAgent.value = agentId;
      }
      const queryInput = document.getElementById('memExplorerQueryInput');
      if (queryInput) {
        queryInput.focus();
        queryInput.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }
    }

    function applyExplorerPreset(preset) {
      const qInput = document.getElementById('memExplorerQueryInput');
      const vecInput = document.getElementById('memExplorerVecInput');
      if (preset === 'systems') {
        if (qInput) qInput.value = 'What programming languages does the user prefer for AI?';
        if (vecInput) vecInput.value = '[0.95, 0.05, 0.0, 0.0, 0.12, -0.04, 0.30]';
      } else if (preset === 'arch') {
        if (qInput) qInput.value = 'What database architecture and consensus mechanism does the system use?';
        if (vecInput) vecInput.value = '[0.10, 0.88, 0.25, -0.10, 0.05, 0.40, -0.02]';
      } else if (preset === 'pref') {
        if (qInput) qInput.value = 'User preferences and development workflow requirements';
        if (vecInput) vecInput.value = '[0.85, 0.15, -0.05, 0.30, 0.0, 0.10, 0.20]';
      }
    }

    function applyIngestPreset(preset) {
      const textInput = document.getElementById('memIngestTextInput');
      const vecInput = document.getElementById('memIngestVecInput');
      const metaInput = document.getElementById('memIngestMetaInput');
      const idInput = document.getElementById('memIngestIdInput');
      if (preset === 'systems') {
        if (textInput) textInput.value = 'The user prefers Python and Rust for systems and AI development.';
        if (vecInput) vecInput.value = '[0.95, 0.05, 0.0, 0.0, 0.12, -0.04, 0.30]';
        if (metaInput) metaInput.value = '{"source": "console_ui", "category": "preferences"}';
        if (idInput) idInput.value = 'mem_systems_01';
      } else if (preset === 'arch') {
        if (textInput) textInput.value = 'Distributed LSM-tree engine with Raft consensus architecture.';
        if (vecInput) vecInput.value = '[0.10, 0.88, 0.25, -0.10, 0.05, 0.40, -0.02]';
        if (metaInput) metaInput.value = '{"source": "console_ui", "category": "architecture"}';
        if (idInput) idInput.value = 'mem_arch_01';
      }
    }

    async function handleExplorerRecall() {
      const agent_id = document.getElementById('memExplorerAgentSelect').value || 'research-agent';
      const query = document.getElementById('memExplorerQueryInput').value.trim();
      let embedding;
      try {
        const rawVec = JSON.parse(document.getElementById('memExplorerVecInput').value);
        if (!Array.isArray(rawVec) || rawVec.length === 0 || !rawVec.every(x => typeof x === 'number' && Number.isFinite(x))) {
          throw new Error('Must be non-empty array of valid numbers');
        }
        embedding = rawVec;
      } catch (e) {
        return alert('Invalid float array in query vector input');
      }
      const top_k = parseInt(document.getElementById('memExplorerTopK').value, 10) || 5;

      const container = document.getElementById('memExplorerResultsContainer');
      const badge = document.getElementById('memExplorerResultsBadge');
      const latEl = document.getElementById('memExplorerLatency');

      container.innerHTML = `
        <div class="p-6 bg-slate-950/60 border border-slate-800/80 rounded-lg text-center space-y-2">
          <div class="text-xs text-slate-400 mono animate-pulse">Executing AVX2 SIMD cosine similarity recall on partition "agent:${escapeHtml(agent_id)}"...</div>
        </div>
      `;

      const t0 = performance.now();
      try {
        const res = await fetch('/v1/agent/memory/recall', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ agent_id, query, embedding, top_k })
        });
        const elapsed = (performance.now() - t0).toFixed(2);
        if (!res.ok) {
          let errDetail = `HTTP ${res.status}`;
          try {
            const errJson = await res.json();
            if (errJson.error) errDetail = errJson.error;
          } catch {
            const errText = await res.text();
            if (errText) errDetail = errText;
          }
          container.innerHTML = `<div class="p-4 bg-rose-950/50 border border-rose-800 rounded-lg text-rose-300 text-xs mono">Recall query failed (${res.status}): ${escapeHtml(errDetail)}</div>`;
          if (badge) badge.classList.add('hidden');
          return;
        }
        const data = await res.json();
        
        if (latEl) latEl.textContent = `${elapsed} ms | AVX2 SIMD`;

        if (data.results && data.results.length > 0) {
          if (badge) {
            badge.textContent = `${data.results.length} memories ranked`;
            badge.classList.remove('hidden');
          }

          // Track discovered memories in map
          if (!discoveredMemoriesMap.has(agent_id)) {
            discoveredMemoriesMap.set(agent_id, new Set());
          }
          data.results.forEach(r => {
            if (r.memory_id) discoveredMemoriesMap.get(agent_id).add(r.memory_id);
          });

          container.innerHTML = data.results.map((m, idx) => {
            const scorePct = (m.score * 100).toFixed(1);
            const scoreExact = (m.score * 100).toFixed(2);
            const clampedScore = Math.min(100, Math.max(0, m.score * 100)).toFixed(1);
            const metaJson = m.metadata ? JSON.stringify(m.metadata) : null;
            const metaPreview = metaJson ? escapeHtml(metaJson) : 'None';
            const recordObj = {
              memory_id: m.memory_id,
              agent_id: agent_id,
              text: m.text,
              metadata: m.metadata || {},
              score: m.score
            };
            const recordJson = JSON.stringify(recordObj).replace(/'/g, "&apos;").replace(/"/g, "&quot;");

            return `
              <div class="p-4 bg-slate-950/80 border border-slate-800/90 rounded-lg space-y-3 hover:border-slate-700 transition">
                <div class="flex items-start justify-between gap-3">
                  <div class="space-y-1.5 flex-1 min-w-0">
                    <div class="flex items-center gap-2 flex-wrap">
                      <span class="text-pink-400 font-bold mono text-xs">#${idx + 1}</span>
                      <span class="text-white font-mono font-semibold text-xs">${escapeHtml(m.memory_id)}</span>
                      <span class="text-[10px] px-1.5 py-0.5 rounded bg-purple-950 border border-purple-800/80 text-purple-300 mono">${escapeHtml(agent_id)}</span>
                      <span class="text-[10px] px-1.5 py-0.5 rounded bg-slate-900 border border-slate-800 text-slate-400 mono">DIM: ${embedding.length}</span>
                    </div>
                    <div class="text-slate-200 text-xs leading-relaxed font-sans">${escapeHtml(m.text)}</div>
                  </div>
                  <div class="text-right flex-shrink-0 min-w-[90px]">
                    <div class="text-sm font-bold mono text-pink-400">${scorePct}%</div>
                    <div class="text-[10px] text-slate-500 mono">SIMD Cosine</div>
                  </div>
                </div>

                <!-- Similarity Score Progress Indicator -->
                <div class="space-y-1">
                  <div class="flex justify-between text-[10px] text-slate-500 mono">
                    <span>Similarity Match</span>
                    <span class="text-pink-300">${scoreExact}%</span>
                  </div>
                  <div class="w-full h-1.5 bg-slate-900 rounded-full overflow-hidden border border-slate-800">
                    <div class="h-full bg-gradient-to-r from-pink-500 via-purple-500 to-indigo-500 rounded-full transition-all duration-500" style="width: ${clampedScore}%"></div>
                  </div>
                </div>

                <!-- Footer & Inspect Details Action -->
                <div class="flex items-center justify-between pt-2 border-t border-slate-900 text-[11px] mono">
                  <div class="text-slate-500 truncate max-w-sm">
                    <span class="text-slate-600">meta:</span> <span class="text-emerald-400/90">${metaPreview}</span>
                  </div>
                  <button onclick="openMemoryDetailModalJson('${recordJson}')" class="text-xs text-indigo-400 hover:text-indigo-300 transition flex items-center gap-1 font-semibold">
                    <span>Inspect Details</span> →
                  </button>
                </div>
              </div>
            `;
          }).join('');
        } else {
          if (badge) badge.classList.add('hidden');
          container.innerHTML = `
            <div class="p-6 bg-slate-950/60 border border-slate-800/80 rounded-lg text-center space-y-2">
              <div class="text-2xl opacity-40">🔍</div>
              <div class="text-xs text-slate-300 font-medium">No matching memories found for query vector in partition "agent:${escapeHtml(agent_id)}".</div>
              <div class="text-[11px] text-slate-500 mono">Zero recall matches does not mean zero memories exist. The query vector similarity may be below the top-K threshold or the partition has no matching embeddings.</div>
            </div>
          `;
        }

        renderMemoryExplorerFleet();
        updateTelemetry();
      } catch (e) {
        container.innerHTML = `<div class="p-4 bg-rose-950/50 border border-rose-800 rounded-lg text-rose-300 text-xs mono">Recall error: ${escapeHtml(e.message)}</div>`;
      }
    }

    async function handleExplorerIngest() {
      const agent_id = document.getElementById('memIngestAgentInput').value.trim() || 'research-agent';
      const memory_id = document.getElementById('memIngestIdInput').value.trim();
      const text = document.getElementById('memIngestTextInput').value.trim();
      const outEl = document.getElementById('memIngestOutput');

      if (!agent_id) {
        if (outEl) outEl.innerHTML = '<span class="text-rose-400 mono">Error: Target Agent ID is required.</span>';
        return;
      }
      if (!memory_id) {
        if (outEl) outEl.innerHTML = '<span class="text-rose-400 mono">Error: Memory ID is required.</span>';
        return;
      }
      if (!text) {
        if (outEl) outEl.innerHTML = '<span class="text-rose-400 mono">Error: Memory text content is required.</span>';
        return;
      }

      let metadata = {};
      try {
        const metaStr = document.getElementById('memIngestMetaInput').value.trim();
        if (metaStr) metadata = JSON.parse(metaStr);
      } catch (e) {
        if (outEl) outEl.innerHTML = '<span class="text-rose-400 mono">Error: Invalid JSON in metadata input.</span>';
        return;
      }
      let embedding;
      try {
        const rawVec = JSON.parse(document.getElementById('memIngestVecInput').value);
        if (!Array.isArray(rawVec) || rawVec.length === 0 || !rawVec.every(x => typeof x === 'number' && Number.isFinite(x))) {
          throw new Error();
        }
        embedding = rawVec;
      } catch (e) {
        if (outEl) outEl.innerHTML = '<span class="text-rose-400 mono">Error: Invalid float array in embedding input (must be non-empty array of numbers).</span>';
        return;
      }

      if (outEl) outEl.innerHTML = '<span class="text-slate-400 mono animate-pulse">Ingesting memory fact into AetherDB...</span>';

      try {
        const res = await fetch('/v1/agent/memory/remember', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ agent_id, memory_id, text, embedding, metadata })
        });
        if (!res.ok) {
          let errDetail = `HTTP ${res.status}`;
          try {
            const errJson = await res.json();
            if (errJson.error) errDetail = errJson.error;
          } catch {
            const errText = await res.text();
            if (errText) errDetail = errText;
          }
          if (outEl) outEl.innerHTML = `<span class="text-rose-400 mono">Ingest failed (${res.status}): ${escapeHtml(errDetail)}</span>`;
          return;
        }
        const data = await res.json();
        
        if (!discoveredMemoriesMap.has(agent_id)) {
          discoveredMemoriesMap.set(agent_id, new Set());
        }
        discoveredMemoriesMap.get(agent_id).add(memory_id);

        if (outEl) {
          outEl.innerHTML = `<span class="text-emerald-400 mono">✓ Memory "${escapeHtml(memory_id)}" stored successfully in partition "agent:${escapeHtml(agent_id)}". (${embedding.length} dims)</span>`;
        }
        
        renderMemoryExplorerFleet();
        updateTelemetry();
      } catch (e) {
        if (outEl) outEl.innerHTML = `<span class="text-rose-400 mono">Ingest error: ${escapeHtml(e.message)}</span>`;
      }
    }

    function openMemoryDetailModalJson(encodedJson) {
      try {
        const str = encodedJson.replace(/&quot;/g, '"').replace(/&apos;/g, "'");
        const record = JSON.parse(str);
        openMemoryDetailModal(record);
      } catch (e) {
        console.error('Failed to parse modal memory record', e);
      }
    }

    function openMemoryDetailModal(record) {
      if (!record) return;
      document.getElementById('modalMemId').textContent = record.memory_id || '—';
      document.getElementById('modalMemAgent').textContent = record.agent_id || '—';
      document.getElementById('modalMemText').textContent = record.text || '—';
      document.getElementById('modalMemKvKey').textContent = `__agent_mem:${record.agent_id}:${record.memory_id}`;
      document.getElementById('modalMemVecKey').textContent = `agent:${record.agent_id}:${record.memory_id}`;
      
      const metaEl = document.getElementById('modalMemMeta');
      if (metaEl) {
        metaEl.textContent = JSON.stringify(record.metadata || {}, null, 2);
      }

      const scoreRow = document.getElementById('modalMemScoreRow');
      const scoreEl = document.getElementById('modalMemScore');
      if (record.score !== undefined && record.score !== null) {
        if (scoreEl) scoreEl.textContent = `${(record.score * 100).toFixed(2)}%`;
        if (scoreRow) scoreRow.classList.remove('hidden');
      } else {
        if (scoreRow) scoreRow.classList.add('hidden');
      }

      const modal = document.getElementById('memoryDetailModal');
      if (modal) modal.classList.remove('hidden');
    }

    function closeMemoryDetailModal() {
      const modal = document.getElementById('memoryDetailModal');
      if (modal) modal.classList.add('hidden');
    }

    // =========================================================================
    // CLUSTER & INFRASTRUCTURE OBSERVABILITY CONTROLLER
    // =========================================================================

    function renderClusterObservability(data) {
      if (!data) return;

      // 1. Cluster Overview strip
      if (data.cluster) {
        const isHealthy = data.cluster.nodes_healthy === data.cluster.nodes_total;
        const healthEl = document.getElementById('clusterObsHealth');
        if (healthEl) {
          healthEl.textContent = isHealthy ? 'HEALTHY' : 'DEGRADED';
          healthEl.className = isHealthy ? 'text-sm font-bold text-emerald-400 mono' : 'text-sm font-bold text-amber-400 mono';
        }

        const nodesEl = document.getElementById('clusterObsNodes');
        if (nodesEl) nodesEl.textContent = `${data.cluster.nodes_healthy} / ${data.cluster.nodes_total} Online`;

        const leaderEl = document.getElementById('clusterObsLeader');
        if (leaderEl) leaderEl.textContent = `Node ${data.cluster.leader_node}`;

        const termEl = document.getElementById('clusterObsTerm');
        if (termEl) termEl.textContent = `Term ${data.cluster.term}`;

        const commitEl = document.getElementById('clusterObsCommit');
        if (commitEl) commitEl.textContent = `#${Number(data.cluster.commit_index).toLocaleString()}`;

        const lagEl = document.getElementById('clusterObsLag');
        if (lagEl) lagEl.textContent = `${data.cluster.replication_lag_ms} ms (Sync)`;

        // Raft topology leader text
        const raftLeaderLabel = document.getElementById('raftLeaderLabel');
        if (raftLeaderLabel) raftLeaderLabel.textContent = `Node ${data.cluster.leader_node} (Leader - 127.0.0.1:8300)`;
        const raftLeaderMeta = document.getElementById('raftLeaderMeta');
        if (raftLeaderMeta) raftLeaderMeta.textContent = `Term ${data.cluster.term} • Commit #${Number(data.cluster.commit_index).toLocaleString()} • Consensus Proposer`;
      }

      // 2. Storage Engine metrics
      if (data.engine) {
        const walKb = (data.engine.wal_bytes / 1024).toFixed(0);
        const walEl = document.getElementById('storageWalSize');
        if (walEl) walEl.textContent = `${walKb} KB`;

        const sstMb = (data.engine.storage_bytes / 1048576).toFixed(1);
        const sstEl = document.getElementById('storageSstSize');
        if (sstEl) sstEl.textContent = `${sstMb} MB on disk`;

        // Request traffic stats
        const rateEl = document.getElementById('obsReqRate');
        if (rateEl) rateEl.textContent = `${data.engine.requests_per_sec.toFixed(1)} req/s`;

        const reqsEl = document.getElementById('obsTotalReqs');
        if (reqsEl) reqsEl.textContent = `${Number(data.engine.requests_total).toLocaleString()} total reqs`;

        const p50El = document.getElementById('obsP50');
        if (p50El) p50El.textContent = `${data.engine.p50_latency_ms.toFixed(2)} ms`;

        const p99El = document.getElementById('obsP99');
        if (p99El) p99El.textContent = `${data.engine.p99_latency_ms.toFixed(2)} ms`;
      }

      // 3. Vector count
      if (data.agent_memory) {
        const vecCountEl = document.getElementById('storageVectorCount');
        if (vecCountEl) {
          const count = data.agent_memory.memory_vectors;
          vecCountEl.textContent = `${count} ${count === 1 ? 'vector' : 'vectors'} indexed`;
        }
      }

      // 4. Operation breakdown table
      const opTableBody = document.getElementById('clusterOpBreakdownBody');
      if (opTableBody && data.live_activity) {
        const opCounts = {};
        const totalOps = data.live_activity.length || 1;
        data.live_activity.forEach(act => {
          const baseOp = act.op || 'OTHER';
          opCounts[baseOp] = (opCounts[baseOp] || 0) + 1;
        });

        const sortedOps = Object.entries(opCounts).sort((a, b) => b[1] - a[1]);
        if (sortedOps.length === 0) {
          opTableBody.innerHTML = `<tr><td colspan="3" class="py-2 text-slate-500 text-center">No traffic recorded yet.</td></tr>`;
        } else {
          opTableBody.innerHTML = sortedOps.map(([op, count]) => {
            const pct = ((count / totalOps) * 100).toFixed(0);
            let path = 'LSM State Machine';
            if (op.includes('VECTOR') || op.includes('REMEMBER') || op.includes('RECALL')) {
              path = 'HNSW Vector Engine';
            } else if (op.includes('INCR')) {
              path = 'Atomic Counter';
            }
            return `
              <tr class="hover:bg-slate-900/40 transition">
                <td class="py-1.5 px-2 font-semibold text-white">${getOpBadge(op)}</td>
                <td class="py-1.5 px-2 text-slate-400">${path}</td>
                <td class="py-1.5 px-2 text-right">
                  <span class="text-pink-400 font-bold">${pct}%</span>
                  <span class="text-slate-500 text-[10px]">(${count})</span>
                </td>
              </tr>
            `;
          }).join('');
        }
      }
    }

    // ==========================================
    // VECTOR ACTIONS
    // ==========================================
    async function handleVecUpsert() {
      const id = document.getElementById('vecId').value.trim();
      let vector;
      try { vector = JSON.parse(document.getElementById('vecFloats').value); } catch { return alert('Invalid vector array'); }
      const res = await fetch('/v1/vector/upsert', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ id, vector, metadata: JSON.stringify({ source: 'console' }) }) });
      const data = await res.json();
      document.getElementById('vecResults').innerHTML = `<div class="p-2.5 bg-emerald-950/50 border border-emerald-800 rounded text-xs text-emerald-400 mono">✓ Vector "${id}" (${vector.length} dims) upserted.</div>`;
      updateTelemetry();
    }

    async function handleVecSearch() {
      let vector;
      try { vector = JSON.parse(document.getElementById('vecFloats').value); } catch { return alert('Invalid vector array'); }
      const res = await fetch('/v1/vector/search', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ vector, top_k: 5 }) });
      const data = await res.json();
      const container = document.getElementById('vecResults');
      container.innerHTML = '';
      if (data.results && data.results.length > 0) {
        data.results.forEach((r, idx) => {
          const div = document.createElement('div');
          div.className = 'p-2.5 bg-slate-950 border border-slate-800 rounded flex justify-between items-center text-xs';
          div.innerHTML = `
            <div>
              <div class="font-semibold text-white mono">${r.id}</div>
              <div class="text-slate-400 text-[10px]">Rank #${idx+1}</div>
            </div>
            <div class="text-right">
              <span class="mono text-indigo-400 font-bold">${(r.score * 100).toFixed(2)}%</span>
              <div class="text-[10px] text-slate-400">Cosine Match</div>
            </div>
          `;
          container.appendChild(div);
        });
      } else {
        container.innerHTML = '<div class="text-xs text-slate-400 p-2 mono">No matching vectors found with dimension ' + vector.length + '.</div>';
      }
    }

    // =========================================================================
    // DEVELOPER PLAYGROUND & INTERACTIVE API CONSOLE CONTROLLER
    // =========================================================================

    let currentPgCategory = 'kv';
    let currentPgKvOp = 'SET';
    let currentPgAgentOp = 'SET';
    let currentPgMemOp = 'remember';
    let currentPgVecOp = 'upsert';

    function switchPlaygroundCategory(cat) {
      currentPgCategory = cat;
      const categories = ['kv', 'agentState', 'agentMemory', 'vector'];
      categories.forEach(c => {
        const btn = document.getElementById('pgTab-' + c);
        const panel = document.getElementById('pgPanel-' + c);
        if (panel) panel.classList.toggle('hidden', c !== cat);
        if (btn) {
          if (c === cat) {
            btn.className = 'flex-1 min-w-[110px] py-2 px-3 rounded bg-indigo-600 text-white font-semibold text-center transition shadow';
          } else {
            btn.className = 'flex-1 min-w-[110px] py-2 px-3 rounded bg-slate-900 text-slate-400 hover:text-white text-center transition';
          }
        }
      });
      updatePlaygroundInspectorPreview();
    }

    // 1. KV Actions
    function selectPgKvOp(op) {
      currentPgKvOp = op;
      ['SET', 'GET', 'INCR', 'DEL'].forEach(o => {
        const btn = document.getElementById('pgKvOpBtn-' + o);
        if (btn) {
          if (o === op) {
            btn.className = 'py-1.5 rounded bg-cyan-900/80 border border-cyan-500 text-cyan-300 font-bold';
          } else {
            btn.className = 'py-1.5 rounded bg-slate-900 border border-slate-800 text-slate-400 hover:text-slate-200';
          }
        }
      });

      const valGroup = document.getElementById('pgKvValGroup');
      const incrGroup = document.getElementById('pgKvIncrGroup');
      const delGuard = document.getElementById('pgKvDelGuard');

      if (valGroup) valGroup.classList.toggle('hidden', op !== 'SET');
      if (incrGroup) incrGroup.classList.toggle('hidden', op !== 'INCR');
      if (delGuard) delGuard.classList.toggle('hidden', op !== 'DEL');

      const submitBtn = document.getElementById('pgKvSubmitBtn');
      if (submitBtn) {
        if (op === 'DEL') {
          submitBtn.className = 'w-full bg-rose-700 hover:bg-rose-600 text-white text-xs font-semibold py-2.5 rounded transition mono flex items-center justify-center gap-2 shadow';
          submitBtn.innerHTML = '<span>⚠️</span> Delete Key from LSM-Tree';
        } else {
          submitBtn.className = 'w-full bg-cyan-600 hover:bg-cyan-500 text-white text-xs font-semibold py-2.5 rounded transition mono flex items-center justify-center gap-2 shadow';
          submitBtn.innerHTML = '<span>◈</span> Send ' + op + ' Request';
        }
      }

      updatePlaygroundInspectorPreview();
    }

    function loadPgKvPreset(type) {
      const keyInput = document.getElementById('pgKvKey');
      const valInput = document.getElementById('pgKvVal');
      const deltaInput = document.getElementById('pgKvDelta');

      if (type === 'session') {
        selectPgKvOp('SET');
        if (keyInput) keyInput.value = 'user_session_99';
        if (valInput) valInput.value = 'eyJ1c2VySWQiOiAiYWRtaW4iLCAicm9sZSI6ICJzeXNvcHMifQ==';
      } else if (type === 'config') {
        selectPgKvOp('SET');
        if (keyInput) keyInput.value = 'config:cluster:timeout_ms';
        if (valInput) valInput.value = '5000';
      } else if (type === 'counter') {
        selectPgKvOp('INCR');
        if (keyInput) keyInput.value = 'rate_limiter:t:default:hits';
        if (deltaInput) deltaInput.value = '1';
      }
      updatePlaygroundInspectorPreview();
    }

    async function executePlaygroundKV() {
      const key = document.getElementById('pgKvKey').value.trim();
      if (!key) return alert('Key is required');

      let endpoint = '/v1/set';
      let method = 'POST';
      let bodyObj = { key };

      if (currentPgKvOp === 'SET') {
        endpoint = '/v1/set';
        bodyObj.value = document.getElementById('pgKvVal').value;
      } else if (currentPgKvOp === 'GET') {
        endpoint = '/v1/get';
      } else if (currentPgKvOp === 'INCR') {
        endpoint = '/v1/incr';
        bodyObj.amount = parseInt(document.getElementById('pgKvDelta').value, 10) || 1;
      } else if (currentPgKvOp === 'DEL') {
        endpoint = '/v1/del';
        const checked = document.getElementById('pgKvDelConfirmCheck').checked;
        if (!checked) return alert('Please check the confirmation box to execute DELETE operation.');
      }

      const t0 = performance.now();
      try {
        const res = await fetch(endpoint, {
          method: method,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(bodyObj)
        });
        const elapsed = (performance.now() - t0).toFixed(2);
        const data = await res.json();
        setPlaygroundInspectorResponse(method, endpoint, res.status, elapsed, bodyObj, data, null);
        updateTelemetry();
      } catch (e) {
        setPlaygroundInspectorResponse(method, endpoint, 500, (performance.now() - t0).toFixed(2), bodyObj, { error: e.message }, null);
      }
    }

    // 2. Agent State Actions
    function selectPgAgentOp(op) {
      currentPgAgentOp = op;
      ['SET', 'GET', 'INCR', 'DEL'].forEach(o => {
        const btn = document.getElementById('pgAgentOpBtn-' + o);
        if (btn) {
          if (o === op) {
            btn.className = 'py-1.5 rounded bg-purple-900/80 border border-purple-500 text-purple-300 font-bold';
          } else {
            btn.className = 'py-1.5 rounded bg-slate-900 border border-slate-800 text-slate-400 hover:text-slate-200';
          }
        }
      });

      const jsonGroup = document.getElementById('pgAgentJsonGroup');
      const incrGroup = document.getElementById('pgAgentIncrGroup');
      const delGuard = document.getElementById('pgAgentDelGuard');

      if (jsonGroup) jsonGroup.classList.toggle('hidden', op !== 'SET');
      if (incrGroup) incrGroup.classList.toggle('hidden', op !== 'INCR');
      if (delGuard) delGuard.classList.toggle('hidden', op !== 'DEL');

      const submitBtn = document.getElementById('pgAgentSubmitBtn');
      if (submitBtn) {
        if (op === 'DEL') {
          submitBtn.className = 'w-full bg-rose-700 hover:bg-rose-600 text-white text-xs font-semibold py-2.5 rounded transition mono flex items-center justify-center gap-2 shadow';
          submitBtn.innerHTML = '<span>⚠️</span> Delete Agent State Key';
        } else {
          submitBtn.className = 'w-full bg-purple-600 hover:bg-purple-500 text-white text-xs font-semibold py-2.5 rounded transition mono flex items-center justify-center gap-2 shadow';
          submitBtn.innerHTML = '<span>◈</span> Send Agent ' + op + ' Request';
        }
      }

      updatePlaygroundInspectorPreview();
    }

    function loadPgAgentStatePreset(type) {
      const agentInput = document.getElementById('pgAgentId');
      const keyInput = document.getElementById('pgAgentKey');
      const jsonInput = document.getElementById('pgAgentStateJson');
      const deltaInput = document.getElementById('pgAgentDelta');

      if (agentInput) agentInput.value = 'research-agent';

      if (type === 'session') {
        selectPgAgentOp('SET');
        if (keyInput) keyInput.value = 'session';
        if (jsonInput) jsonInput.value = JSON.stringify({ task: 'distributed systems research', status: 'in-progress', step: 'Step 4' }, null, 2);
      } else if (type === 'plan') {
        selectPgAgentOp('SET');
        if (keyInput) keyInput.value = 'plan';
        if (jsonInput) jsonInput.value = JSON.stringify({ goals: ['verify linearizability', 'benchmark p99'], current_goal: 1 }, null, 2);
      } else if (type === 'tokens') {
        selectPgAgentOp('INCR');
        if (keyInput) keyInput.value = 'tokens';
        if (deltaInput) deltaInput.value = '100';
      }
      updatePlaygroundInspectorPreview();
    }

    function formatPgAgentJson() {
      const jsonInput = document.getElementById('pgAgentStateJson');
      if (!jsonInput) return;
      try {
        const parsed = JSON.parse(jsonInput.value);
        jsonInput.value = JSON.stringify(parsed, null, 2);
      } catch (e) {
        alert('Invalid JSON syntax: ' + e.message);
      }
    }

    async function executePlaygroundAgentState() {
      const agent_id = document.getElementById('pgAgentId').value.trim();
      const key = document.getElementById('pgAgentKey').value.trim();
      if (!agent_id) return alert('Agent ID is required');

      let endpoint = '/v1/agent/state/set';
      let method = 'POST';
      let bodyObj = { agent_id, key };

      if (currentPgAgentOp === 'SET') {
        endpoint = '/v1/agent/state/set';
        try {
          bodyObj.state = JSON.parse(document.getElementById('pgAgentStateJson').value);
        } catch (e) {
          return alert('Invalid JSON in state payload: ' + e.message);
        }
      } else if (currentPgAgentOp === 'GET') {
        endpoint = '/v1/agent/state/get';
      } else if (currentPgAgentOp === 'INCR') {
        endpoint = '/v1/agent/state/incr';
        bodyObj.delta = parseInt(document.getElementById('pgAgentDelta').value, 10) || 1;
      } else if (currentPgAgentOp === 'DEL') {
        endpoint = '/v1/agent/state/delete';
        const checked = document.getElementById('pgAgentDelConfirmCheck').checked;
        if (!checked) return alert('Please check the confirmation box to execute DELETE operation.');
      }

      const t0 = performance.now();
      try {
        const res = await fetch(endpoint, {
          method: method,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(bodyObj)
        });
        const elapsed = (performance.now() - t0).toFixed(2);
        const data = await res.json();
        setPlaygroundInspectorResponse(method, endpoint, res.status, elapsed, bodyObj, data, null);
        updateTelemetry();
      } catch (e) {
        setPlaygroundInspectorResponse(method, endpoint, 500, (performance.now() - t0).toFixed(2), bodyObj, { error: e.message }, null);
      }
    }

    // 3. Agent Memory Actions
    function selectPgMemoryOp(op) {
      currentPgMemOp = op;
      const btnRem = document.getElementById('pgMemOpBtn-remember');
      const btnRec = document.getElementById('pgMemOpBtn-recall');
      const remInputs = document.getElementById('pgMemRememberInputs');
      const recInputs = document.getElementById('pgMemRecallInputs');

      if (btnRem) {
        btnRem.className = op === 'remember' ? 'py-2 rounded bg-pink-900/80 border border-pink-500 text-pink-300 font-bold' : 'py-2 rounded bg-slate-900 border border-slate-800 text-slate-400 hover:text-slate-200 font-bold';
      }
      if (btnRec) {
        btnRec.className = op === 'recall' ? 'py-2 rounded bg-pink-900/80 border border-pink-500 text-pink-300 font-bold' : 'py-2 rounded bg-slate-900 border border-slate-800 text-slate-400 hover:text-slate-200 font-bold';
      }

      if (remInputs) remInputs.classList.toggle('hidden', op !== 'remember');
      if (recInputs) recInputs.classList.toggle('hidden', op !== 'recall');

      const submitBtn = document.getElementById('pgMemSubmitBtn');
      if (submitBtn) {
        submitBtn.innerHTML = op === 'remember' ? '<span>◈</span> Ingest Memory (remember)' : '<span>🔍</span> Execute Semantic Recall';
      }

      updatePlaygroundInspectorPreview();
    }

    function loadPgMemoryPreset(type) {
      if (type === 'systems') {
        selectPgMemoryOp('remember');
        document.getElementById('pgMemAgent').value = 'research-agent';
        document.getElementById('pgMemId').value = 'mem_' + Date.now().toString().slice(-4);
        document.getElementById('pgMemText').value = 'The user prefers Python and Rust for systems and AI.';
        document.getElementById('pgMemVec').value = '[0.95, 0.05, 0.0, 0.0, 0.12, -0.04, 0.30]';
        document.getElementById('pgMemMeta').value = '{"source": "playground", "category": "preferences"}';
      } else if (type === 'arch') {
        selectPgMemoryOp('remember');
        document.getElementById('pgMemAgent').value = 'research-agent';
        document.getElementById('pgMemId').value = 'mem_' + Date.now().toString().slice(-4);
        document.getElementById('pgMemText').value = 'Distributed LSM-tree engine with Raft consensus architecture.';
        document.getElementById('pgMemVec').value = '[0.10, 0.88, 0.25, -0.10, 0.05, 0.40, -0.02]';
        document.getElementById('pgMemMeta').value = '{"source": "playground", "category": "architecture"}';
      } else if (type === 'recall') {
        selectPgMemoryOp('recall');
        document.getElementById('pgRecallAgent').value = 'research-agent';
        document.getElementById('pgRecallQuery').value = 'What programming languages does the user prefer?';
        document.getElementById('pgRecallVec').value = '[0.95, 0.05, 0.0, 0.0, 0.12, -0.04, 0.30]';
      }
      updatePlaygroundInspectorPreview();
    }

    async function executePlaygroundAgentMemory() {
      let endpoint = '/v1/agent/memory/remember';
      let method = 'POST';
      let bodyObj = {};

      if (currentPgMemOp === 'remember') {
        endpoint = '/v1/agent/memory/remember';
        const agent_id = document.getElementById('pgMemAgent').value.trim();
        const memory_id = document.getElementById('pgMemId').value.trim();
        const text = document.getElementById('pgMemText').value.trim();
        let embedding, metadata = {};
        try {
          embedding = JSON.parse(document.getElementById('pgMemVec').value);
          if (!Array.isArray(embedding)) throw new Error('Array required');
        } catch (e) {
          return alert('Invalid float array in embedding');
        }
        try {
          const metaStr = document.getElementById('pgMemMeta').value.trim();
          if (metaStr) metadata = JSON.parse(metaStr);
        } catch (e) {
          return alert('Invalid JSON in metadata');
        }
        bodyObj = { agent_id, memory_id, text, embedding, metadata };
      } else {
        endpoint = '/v1/agent/memory/recall';
        const agent_id = document.getElementById('pgRecallAgent').value.trim();
        const query = document.getElementById('pgRecallQuery').value.trim();
        const top_k = parseInt(document.getElementById('pgRecallTopK').value, 10) || 5;
        let embedding;
        try {
          embedding = JSON.parse(document.getElementById('pgRecallVec').value);
          if (!Array.isArray(embedding)) throw new Error('Array required');
        } catch (e) {
          return alert('Invalid float array in query vector');
        }
        bodyObj = { agent_id, query, embedding, top_k };
      }

      const t0 = performance.now();
      try {
        const res = await fetch(endpoint, {
          method: method,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(bodyObj)
        });
        const elapsed = (performance.now() - t0).toFixed(2);
        const data = await res.json();
        
        // Track in discovered memories map
        if (currentPgMemOp === 'remember' && bodyObj.agent_id && bodyObj.memory_id) {
          if (!discoveredMemoriesMap.has(bodyObj.agent_id)) {
            discoveredMemoriesMap.set(bodyObj.agent_id, new Set());
          }
          discoveredMemoriesMap.get(bodyObj.agent_id).add(bodyObj.memory_id);
        }

        setPlaygroundInspectorResponse(method, endpoint, res.status, elapsed, bodyObj, data, data.results || null);
        updateTelemetry();
      } catch (e) {
        setPlaygroundInspectorResponse(method, endpoint, 500, (performance.now() - t0).toFixed(2), bodyObj, { error: e.message }, null);
      }
    }

    // 4. Vector Search Actions
    function selectPgVectorOp(op) {
      currentPgVecOp = op;
      const btnUpsert = document.getElementById('pgVecOpBtn-upsert');
      const btnSearch = document.getElementById('pgVecOpBtn-search');
      const idGroup = document.getElementById('pgVecIdGroup');
      const topKGroup = document.getElementById('pgVecTopKGroup');
      const metaGroup = document.getElementById('pgVecMetaGroup');

      if (btnUpsert) {
        btnUpsert.className = op === 'upsert' ? 'py-2 rounded bg-indigo-900/80 border border-indigo-500 text-indigo-300 font-bold' : 'py-2 rounded bg-slate-900 border border-slate-800 text-slate-400 hover:text-slate-200 font-bold';
      }
      if (btnSearch) {
        btnSearch.className = op === 'search' ? 'py-2 rounded bg-indigo-900/80 border border-indigo-500 text-indigo-300 font-bold' : 'py-2 rounded bg-slate-900 border border-slate-800 text-slate-400 hover:text-slate-200 font-bold';
      }

      if (idGroup) idGroup.classList.toggle('hidden', op !== 'upsert');
      if (topKGroup) topKGroup.classList.toggle('hidden', op !== 'search');
      if (metaGroup) metaGroup.classList.toggle('hidden', op !== 'upsert');

      const submitBtn = document.getElementById('pgVecSubmitBtn');
      if (submitBtn) {
        submitBtn.innerHTML = op === 'upsert' ? '<span>◈</span> Upsert Vector into HNSW Graph' : '<span>🔍</span> Search Nearest Vector Neighbors';
      }

      updatePlaygroundInspectorPreview();
    }

    function loadPgVectorPreset(type) {
      if (type === 'vec1') {
        selectPgVectorOp('upsert');
        document.getElementById('pgVecId').value = 'doc_vec_' + Date.now().toString().slice(-4);
        document.getElementById('pgVecFloats').value = '[0.10, 0.88, 0.25, -0.10, 0.05, 0.40, -0.02]';
        document.getElementById('pgVecMeta').value = '{"source": "playground", "topic": "systems_architecture"}';
      } else if (type === 'search') {
        selectPgVectorOp('search');
        document.getElementById('pgVecFloats').value = '[0.10, 0.88, 0.25, -0.10, 0.05, 0.40, -0.02]';
      }
      updatePlaygroundInspectorPreview();
    }

    async function executePlaygroundVector() {
      let endpoint = '/v1/vector/upsert';
      let method = 'POST';
      let bodyObj = {};

      if (currentPgVecOp === 'upsert') {
        endpoint = '/v1/vector/upsert';
        const id = document.getElementById('pgVecId').value.trim();
        let vector, metadata = {};
        try {
          vector = JSON.parse(document.getElementById('pgVecFloats').value);
          if (!Array.isArray(vector)) throw new Error('Array required');
        } catch (e) {
          return alert('Invalid vector float array');
        }
        try {
          const metaStr = document.getElementById('pgVecMeta').value.trim();
          if (metaStr) metadata = metaStr;
        } catch (e) {
          metadata = document.getElementById('pgVecMeta').value.trim();
        }
        bodyObj = { id, vector, metadata };
      } else {
        endpoint = '/v1/vector/search';
        const top_k = parseInt(document.getElementById('pgVecTopK').value, 10) || 5;
        let vector;
        try {
          vector = JSON.parse(document.getElementById('pgVecFloats').value);
          if (!Array.isArray(vector)) throw new Error('Array required');
        } catch (e) {
          return alert('Invalid vector float array');
        }
        bodyObj = { vector, top_k };
      }

      const t0 = performance.now();
      try {
        const res = await fetch(endpoint, {
          method: method,
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(bodyObj)
        });
        const elapsed = (performance.now() - t0).toFixed(2);
        const data = await res.json();
        setPlaygroundInspectorResponse(method, endpoint, res.status, elapsed, bodyObj, data, data.results || null);
        updateTelemetry();
      } catch (e) {
        setPlaygroundInspectorResponse(method, endpoint, 500, (performance.now() - t0).toFixed(2), bodyObj, { error: e.message }, null);
      }
    }

    // Inspector Helpers
    function updatePlaygroundInspectorPreview() {
      let method = 'POST';
      let endpoint = '/v1/set';
      let payload = {};
      let sdkCode = '';

      if (currentPgCategory === 'kv') {
        const key = document.getElementById('pgKvKey')?.value || 'user_session_99';
        if (currentPgKvOp === 'SET') {
          endpoint = '/v1/set';
          payload = { key, value: document.getElementById('pgKvVal')?.value || '' };
          sdkCode = `// TypeScript SDK\nawait db.kv.set("${key}", "${payload.value}");`;
        } else if (currentPgKvOp === 'GET') {
          endpoint = '/v1/get';
          payload = { key };
          sdkCode = `// TypeScript SDK\nconst val = await db.kv.get("${key}");`;
        } else if (currentPgKvOp === 'INCR') {
          endpoint = '/v1/incr';
          payload = { key, amount: parseInt(document.getElementById('pgKvDelta')?.value || '1', 10) };
          sdkCode = `// TypeScript SDK\nconst next = await db.kv.incr("${key}", ${payload.amount});`;
        } else if (currentPgKvOp === 'DEL') {
          endpoint = '/v1/del';
          payload = { key };
          sdkCode = `// TypeScript SDK\nawait db.kv.delete("${key}");`;
        }
      } else if (currentPgCategory === 'agentState') {
        const agent_id = document.getElementById('pgAgentId')?.value || 'research-agent';
        const key = document.getElementById('pgAgentKey')?.value || 'session';
        if (currentPgAgentOp === 'SET') {
          endpoint = '/v1/agent/state/set';
          try { payload = { agent_id, key, state: JSON.parse(document.getElementById('pgAgentStateJson')?.value || '{}') }; } catch (e) { payload = { agent_id, key, state: {} }; }
          sdkCode = `// TypeScript SDK\nconst agent = db.agent("${agent_id}");\nawait agent.state.set("${key}", ${JSON.stringify(payload.state)});\n`;
        } else if (currentPgAgentOp === 'GET') {
          endpoint = '/v1/agent/state/get';
          payload = { agent_id, key };
          sdkCode = `// TypeScript SDK\nconst agent = db.agent("${agent_id}");\nconst state = await agent.state.get("${key}");`;
        } else if (currentPgAgentOp === 'INCR') {
          endpoint = '/v1/agent/state/incr';
          payload = { agent_id, key, delta: parseInt(document.getElementById('pgAgentDelta')?.value || '1', 10) };
          sdkCode = `// TypeScript SDK\nconst agent = db.agent("${agent_id}");\nawait agent.state.incr("${key}", ${payload.delta});`;
        } else if (currentPgAgentOp === 'DEL') {
          endpoint = '/v1/agent/state/delete';
          payload = { agent_id, key };
          sdkCode = `// TypeScript SDK\nconst agent = db.agent("${agent_id}");\nawait agent.state.delete("${key}");`;
        }
      } else if (currentPgCategory === 'agentMemory') {
        if (currentPgMemOp === 'remember') {
          endpoint = '/v1/agent/memory/remember';
          const agent_id = document.getElementById('pgMemAgent')?.value || 'research-agent';
          const memory_id = document.getElementById('pgMemId')?.value || 'mem_001';
          const text = document.getElementById('pgMemText')?.value || '';
          let embedding = [];
          try { embedding = JSON.parse(document.getElementById('pgMemVec')?.value || '[]'); } catch (e) {}
          payload = { agent_id, memory_id, text, embedding, metadata: { source: 'playground' } };
          sdkCode = `// TypeScript SDK\nconst agent = db.agent("${agent_id}");\nawait agent.memory.remember({\n  id: "${memory_id}",\n  text: "${text}",\n  embedding: ${JSON.stringify(embedding)}\n});`;
        } else {
          endpoint = '/v1/agent/memory/recall';
          const agent_id = document.getElementById('pgRecallAgent')?.value || 'research-agent';
          const query = document.getElementById('pgRecallQuery')?.value || '';
          const top_k = parseInt(document.getElementById('pgRecallTopK')?.value || '5', 10);
          let embedding = [];
          try { embedding = JSON.parse(document.getElementById('pgRecallVec')?.value || '[]'); } catch (e) {}
          payload = { agent_id, query, embedding, top_k };
          sdkCode = `// TypeScript SDK\nconst agent = db.agent("${agent_id}");\nconst memories = await agent.memory.recall({\n  query: "${query}",\n  embedding: ${JSON.stringify(embedding)},\n  topK: ${top_k}\n});`;
        }
      } else if (currentPgCategory === 'vector') {
        if (currentPgVecOp === 'upsert') {
          endpoint = '/v1/vector/upsert';
          const id = document.getElementById('pgVecId')?.value || 'doc_01';
          let vector = [];
          try { vector = JSON.parse(document.getElementById('pgVecFloats')?.value || '[]'); } catch (e) {}
          payload = { id, vector, metadata: '{"source":"playground"}' };
          sdkCode = `// TypeScript SDK\nawait db.vector.upsert("${id}", ${JSON.stringify(vector)}, { source: "playground" });`;
        } else {
          endpoint = '/v1/vector/search';
          const top_k = parseInt(document.getElementById('pgVecTopK')?.value || '5', 10);
          let vector = [];
          try { vector = JSON.parse(document.getElementById('pgVecFloats')?.value || '[]'); } catch (e) {}
          payload = { vector, top_k };
          sdkCode = `// TypeScript SDK\nconst results = await db.vector.search(${JSON.stringify(vector)}, ${top_k});`;
        }
      }

      const methEl = document.getElementById('pgInspectorMethod');
      if (methEl) methEl.textContent = method;
      const pathEl = document.getElementById('pgInspectorPath');
      if (pathEl) pathEl.textContent = endpoint;
      const reqPre = document.getElementById('pgInspectorReqPre');
      if (reqPre) reqPre.textContent = JSON.stringify(payload, null, 2);
      const sdkPre = document.getElementById('pgSdkCodePre');
      if (sdkPre) sdkPre.innerHTML = `<code>${escapeHtml(sdkCode)}</code>`;
    }

    function setPlaygroundInspectorResponse(method, endpoint, status, latencyMs, reqBody, respData, rankedResults) {
      const methEl = document.getElementById('pgInspectorMethod');
      if (methEl) methEl.textContent = method;
      const pathEl = document.getElementById('pgInspectorPath');
      if (pathEl) pathEl.textContent = endpoint;
      const latEl = document.getElementById('pgInspectorLatency');
      if (latEl) latEl.textContent = `${latencyMs} ms`;

      const statEl = document.getElementById('pgInspectorStatus');
      if (statEl) {
        if (status >= 200 && status < 300) {
          statEl.textContent = `${status} OK`;
          statEl.className = 'px-2 py-0.5 rounded bg-emerald-950 border border-emerald-800 text-emerald-400 text-[10px] font-bold mono';
        } else {
          statEl.textContent = `${status} ERROR`;
          statEl.className = 'px-2 py-0.5 rounded bg-rose-950 border border-rose-800 text-rose-400 text-[10px] font-bold mono';
        }
      }

      const reqPre = document.getElementById('pgInspectorReqPre');
      if (reqPre) reqPre.textContent = JSON.stringify(reqBody, null, 2);

      const respPre = document.getElementById('pgInspectorRespPre');
      if (respPre) respPre.textContent = JSON.stringify(respData, null, 2);

      // Ranked Results Visualizer
      const resultsBox = document.getElementById('pgRankedResultsBox');
      const resultsList = document.getElementById('pgRankedResultsList');
      if (rankedResults && rankedResults.length > 0 && resultsBox && resultsList) {
        resultsBox.classList.remove('hidden');
        resultsList.innerHTML = rankedResults.map((r, idx) => {
          const scorePct = (r.score * 100).toFixed(1);
          const clamped = Math.min(100, Math.max(0, r.score * 100)).toFixed(1);
          const id = r.memory_id || r.id || `#${idx+1}`;
          const text = r.text || '';
          return `
            <div class="p-2.5 bg-slate-950 border border-slate-800 rounded text-xs mono space-y-1.5">
              <div class="flex justify-between items-center">
                <span class="font-bold text-white">${escapeHtml(id)}</span>
                <span class="text-pink-400 font-bold">${scorePct}% SIMD</span>
              </div>
              ${text ? `<div class="text-[11px] text-slate-300 font-sans">${escapeHtml(text)}</div>` : ''}
              <div class="w-full h-1 bg-slate-900 rounded-full overflow-hidden border border-slate-800">
                <div class="h-full bg-gradient-to-r from-pink-500 to-indigo-500" style="width: ${clamped}%"></div>
              </div>
            </div>
          `;
        }).join('');
      } else if (resultsBox) {
        resultsBox.classList.add('hidden');
      }
    }

    function copyPlaygroundJson(elementId, btn) {
      const el = document.getElementById(elementId);
      if (!el) return;
      const text = el.textContent || '';
      navigator.clipboard.writeText(text).then(() => {
        const orig = btn.textContent;
        btn.textContent = '✓ Copied!';
        setTimeout(() => { btn.textContent = orig; }, 1500);
      });
    }
  </script>
</body>
</html>"##;
