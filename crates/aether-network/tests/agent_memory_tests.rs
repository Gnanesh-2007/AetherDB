use std::net::SocketAddr;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use aether_network::{AuthManager, HttpServer};
use aether_storage::StorageEngine;

async fn send_http_post(addr: SocketAddr, path: &str, body: &str) -> (u16, serde_json::Value) {
    let mut stream = TcpStream::connect(addr).await.expect("Failed to connect");
    let req = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        path,
        addr,
        body.len(),
        body
    );
    stream
        .write_all(req.as_bytes())
        .await
        .expect("Failed to write request");

    let mut response_bytes = Vec::new();
    stream
        .read_to_end(&mut response_bytes)
        .await
        .expect("Failed to read response");
    let response_str = String::from_utf8_lossy(&response_bytes);

    let status_code = response_str
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(500);

    let body_part = response_str.split("\r\n\r\n").nth(1).unwrap_or("{}");

    let json_val: serde_json::Value =
        serde_json::from_str(body_part).unwrap_or(serde_json::Value::Null);
    (status_code, json_val)
}

async fn setup_test_server() -> (SocketAddr, tempfile::TempDir) {
    let dir = tempdir().expect("tempdir");
    let storage = Arc::new(StorageEngine::open(dir.path()).expect("open storage"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);

    let server = HttpServer::with_auth(addr, 1, storage, Arc::new(AuthManager::new(false)));
    tokio::spawn(async move {
        let _ = server.run().await;
    });

    // Give server a moment to start listening
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    (addr, dir)
}

#[tokio::test]
async fn test_agent_state_lifecycle_crud() {
    let (addr, _dir) = setup_test_server().await;

    // 1. Initial GET -> not found
    let (status, resp) = send_http_post(
        addr,
        "/v1/agent/state/get",
        r#"{"agent_id":"research-agent"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(resp["found"], false);
    assert!(resp["state"].is_null());

    // 2. SET Root State
    let (status, resp) = send_http_post(
        addr,
        "/v1/agent/state/set",
        r#"{"agent_id":"research-agent","state":{"task":"database research","status":"running","step":4}}"#,
    ).await;
    assert_eq!(status, 200);
    assert_eq!(resp["status"], "ok");
    assert_eq!(resp["agent_id"], "research-agent");

    // 3. GET Root State
    let (status, resp) = send_http_post(
        addr,
        "/v1/agent/state/get",
        r#"{"agent_id":"research-agent"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(resp["found"], true);
    assert_eq!(resp["state"]["task"], "database research");
    assert_eq!(resp["state"]["step"], 4);
    assert_eq!(resp["state"]["status"], "running");

    // 4. SET Subkey State
    let (status, resp) = send_http_post(
        addr,
        "/v1/agent/state/set",
        r#"{"agent_id":"research-agent","key":"scratchpad","state":{"notes":["found MVCC","indexed vector"]}}"#,
    ).await;
    assert_eq!(status, 200);
    assert_eq!(resp["status"], "ok");

    // 5. GET Subkey State
    let (status, resp) = send_http_post(
        addr,
        "/v1/agent/state/get",
        r#"{"agent_id":"research-agent","key":"scratchpad"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(resp["found"], true);
    assert_eq!(resp["state"]["notes"][0], "found MVCC");

    // 6. DELETE State
    let (status, resp) = send_http_post(
        addr,
        "/v1/agent/state/delete",
        r#"{"agent_id":"research-agent"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(resp["deleted"], true);

    // 7. GET after DELETE -> not found
    let (status, resp) = send_http_post(
        addr,
        "/v1/agent/state/get",
        r#"{"agent_id":"research-agent"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(resp["found"], false);
}

#[tokio::test]
async fn test_cross_agent_state_and_memory_isolation() {
    let (addr, _dir) = setup_test_server().await;

    // Agent Alpha stores state & memory
    let (status, _) = send_http_post(
        addr,
        "/v1/agent/state/set",
        r#"{"agent_id":"agent-alpha","state":{"secret":"alpha-token-xyz"}}"#,
    )
    .await;
    assert_eq!(status, 200);

    let (status, _) = send_http_post(
        addr,
        "/v1/agent/memory/remember",
        r#"{"agent_id":"agent-alpha","memory_id":"mem_01","text":"Alpha classified document","embedding":[1.0, 0.0, 0.0, 0.0]}"#,
    ).await;
    assert_eq!(status, 200);

    // Agent Beta attempts to read Alpha's state -> Must return found: false
    let (status, resp) =
        send_http_post(addr, "/v1/agent/state/get", r#"{"agent_id":"agent-beta"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(resp["found"], false);

    // Agent Beta attempts to recall with same embedding -> Must return empty results
    let (status, resp) = send_http_post(
        addr,
        "/v1/agent/memory/recall",
        r#"{"agent_id":"agent-beta","embedding":[1.0, 0.0, 0.0, 0.0],"top_k":5}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(resp["agent_id"], "agent-beta");
    assert_eq!(resp["results"].as_array().unwrap().len(), 0);

    // Agent Alpha recalls -> Gets its own memory
    let (status, resp) = send_http_post(
        addr,
        "/v1/agent/memory/recall",
        r#"{"agent_id":"agent-alpha","embedding":[1.0, 0.0, 0.0, 0.0],"top_k":5}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(resp["results"].as_array().unwrap().len(), 1);
    assert_eq!(resp["results"][0]["memory_id"], "mem_01");
    assert_eq!(resp["results"][0]["text"], "Alpha classified document");
}

#[tokio::test]
async fn test_agent_memory_remember_and_semantic_recall() {
    let (addr, _dir) = setup_test_server().await;

    // 1. Remember multiple memories
    let (status, _) = send_http_post(
        addr,
        "/v1/agent/memory/remember",
        r#"{"agent_id":"assistant-01","memory_id":"mem_py","text":"User prefers Python for AI development.","embedding":[1.0, 0.0, 0.0, 0.0],"metadata":{"category":"preferences"}}"#,
    ).await;
    assert_eq!(status, 200);

    let (status, _) = send_http_post(
        addr,
        "/v1/agent/memory/remember",
        r#"{"agent_id":"assistant-01","memory_id":"mem_rust","text":"AetherDB is written in high-performance Rust.","embedding":[0.0, 1.0, 0.0, 0.0],"metadata":{"category":"systems"}}"#,
    ).await;
    assert_eq!(status, 200);

    let (status, _) = send_http_post(
        addr,
        "/v1/agent/memory/remember",
        r#"{"agent_id":"assistant-01","memory_id":"mem_db","text":"Storage engines use LSM Trees and MemTables.","embedding":[0.0, 0.9, 0.1, 0.0],"metadata":{"category":"storage"}}"#,
    ).await;
    assert_eq!(status, 200);

    // 2. Semantic recall with embedding close to Rust & storage
    let (status, resp) = send_http_post(
        addr,
        "/v1/agent/memory/recall",
        r#"{"agent_id":"assistant-01","query":"What language is the database built in?","embedding":[0.05, 0.95, 0.05, 0.0],"top_k":2}"#,
    ).await;
    assert_eq!(status, 200);
    let results = resp["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);

    // Top result should be mem_rust
    assert_eq!(results[0]["memory_id"], "mem_rust");
    assert_eq!(
        results[0]["text"],
        "AetherDB is written in high-performance Rust."
    );
    assert_eq!(results[0]["metadata"]["category"], "systems");
    assert!(results[0]["score"].as_f64().unwrap() > 0.95);

    // Second result should be mem_db
    assert_eq!(results[1]["memory_id"], "mem_db");
}

#[tokio::test]
async fn test_agent_atomic_state_incr_concurrent() {
    let (addr, _dir) = setup_test_server().await;

    // Run 20 concurrent tasks doing atomic INCR on agent token quota
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let task_addr = addr;
        tasks.push(tokio::spawn(async move {
            let (status, resp) = send_http_post(
                task_addr,
                "/v1/agent/state/incr",
                r#"{"agent_id":"billing-agent","key":"tokens_used","amount":10}"#,
            )
            .await;
            assert_eq!(status, 200);
            resp["value"].as_i64().unwrap()
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }

    // Verify final state via get
    let (status, resp) = send_http_post(
        addr,
        "/v1/agent/state/get",
        r#"{"agent_id":"billing-agent","key":"tokens_used"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(resp["found"], true);
    assert_eq!(resp["state"], 200); // 20 tasks * 10 = 200
}

#[tokio::test]
async fn test_validation_errors() {
    let (addr, _dir) = setup_test_server().await;

    // 1. Empty agent_id
    let (status, resp) = send_http_post(
        addr,
        "/v1/agent/state/set",
        r#"{"agent_id":"","state":{"x":1}}"#,
    )
    .await;
    assert_eq!(status, 400);
    assert!(resp["error"].as_str().unwrap().contains("agent_id"));

    // 2. Invalid embedding (empty)
    let (status, resp) = send_http_post(
        addr,
        "/v1/agent/memory/remember",
        r#"{"agent_id":"agent1","memory_id":"m1","text":"hello","embedding":[]}"#,
    )
    .await;
    assert_eq!(status, 400);
    assert!(resp["error"].as_str().unwrap().contains("embedding"));

    // 3. Malformed JSON
    let (status, resp) = send_http_post(
        addr,
        "/v1/agent/memory/recall",
        r#"{"agent_id": "agent1", "embedding": [not_valid_json]}"#,
    )
    .await;
    assert_eq!(status, 400);
    assert!(resp["error"].as_str().unwrap().contains("Invalid JSON"));
}
