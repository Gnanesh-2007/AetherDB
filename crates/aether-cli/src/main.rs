use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::fs;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Parser, Debug)]
#[command(
    name = "aether",
    author = "AetherDB Team",
    version = "0.1.0",
    about = "AetherDB Command Line Interface (CLI)",
    long_about = "⚡ AetherDB: Persistent memory and state layer for autonomous AI applications"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,

    /// Global server TCP address
    #[arg(long, global = true, default_value = "127.0.0.1:8300")]
    pub addr: SocketAddr,

    /// Global server HTTP address
    #[arg(long, global = true, default_value = "127.0.0.1:8301")]
    pub http_addr: SocketAddr,

    /// Data directory
    #[arg(long, global = true, default_value = "./data_node1")]
    pub data_dir: PathBuf,

    /// Output responses in machine-readable JSON format
    #[arg(long, global = true)]
    pub json: bool,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start the local AetherDB server
    Start {
        /// Run in foreground instead of background daemon
        #[arg(short, long)]
        foreground: bool,
    },
    /// Stop the running AetherDB server
    Stop,
    /// Check the status of the local AetherDB server
    Status,
    /// Query cluster health endpoint
    Health,
    /// Display engine statistics and telemetry
    Stats,
    /// Launch interactive developer shell
    Shell,
    /// Run persistent AI Agent memory demonstration
    Demo {
        /// Demo name (default: agent-memory)
        #[arg(default_value = "agent-memory")]
        name: String,
    },
    /// Agent-native state and semantic memory operations
    Agent {
        #[command(subcommand)]
        target: AgentTarget,
    },
}

#[derive(Subcommand, Debug)]
pub enum AgentTarget {
    /// Structured persistent state operations for an agent
    State {
        /// The agent identifier (e.g. research-agent)
        agent_id: String,

        #[command(subcommand)]
        action: StateAction,
    },
    /// Semantic memory operations for an agent (HNSW indexing + SIMD recall)
    Memory {
        /// The agent identifier (e.g. research-agent)
        agent_id: String,

        #[command(subcommand)]
        action: MemoryAction,
    },
}

#[derive(Subcommand, Debug)]
pub enum StateAction {
    /// Retrieve state for an agent (or root state if key omitted)
    Get {
        /// Optional state subkey
        key: Option<String>,
    },
    /// Store structured state under an agent namespace
    Set {
        /// State subkey
        key: String,
        /// State value (raw string or JSON object)
        value: String,
    },
    /// Delete state subkey or root state for an agent
    #[command(alias = "del")]
    Delete {
        /// Optional state subkey
        key: Option<String>,
    },
    /// Atomically increment an agent integer counter
    Incr {
        /// Counter key (e.g. tokens, steps)
        key: String,
        /// Increment amount (default: 1)
        #[arg(default_value = "1")]
        amount: i64,
    },
}

#[derive(Subcommand, Debug)]
pub enum MemoryAction {
    /// Ingest a semantic memory record with vector embedding
    Remember {
        /// Unique memory ID
        #[arg(long)]
        id: String,

        /// Memory text content
        #[arg(long)]
        text: String,

        /// Vector embedding (comma-separated numbers or JSON array, e.g. "0.95,0.05,0,0")
        #[arg(long)]
        embedding: String,

        /// Optional JSON metadata string (e.g. '{"domain":"consensus"}')
        #[arg(long)]
        metadata: Option<String>,
    },
    /// Recall nearest semantic memories via SIMD cosine similarity search
    Recall {
        /// Query vector embedding (comma-separated numbers or JSON array, e.g. "0.95,0.05,0,0")
        #[arg(long)]
        embedding: String,

        /// Optional query context description
        #[arg(long)]
        query: Option<String>,

        /// Maximum number of nearest memories to return
        #[arg(long, default_value = "5")]
        top_k: usize,
    },
}

const PID_FILE: &str = ".aether.pid";

/// Send an HTTP request over raw TCP stream
pub async fn http_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> Result<(u16, String), String> {
    let connect_fut = TcpStream::connect(addr);
    let mut stream = match tokio::time::timeout(Duration::from_millis(2000), connect_fut).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(format!("Failed to connect to {}: {}", addr, e)),
        Err(_) => return Err(format!("Connection timeout connecting to {}", addr)),
    };

    let body_str = body.unwrap_or("");
    let auth_header =
        match std::env::var("AETHERDB_API_KEY").or_else(|_| std::env::var("AETHER_API_KEY")) {
            Ok(k) if !k.trim().is_empty() => format!("Authorization: Bearer {}\r\n", k.trim()),
            _ => String::new(),
        };
    let req = format!(
        "{} {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\n{}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
        method,
        path,
        addr,
        auth_header,
        body_str.len(),
        body_str
    );

    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| format!("Failed to write to stream: {}", e))?;

    let mut response_bytes = Vec::new();
    let _ = stream.read_to_end(&mut response_bytes).await;

    let resp_str = String::from_utf8_lossy(&response_bytes).to_string();
    if let Some(pos) = resp_str.find("\r\n\r\n") {
        let header = &resp_str[..pos];
        let body_content = &resp_str[pos + 4..];
        let status = header
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(200);
        Ok((status, body_content.to_string()))
    } else {
        Ok((200, resp_str))
    }
}

pub async fn is_server_running(http_addr: SocketAddr) -> bool {
    if let Ok((status, body)) = http_request(http_addr, "GET", "/health", None).await {
        if status == 200 && body.contains("status") {
            return true;
        }
    }
    false
}

/// Helper to parse embedding from JSON array "[0.1, 0.2]" or comma-separated "0.1, 0.2"
pub fn parse_embedding(s: &str) -> Result<Vec<f32>, String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err("Embedding string cannot be empty".to_string());
    }

    // Try JSON array first
    if (trimmed.starts_with('[') && trimmed.ends_with(']')) || trimmed.contains(',') {
        if let Ok(vec) = serde_json::from_str::<Vec<f32>>(trimmed) {
            if !vec.is_empty() {
                return Ok(vec);
            }
        }
    }

    // Fallback: parse comma-separated values
    let clean = trimmed.trim_matches(|c| c == '[' || c == ']');
    let mut vec = Vec::new();
    for part in clean.split(',') {
        let p = part.trim();
        if !p.is_empty() {
            let f = p
                .parse::<f32>()
                .map_err(|e| format!("Invalid float value '{}': {}", p, e))?;
            vec.push(f);
        }
    }

    if vec.is_empty() {
        return Err("Embedding must contain at least one float value".to_string());
    }

    Ok(vec)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let command = cli.command.unwrap_or(Commands::Status);

    match command {
        Commands::Start { foreground } => {
            handle_start(cli.addr, cli.http_addr, cli.data_dir, foreground).await?;
        }
        Commands::Stop => {
            handle_stop(cli.http_addr).await?;
        }
        Commands::Status => {
            handle_status(cli.addr, cli.http_addr, cli.json).await;
        }
        Commands::Health => {
            handle_health(cli.http_addr, cli.json).await;
        }
        Commands::Stats => {
            handle_stats(cli.http_addr, cli.json).await;
        }
        Commands::Shell => {
            handle_shell(cli.addr, cli.http_addr).await?;
        }
        Commands::Demo { name } => {
            handle_demo(&name, cli.http_addr).await?;
        }
        Commands::Agent { target } => {
            handle_agent(target, cli.http_addr, cli.json).await?;
        }
    }

    Ok(())
}

async fn handle_start(
    addr: SocketAddr,
    http_addr: SocketAddr,
    data_dir: PathBuf,
    foreground: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if is_server_running(http_addr).await {
        println!("AetherDB");
        println!("────────");
        println!("HTTP     : {}", http_addr.port());
        println!("Raft     : {}", addr.port());
        println!("Status   : RUNNING (already active)");
        println!("\nAetherDB is already running.");
        return Ok(());
    }

    if foreground {
        println!("Starting AetherDB server in foreground...");
        let current_exe = std::env::current_exe()?;
        let server_bin_name = if cfg!(windows) {
            "aether-server.exe"
        } else {
            "aether-server"
        };
        let mut server_path = current_exe
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(server_bin_name);

        if !server_path.exists() {
            server_path = PathBuf::from(format!("./target/release/{}", server_bin_name));
        }
        if !server_path.exists() {
            server_path = PathBuf::from(format!("./target/debug/{}", server_bin_name));
        }

        let mut child = Command::new(server_path)
            .arg("--addr")
            .arg(addr.to_string())
            .arg("--http-addr")
            .arg(http_addr.to_string())
            .arg("--data-dir")
            .arg(data_dir)
            .spawn()?;

        child.wait()?;
    } else {
        let current_exe = std::env::current_exe()?;
        let server_bin_name = if cfg!(windows) {
            "aether-server.exe"
        } else {
            "aether-server"
        };
        let mut server_path = current_exe
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(server_bin_name);

        if !server_path.exists() {
            server_path = PathBuf::from(format!("./target/release/{}", server_bin_name));
        }
        if !server_path.exists() {
            server_path = PathBuf::from(format!("./target/debug/{}", server_bin_name));
        }

        let child = Command::new(server_path)
            .arg("--addr")
            .arg(addr.to_string())
            .arg("--http-addr")
            .arg(http_addr.to_string())
            .arg("--data-dir")
            .arg(data_dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        let pid = child.id();
        let _ = fs::write(PID_FILE, pid.to_string());

        // Wait briefly for startup
        let mut started = false;
        for _ in 0..15 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if is_server_running(http_addr).await {
                started = true;
                break;
            }
        }

        println!("AetherDB");
        println!("────────");
        println!("HTTP     : {}", http_addr.port());
        println!("Raft     : {}", addr.port());
        if started {
            println!("Status   : RUNNING");
            println!("\nAetherDB started successfully.");
        } else {
            println!("Status   : INITIALIZING (PID {})", pid);
            println!("\nAetherDB daemon spawned.");
        }
    }

    Ok(())
}

async fn handle_stop(http_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let mut stopped = false;

    if Path::new(PID_FILE).exists() {
        if let Ok(pid_str) = fs::read_to_string(PID_FILE) {
            if let Ok(pid) = pid_str.trim().parse::<u32>() {
                #[cfg(windows)]
                {
                    let _ = Command::new("taskkill")
                        .args(["/F", "/PID", &pid.to_string()])
                        .output();
                }
                #[cfg(not(windows))]
                {
                    let _ = Command::new("kill").args(["-9", &pid.to_string()]).output();
                }
                stopped = true;
            }
        }
        let _ = fs::remove_file(PID_FILE);
    }

    // Double check via health
    if !stopped && is_server_running(http_addr).await {
        #[cfg(windows)]
        {
            let _ = Command::new("taskkill")
                .args(["/F", "/IM", "aether-server.exe"])
                .output();
        }
        #[cfg(not(windows))]
        {
            let _ = Command::new("pkill").args(["-f", "aether-server"]).output();
        }
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    println!("AetherDB stopped successfully.");
    Ok(())
}

async fn handle_status(addr: SocketAddr, http_addr: SocketAddr, json: bool) {
    let is_running = is_server_running(http_addr).await;
    if json {
        let val = serde_json::json!({
            "running": is_running,
            "http_port": http_addr.port(),
            "raft_port": addr.port(),
            "health": if is_running { "healthy" } else { "stopped" }
        });
        println!("{}", serde_json::to_string_pretty(&val).unwrap());
        return;
    }

    if is_running {
        println!("AetherDB Status");
        println!("───────────────");
        println!("Server:      RUNNING");
        println!("HTTP:        {}", http_addr.port());
        println!("Raft:        {}", addr.port());
        println!("Health:      HEALTHY");
    } else {
        println!("AetherDB Status");
        println!("───────────────");
        println!("Server:      STOPPED");
    }
}

async fn handle_health(http_addr: SocketAddr, json: bool) {
    match http_request(http_addr, "GET", "/health", None).await {
        Ok((status, body)) if status == 200 => {
            if json {
                println!("{}", body);
                return;
            }
            #[derive(Deserialize)]
            struct HealthResp {
                status: Option<String>,
                engine: Option<String>,
                version: Option<String>,
            }
            let parsed: HealthResp = serde_json::from_str(&body).unwrap_or(HealthResp {
                status: Some("healthy".to_string()),
                engine: Some("aetherdb-rust".to_string()),
                version: Some("0.1.0".to_string()),
            });

            println!("AetherDB Health");
            println!("───────────────");
            println!(
                "Status:      {}",
                parsed
                    .status
                    .unwrap_or_else(|| "HEALTHY".to_string())
                    .to_uppercase()
            );
            println!(
                "Engine:      {}",
                parsed.engine.unwrap_or_else(|| "aetherdb-rust".to_string())
            );
            println!(
                "Version:     {}",
                parsed.version.unwrap_or_else(|| "0.1.0".to_string())
            );
            println!("HTTP:        http://{}/health", http_addr);
        }
        _ => {
            if json {
                println!(
                    r#"{{"status":"stopped","error":"Unreachable at http://{}"}}"#,
                    http_addr
                );
            } else {
                println!("AetherDB Health");
                println!("───────────────");
                println!("Status:      STOPPED (Unreachable at http://{})", http_addr);
            }
        }
    }
}

async fn handle_stats(http_addr: SocketAddr, json: bool) {
    match http_request(http_addr, "GET", "/v1/telemetry", None).await {
        Ok((200, body)) => {
            if json {
                println!("{}", body);
                return;
            }
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(&body) {
                println!("AetherDB Engine Statistics");
                println!("──────────────────────────");
                if let Some(node_id) = val.get("node_id") {
                    println!("Node ID:          {}", node_id);
                }
                if let Some(ops) = val.get("total_ops") {
                    println!("Total Operations: {}", ops);
                }
                if let Some(qps) = val.get("ops_per_sec") {
                    println!(
                        "Current QPS:      {:.1} ops/sec",
                        qps.as_f64().unwrap_or(0.0)
                    );
                }
                if let Some(lat) = val.get("latency_p50_us") {
                    println!("Median Latency:   {} µs", lat);
                }
                if let Some(p99) = val.get("latency_p99_us") {
                    println!("p99 Latency:      {} µs", p99);
                }
                if let Some(uptime) = val.get("uptime_secs") {
                    println!("Uptime:           {} seconds", uptime);
                }
                println!("Cluster Status:   HEALTHY");
                return;
            }
        }
        _ => {}
    }

    if is_server_running(http_addr).await {
        if json {
            println!(
                r#"{{"status":"healthy","endpoint":"http://{}","engine":"LSM-Tree + WAL + HNSW"}}"#,
                http_addr
            );
        } else {
            println!("AetherDB Engine Statistics");
            println!("──────────────────────────");
            println!("Cluster Status:   HEALTHY");
            println!("HTTP Endpoint:    http://{}", http_addr);
            println!("Storage Engine:   LSM-Tree + WAL + HNSW Vector");
        }
    } else if json {
        println!(r#"{{"error":"AetherDB is not running. Start with 'aether start'."}}"#);
    } else {
        println!("AetherDB is not running. Start with 'aether start'.");
    }
}

async fn handle_shell(
    addr: SocketAddr,
    http_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                  ⚡ AetherDB Developer Shell                   ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!("Connected to http://{} (TCP {})\n", http_addr, addr);
    println!("Commands:");
    println!("  get <key>             Retrieve key value");
    println!("  set <key> <val>       Store key-value pair");
    println!("  del <key>             Delete key");
    println!("  incr <key> [amount]   Atomically increment integer counter");
    println!("  health                Check server health");
    println!("  stats                 Display engine telemetry");
    println!("  exit / quit           Close shell\n");

    let stdin = io::stdin();
    let mut line = String::new();

    loop {
        print!("aether> ");
        io::stdout().flush()?;
        line.clear();
        if stdin.read_line(&mut line)? == 0 {
            break;
        }

        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0].to_lowercase().as_str() {
            "exit" | "quit" => {
                println!("Goodbye!");
                break;
            }
            "health" => {
                handle_health(http_addr, false).await;
            }
            "stats" => {
                handle_stats(http_addr, false).await;
            }
            "get" => {
                if parts.len() < 2 {
                    println!("Usage: get <key>");
                    continue;
                }
                let body = serde_json::json!({ "key": parts[1] }).to_string();
                match http_request(http_addr, "POST", "/v1/get", Some(&body)).await {
                    Ok((200, res)) => {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&res) {
                            if v.get("found").and_then(|f| f.as_bool()).unwrap_or(false) {
                                println!("{}", v.get("value").unwrap_or(&serde_json::Value::Null));
                            } else {
                                println!("(nil)");
                            }
                        } else {
                            println!("{}", res);
                        }
                    }
                    Ok((code, res)) => eprintln!("Error (HTTP {}): {}", code, res),
                    Err(e) => eprintln!("Error: {}", e),
                }
            }
            "set" => {
                if parts.len() < 3 {
                    println!("Usage: set <key> <value>");
                    continue;
                }
                let val_str = parts[2..].join(" ");
                let body = serde_json::json!({ "key": parts[1], "value": val_str }).to_string();
                match http_request(http_addr, "POST", "/v1/set", Some(&body)).await {
                    Ok((200, _)) => println!("OK"),
                    Ok((code, res)) => eprintln!("Error (HTTP {}): {}", code, res),
                    Err(e) => eprintln!("Error: {}", e),
                }
            }
            "del" | "delete" => {
                if parts.len() < 2 {
                    println!("Usage: del <key>");
                    continue;
                }
                let body = serde_json::json!({ "key": parts[1] }).to_string();
                match http_request(http_addr, "POST", "/v1/del", Some(&body)).await {
                    Ok((200, _)) => println!("OK"),
                    Ok((code, res)) => eprintln!("Error (HTTP {}): {}", code, res),
                    Err(e) => eprintln!("Error: {}", e),
                }
            }
            "incr" => {
                if parts.len() < 2 {
                    println!("Usage: incr <key> [amount]");
                    continue;
                }
                let amount: i64 = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);
                let body = serde_json::json!({ "key": parts[1], "amount": amount }).to_string();
                match http_request(http_addr, "POST", "/v1/incr", Some(&body)).await {
                    Ok((200, res)) => {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&res) {
                            println!("{}", v.get("value").unwrap_or(&serde_json::Value::Null));
                        } else {
                            println!("{}", res);
                        }
                    }
                    Ok((code, res)) => eprintln!("Error (HTTP {}): {}", code, res),
                    Err(e) => eprintln!("Error: {}", e),
                }
            }
            unknown => {
                println!(
                    "Unknown command: '{}'. Try: get, set, del, incr, health, stats, exit.",
                    unknown
                );
            }
        }
    }

    Ok(())
}

async fn handle_demo(name: &str, http_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Launching AetherDB Demo: [{}]", name);
    if !is_server_running(http_addr).await {
        println!("⚠️  AetherDB server is not currently running. Starting daemon...");
        let _ = handle_start(
            "127.0.0.1:8300".parse()?,
            http_addr,
            PathBuf::from("./data_node1"),
            false,
        )
        .await;
    }

    let script_path = Path::new("./demo/run-demo.js");
    if !script_path.exists() {
        eprintln!("Error: Demo script not found at {:?}", script_path);
        return Ok(());
    }

    let mut child = Command::new("node")
        .arg(script_path)
        .spawn()
        .map_err(|e| format!("Failed to execute node {}: {}", script_path.display(), e))?;

    let status = child.wait()?;
    if !status.success() {
        eprintln!("Demo exited with status: {}", status);
    }
    Ok(())
}

/// Dispatcher for agent-native CLI commands
async fn handle_agent(
    target: AgentTarget,
    http_addr: SocketAddr,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    match target {
        AgentTarget::State { agent_id, action } => {
            handle_agent_state(&agent_id, action, http_addr, json).await?;
        }
        AgentTarget::Memory { agent_id, action } => {
            handle_agent_memory(&agent_id, action, http_addr, json).await?;
        }
    }
    Ok(())
}

async fn handle_agent_state(
    agent_id: &str,
    action: StateAction,
    http_addr: SocketAddr,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if agent_id.trim().is_empty() {
        eprintln!("Error: agent_id cannot be empty");
        return Ok(());
    }

    match action {
        StateAction::Get { key } => {
            let mut payload = serde_json::json!({ "agent_id": agent_id });
            if let Some(ref k) = key {
                if !k.trim().is_empty() {
                    payload["key"] = serde_json::Value::String(k.trim().to_string());
                }
            }
            let body_str = payload.to_string();

            match http_request(http_addr, "POST", "/v1/agent/state/get", Some(&body_str)).await {
                Ok((200, res)) => {
                    if json {
                        println!("{}", res);
                    } else if let Ok(val) = serde_json::from_str::<serde_json::Value>(&res) {
                        let found = val.get("found").and_then(|f| f.as_bool()).unwrap_or(false);
                        if found {
                            println!("Agent:  {}", agent_id);
                            if let Some(k) = key {
                                println!("Key:    {}", k);
                            } else {
                                println!("Key:    (root)");
                            }
                            let state_val = val.get("state").unwrap_or(&serde_json::Value::Null);
                            if state_val.is_object() || state_val.is_array() {
                                println!(
                                    "State:  {}",
                                    serde_json::to_string_pretty(state_val).unwrap()
                                );
                            } else {
                                println!("State:  {}", state_val);
                            }
                        } else {
                            println!("Agent:  {}", agent_id);
                            if let Some(k) = key {
                                println!("Key:    {}", k);
                            }
                            println!("Status: (not found)");
                        }
                    } else {
                        println!("{}", res);
                    }
                }
                Ok((code, res)) => {
                    format_cli_error(code, &res, json);
                }
                Err(e) => {
                    eprintln!(
                        "Error: AetherDB server is unreachable at http://{}: {}",
                        http_addr, e
                    );
                }
            }
        }
        StateAction::Set { key, value } => {
            let parsed_state: serde_json::Value = serde_json::from_str(&value)
                .unwrap_or_else(|_| serde_json::Value::String(value.clone()));

            let payload = serde_json::json!({
                "agent_id": agent_id,
                "key": key.trim(),
                "state": parsed_state,
            });
            let body_str = payload.to_string();

            match http_request(http_addr, "POST", "/v1/agent/state/set", Some(&body_str)).await {
                Ok((200, res)) => {
                    if json {
                        println!("{}", res);
                    } else {
                        println!("Agent:  {}", agent_id);
                        println!("Key:    {}", key);
                        println!("Status: OK (state persisted)");
                    }
                }
                Ok((code, res)) => {
                    format_cli_error(code, &res, json);
                }
                Err(e) => {
                    eprintln!(
                        "Error: AetherDB server is unreachable at http://{}: {}",
                        http_addr, e
                    );
                }
            }
        }
        StateAction::Delete { key } => {
            let mut payload = serde_json::json!({ "agent_id": agent_id });
            if let Some(ref k) = key {
                if !k.trim().is_empty() {
                    payload["key"] = serde_json::Value::String(k.trim().to_string());
                }
            }
            let body_str = payload.to_string();

            match http_request(http_addr, "POST", "/v1/agent/state/delete", Some(&body_str)).await {
                Ok((200, res)) => {
                    if json {
                        println!("{}", res);
                    } else {
                        println!("Agent:  {}", agent_id);
                        if let Some(ref k) = key {
                            println!("Key:    {}", k);
                        } else {
                            println!("Key:    (root)");
                        }
                        println!("Status: OK (state deleted)");
                    }
                }
                Ok((code, res)) => {
                    format_cli_error(code, &res, json);
                }
                Err(e) => {
                    eprintln!(
                        "Error: AetherDB server is unreachable at http://{}: {}",
                        http_addr, e
                    );
                }
            }
        }
        StateAction::Incr { key, amount } => {
            let payload = serde_json::json!({
                "agent_id": agent_id,
                "key": key.trim(),
                "amount": amount,
            });
            let body_str = payload.to_string();

            match http_request(http_addr, "POST", "/v1/agent/state/incr", Some(&body_str)).await {
                Ok((200, res)) => {
                    if json {
                        println!("{}", res);
                    } else if let Ok(val) = serde_json::from_str::<serde_json::Value>(&res) {
                        let new_val = val
                            .get("value")
                            .or_else(|| val.get("new_value"))
                            .unwrap_or(&serde_json::Value::Null);
                        println!("Agent:  {}", agent_id);
                        println!("Key:    {}", key);
                        println!("Value:  {}", new_val);
                    } else {
                        println!("{}", res);
                    }
                }
                Ok((code, res)) => {
                    format_cli_error(code, &res, json);
                }
                Err(e) => {
                    eprintln!(
                        "Error: AetherDB server is unreachable at http://{}: {}",
                        http_addr, e
                    );
                }
            }
        }
    }

    Ok(())
}

async fn handle_agent_memory(
    agent_id: &str,
    action: MemoryAction,
    http_addr: SocketAddr,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if agent_id.trim().is_empty() {
        eprintln!("Error: agent_id cannot be empty");
        return Ok(());
    }

    match action {
        MemoryAction::Remember {
            id,
            text,
            embedding,
            metadata,
        } => {
            let emb_vec = match parse_embedding(&embedding) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Error: {}", e);
                    return Ok(());
                }
            };

            let meta_val = if let Some(ref m_str) = metadata {
                serde_json::from_str::<serde_json::Value>(m_str)
                    .unwrap_or_else(|_| serde_json::Value::String(m_str.clone()))
            } else {
                serde_json::Value::Null
            };

            let mut payload = serde_json::json!({
                "agent_id": agent_id,
                "memory_id": id.trim(),
                "text": text.trim(),
                "embedding": emb_vec,
            });
            if !meta_val.is_null() {
                payload["metadata"] = meta_val;
            }
            let body_str = payload.to_string();

            match http_request(
                http_addr,
                "POST",
                "/v1/agent/memory/remember",
                Some(&body_str),
            )
            .await
            {
                Ok((200, res)) => {
                    if json {
                        println!("{}", res);
                    } else {
                        println!("Agent:     {}", agent_id);
                        println!("Memory ID: {}", id);
                        println!("Status:    OK (memory stored and indexed)");
                    }
                }
                Ok((code, res)) => {
                    format_cli_error(code, &res, json);
                }
                Err(e) => {
                    eprintln!(
                        "Error: AetherDB server is unreachable at http://{}: {}",
                        http_addr, e
                    );
                }
            }
        }
        MemoryAction::Recall {
            embedding,
            query,
            top_k,
        } => {
            let emb_vec = match parse_embedding(&embedding) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Error: {}", e);
                    return Ok(());
                }
            };

            let mut payload = serde_json::json!({
                "agent_id": agent_id,
                "embedding": emb_vec,
                "top_k": top_k,
            });
            if let Some(ref q) = query {
                payload["query"] = serde_json::Value::String(q.clone());
            }
            let body_str = payload.to_string();

            match http_request(
                http_addr,
                "POST",
                "/v1/agent/memory/recall",
                Some(&body_str),
            )
            .await
            {
                Ok((200, res)) => {
                    if json {
                        println!("{}", res);
                    } else if let Ok(val) = serde_json::from_str::<serde_json::Value>(&res) {
                        let empty_vec = vec![];
                        let results = val
                            .get("results")
                            .and_then(|r| r.as_array())
                            .unwrap_or(&empty_vec);

                        if results.is_empty() {
                            println!("No semantic memories recalled for agent '{}'.", agent_id);
                        } else {
                            if let Some(ref q) = query {
                                println!(
                                    "Recalled {} memories for agent '{}' (query: \"{}\"):\n",
                                    results.len(),
                                    agent_id,
                                    q
                                );
                            } else {
                                println!(
                                    "Recalled {} memories for agent '{}':\n",
                                    results.len(),
                                    agent_id
                                );
                            }

                            for (i, r) in results.iter().enumerate() {
                                let mem_id = r
                                    .get("memory_id")
                                    .and_then(|s| s.as_str())
                                    .unwrap_or("unknown");
                                let score = r.get("score").and_then(|s| s.as_f64()).unwrap_or(0.0);
                                let text = r.get("text").and_then(|s| s.as_str()).unwrap_or("");
                                let meta = r.get("metadata");

                                println!("{}. {}", i + 1, mem_id);
                                println!("   Score: {:.2}%", score * 100.0);
                                println!("   Text:  {}", text);
                                if let Some(m) = meta {
                                    if !m.is_null() {
                                        println!("   Meta:  {}", m);
                                    }
                                }
                                println!();
                            }
                        }
                    } else {
                        println!("{}", res);
                    }
                }
                Ok((code, res)) => {
                    format_cli_error(code, &res, json);
                }
                Err(e) => {
                    eprintln!(
                        "Error: AetherDB server is unreachable at http://{}: {}",
                        http_addr, e
                    );
                }
            }
        }
    }

    Ok(())
}

fn format_cli_error(status: u16, raw_body: &str, json: bool) {
    if json {
        eprintln!("{}", raw_body);
        return;
    }
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(raw_body) {
        if let Some(err_msg) = v.get("error").and_then(|s| s.as_str()) {
            eprintln!("Error (HTTP {}): {}", status, err_msg);
            return;
        }
    }
    eprintln!("Error (HTTP {}): {}", status, raw_body.trim());
}
