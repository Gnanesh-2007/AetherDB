use std::io::{self, Write};
use std::net::SocketAddr;
use std::time::Instant;
use clap::Parser;
use aether_network::{AetherClient, Request, Response};

#[derive(Parser, Debug)]
#[command(author, version, about = "AetherDB Interactive Cluster CLI & Benchmark Tool", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:8300")]
    server: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                   ⚡ AetherDB Cluster CLI ⚡                  ║");
    println!("║  Type SET <k> <v>, GET <k>, DEL <k>, BENCH <n>, or QUIT        ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!("Connecting to {}...", args.server);

    let mut client = match AetherClient::connect(args.server).await {
        Ok(c) => {
            println!("Connected successfully to cluster!\n");
            c
        }
        Err(e) => {
            eprintln!("Failed to connect to cluster: {}", e);
            return Ok(());
        }
    };

    let stdin = io::stdin();
    let mut line = String::new();

    loop {
        print!("aether-db> ");
        io::stdout().flush()?;
        line.clear();
        if stdin.read_line(&mut line)? == 0 {
            break;
        }

        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0].to_uppercase().as_str() {
            "PING" => match client.send(Request::Ping).await {
                Ok(Response::Pong) => println!("PONG"),
                Ok(resp) => println!("Unexpected response: {:?}", resp),
                Err(e) => eprintln!("Error: {}", e),
            },
            "GET" => {
                if parts.len() < 2 {
                    println!("Usage: GET <key>");
                    continue;
                }
                match client.get(parts[1].as_bytes()).await {
                    Ok(Some(val)) => println!("\"{}\"", String::from_utf8_lossy(&val)),
                    Ok(None) => println!("(nil)"),
                    Err(e) => eprintln!("Error: {}", e),
                }
            }
            "SET" => {
                if parts.len() < 3 {
                    println!("Usage: SET <key> <value>");
                    continue;
                }
                let val = parts[2..].join(" ");
                match client.set(parts[1].as_bytes(), val.as_bytes()).await {
                    Ok(_) => println!("OK"),
                    Err(e) => eprintln!("Error: {}", e),
                }
            }
            "DEL" | "DELETE" => {
                if parts.len() < 2 {
                    println!("Usage: DEL <key>");
                    continue;
                }
                match client.send(Request::Delete { key: parts[1].as_bytes().to_vec() }).await {
                    Ok(Response::Ok) => println!("OK"),
                    Ok(Response::Error(e)) => eprintln!("Error: {}", e),
                    _ => println!("(unknown response)"),
                }
            }
            "INFO" => match client.send(Request::ClusterInfo).await {
                Ok(Response::ClusterInfo { node_id, ranges, status }) => {
                    println!("Node ID:  {}", node_id);
                    println!("Ranges:   {}", ranges);
                    println!("Status:   {}", status);
                }
                _ => eprintln!("Failed to retrieve cluster info"),
            },
            "BENCH" | "BENCHMARK" => {
                let n: usize = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(1000);
                println!("Running high-throughput benchmark for {} operations...", n);
                let start = Instant::now();

                for i in 0..n {
                    let key = format!("bench:key:{:06}", i);
                    let val = format!("bench:val:{:06}", i);
                    if let Err(e) = client.set(key.as_bytes(), val.as_bytes()).await {
                        eprintln!("Benchmark write failed at {}: {}", i, e);
                        break;
                    }
                }

                let elapsed = start.elapsed();
                let ops_per_sec = (n as f64) / elapsed.as_secs_f64();
                println!("Completed {} writes in {:.2?}", n, elapsed);
                println!("Throughput: {:.2} ops/sec", ops_per_sec);
            }
            "QUIT" | "EXIT" => {
                println!("Goodbye!");
                break;
            }
            unknown => println!("Unknown command: '{}'. Try SET, GET, DEL, INFO, BENCH, QUIT.", unknown),
        }
    }

    Ok(())
}
