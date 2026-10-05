use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::FmtSubscriber;

use aether_core::hlc::HybridLogicalClock;
use aether_multiraft::{MultiRaftManager, RangeRouter};
use aether_network::NetworkServer;
use aether_storage::StorageEngine;
use aether_txn::{MvccEngine, TxnCoordinator};

#[derive(Parser, Debug)]
#[command(author, version, about = "AetherDB Storage & Consensus Server Node", long_about = None)]
struct Args {
    #[arg(short, long, env = "AETHERDB_NODE_ID", default_value = "1")]
    node_id: u64,

    #[arg(short, long, env = "AETHERDB_ADDR", default_value = "0.0.0.0:8300")]
    addr: SocketAddr,

    #[arg(long, env = "AETHERDB_HTTP_ADDR")]
    http_addr: Option<SocketAddr>,

    #[arg(short, long, env = "AETHERDB_DATA_DIR", default_value = "./data_node1")]
    data_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();
    info!(
        "🚀 Initializing AetherDB Node {} (TCP: {})",
        args.node_id, args.addr
    );

    // 1. Initialize Storage Engine (LSM-Tree + WAL)
    let storage = Arc::new(StorageEngine::open(&args.data_dir)?);
    info!("📦 Storage engine initialized at {:?}", args.data_dir);

    // 2. Initialize Hybrid Logical Clock & MVCC
    let hlc = Arc::new(HybridLogicalClock::new(5000));
    let mvcc = Arc::new(MvccEngine::new(storage.clone()));
    let coordinator = Arc::new(TxnCoordinator::new(hlc, mvcc));

    // 3. Initialize Multi-Raft Manager & Range Router
    let router = Arc::new(RangeRouter::new());
    let multi_raft = Arc::new(MultiRaftManager::new(args.node_id, router));

    // 4. Start HTTP REST Gateway
    let http_addr = if let Some(addr) = args.http_addr {
        addr
    } else if let Ok(port_str) = std::env::var("PORT") {
        if let Ok(port) = port_str.parse::<u16>() {
            SocketAddr::new("0.0.0.0".parse().unwrap(), port)
        } else {
            SocketAddr::new(args.addr.ip(), args.addr.port() + 1)
        }
    } else {
        SocketAddr::new(args.addr.ip(), args.addr.port() + 1)
    };
    let http_server = aether_network::HttpServer::new(http_addr, args.node_id, storage.clone());
    tokio::spawn(async move {
        if let Err(e) = http_server.run().await {
            tracing::error!("HTTP Gateway error: {}", e);
        }
    });

    // 5. Start TCP Binary Network Server
    let server = NetworkServer::new(args.addr, args.node_id, coordinator, multi_raft);
    info!(
        "⚡ AetherDB Cluster Node is fully online (TCP: {}, HTTP: http://{})",
        args.addr, http_addr
    );
    server.run().await?;

    Ok(())
}
