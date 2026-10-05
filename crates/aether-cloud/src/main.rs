use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::FmtSubscriber;

use aether_cloud::{AetherCloudServer, ApiKeyManager, MeteringEngine, TenantManager};

#[derive(Parser, Debug)]
#[command(author, version, about = "AetherCloud Control Plane & SaaS Platform Server", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:8400")]
    addr: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();
    info!("🚀 Initializing AetherCloud Control Plane on {}", args.addr);

    let tenants = Arc::new(TenantManager::new());
    let keys = Arc::new(ApiKeyManager::new());
    let metering = Arc::new(MeteringEngine::new());

    let server = AetherCloudServer::new(args.addr, tenants, keys, metering);
    info!(
        "⚡ AetherCloud Developer Portal online at http://{}",
        args.addr
    );
    server.run().await?;

    Ok(())
}
