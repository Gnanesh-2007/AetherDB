use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

use aether_core::error::{AetherError, Result};
use aether_multiraft::MultiRaftManager;
use aether_txn::TxnCoordinator;
use crate::protocol::{Request, Response};

pub struct NetworkServer {
    addr: SocketAddr,
    node_id: u64,
    coordinator: Arc<TxnCoordinator>,
    _multi_raft: Arc<MultiRaftManager>,
}

impl NetworkServer {
    pub fn new(
        addr: SocketAddr,
        node_id: u64,
        coordinator: Arc<TxnCoordinator>,
        multi_raft: Arc<MultiRaftManager>,
    ) -> Self {
        Self {
            addr,
            node_id,
            coordinator,
            _multi_raft: multi_raft,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(self.addr)
            .await
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        info!("AetherDB node {} listening on {}", self.node_id, self.addr);

        loop {
            let (socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!("Connection accept error: {}", e);
                    continue;
                }
            };

            let coordinator = self.coordinator.clone();
            let node_id = self.node_id;

            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(socket, node_id, coordinator).await {
                    error!("Client connection handler error: {}", e);
                }
            });
        }
    }

    async fn handle_connection(
        mut socket: TcpStream,
        node_id: u64,
        coordinator: Arc<TxnCoordinator>,
    ) -> Result<()> {
        loop {
            let mut len_buf = [0u8; 4];
            match socket.read_exact(&mut len_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(AetherError::IoError(e.to_string())),
            }

            let msg_len = u32::from_be_bytes(len_buf) as usize;
            let mut msg_buf = vec![0u8; msg_len];
            socket
                .read_exact(&mut msg_buf)
                .await
                .map_err(|e| AetherError::IoError(e.to_string()))?;

            let request: Request = bincode::deserialize(&msg_buf)
                .map_err(|e| AetherError::SerializationError(e.to_string()))?;

            let response = match request {
                Request::Ping => Response::Pong,
                Request::Get { key } => {
                    let txn = coordinator.begin(1);
                    match coordinator.get(&txn, &key) {
                        Ok(val) => Response::Value(val),
                        Err(e) => Response::Error(e.to_string()),
                    }
                }
                Request::Set { key, value } => {
                    let mut txn = coordinator.begin(1);
                    coordinator.set(&mut txn, key, value);
                    match coordinator.commit(txn) {
                        Ok(_) => Response::Ok,
                        Err(e) => Response::Error(e.to_string()),
                    }
                }
                Request::Delete { key } => {
                    let mut txn = coordinator.begin(1);
                    coordinator.delete(&mut txn, key);
                    match coordinator.commit(txn) {
                        Ok(_) => Response::Ok,
                        Err(e) => Response::Error(e.to_string()),
                    }
                }
                Request::ClusterInfo => Response::ClusterInfo {
                    node_id,
                    ranges: 1,
                    status: "HEALTHY".to_string(),
                },
            };

            let resp_bytes = bincode::serialize(&response)
                .map_err(|e| AetherError::SerializationError(e.to_string()))?;

            let resp_len = resp_bytes.len() as u32;
            socket
                .write_all(&resp_len.to_be_bytes())
                .await
                .map_err(|e| AetherError::IoError(e.to_string()))?;
            socket
                .write_all(&resp_bytes)
                .await
                .map_err(|e| AetherError::IoError(e.to_string()))?;
        }

        Ok(())
    }
}
