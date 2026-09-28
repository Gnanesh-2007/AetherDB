use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Ping,
    Get { key: Vec<u8> },
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    ClusterInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Pong,
    Value(Option<Vec<u8>>),
    Ok,
    ClusterInfo {
        node_id: u64,
        ranges: usize,
        status: String,
    },
    Error(String),
}
