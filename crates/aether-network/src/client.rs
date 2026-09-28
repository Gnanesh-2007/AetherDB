use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use aether_core::error::{AetherError, Result};
use crate::protocol::{Request, Response};

pub struct AetherClient {
    stream: TcpStream,
}

impl AetherClient {
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| AetherError::IoError(e.to_string()))?;
        Ok(Self { stream })
    }

    pub async fn send(&mut self, request: Request) -> Result<Response> {
        let req_bytes = bincode::serialize(&request)
            .map_err(|e| AetherError::SerializationError(e.to_string()))?;

        let req_len = req_bytes.len() as u32;
        self.stream
            .write_all(&req_len.to_be_bytes())
            .await
            .map_err(|e| AetherError::IoError(e.to_string()))?;
        self.stream
            .write_all(&req_bytes)
            .await
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        let mut len_buf = [0u8; 4];
        self.stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        let resp_len = u32::from_be_bytes(len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        self.stream
            .read_exact(&mut resp_buf)
            .await
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        let response: Response = bincode::deserialize(&resp_buf)
            .map_err(|e| AetherError::SerializationError(e.to_string()))?;

        Ok(response)
    }

    pub async fn get(&mut self, key: impl Into<Vec<u8>>) -> Result<Option<Vec<u8>>> {
        match self.send(Request::Get { key: key.into() }).await? {
            Response::Value(v) => Ok(v),
            Response::Error(e) => Err(AetherError::Corruption(e)),
            _ => Err(AetherError::Corruption("Unexpected response".to_string())),
        }
    }

    pub async fn set(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        match self.send(Request::Set { key: key.into(), value: value.into() }).await? {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(AetherError::Corruption(e)),
            _ => Err(AetherError::Corruption("Unexpected response".to_string())),
        }
    }
}
