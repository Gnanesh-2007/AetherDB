use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum AetherError {
    #[error("Key not found")]
    KeyNotFound,

    #[error("Transaction conflict: key {0} locked by txn {1}")]
    TxnConflict(String, u64),

    #[error("Transaction aborted: {0}")]
    TxnAborted(String),

    #[error("Hybrid logical clock drift exceeded maximum bound of {0} ms")]
    ClockDriftExceeded(u64),

    #[error("Checksum mismatch: expected {expected:#x}, found {found:#x}")]
    ChecksumMismatch { expected: u32, found: u32 },

    #[error("Corruption: {0}")]
    Corruption(String),

    #[error("Invalid key format: {0}")]
    InvalidKeyFormat(String),

    #[error("Raft error: {0}")]
    RaftError(String),

    #[error("Not leader: current leader is {0:?}")]
    NotLeader(Option<u64>),

    #[error("Range split conflict: range {0} already split")]
    RangeSplitConflict(u64),

    #[error("I/O error: {0}")]
    IoError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),
}

pub type Result<T> = std::result::Result<T, AetherError>;
