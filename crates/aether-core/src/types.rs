use serde::{Deserialize, Serialize};
use crate::hlc::HlcTimestamp;

pub type TxnId = u64;

/// Represents the state of a value stored in the database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ValueState {
    /// Active committed value.
    Some(Vec<u8>),
    /// Tombstone marker (logically deleted key).
    #[default]
    Tombstone,
    /// Uncommitted write intent held by a running 2PC transaction.
    Intent {
        txn_id: TxnId,
        primary_key: Vec<u8>,
        commit_ts: Option<HlcTimestamp>,
        value: Option<Vec<u8>>,
    },
}

/// Represents a contiguous key range partition in the Multi-Raft cluster.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RangeKey {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

impl RangeKey {
    pub fn new(start_key: impl Into<Vec<u8>>, end_key: impl Into<Vec<u8>>) -> Self {
        Self {
            start_key: start_key.into(),
            end_key: end_key.into(),
        }
    }

    /// Checks if a given user key falls inside [start_key, end_key).
    pub fn contains(&self, key: &[u8]) -> bool {
        (self.start_key.is_empty() || key >= self.start_key.as_slice())
            && (self.end_key.is_empty() || key < self.end_key.as_slice())
    }
}
