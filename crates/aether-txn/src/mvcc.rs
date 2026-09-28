use std::sync::Arc;
use aether_core::error::{AetherError, Result};
use aether_core::hlc::HlcTimestamp;
use aether_core::key::MvccKey;
use aether_core::types::{TxnId, ValueState};
use aether_storage::StorageEngine;

pub struct MvccEngine {
    storage: Arc<StorageEngine>,
}

impl MvccEngine {
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self { storage }
    }

    /// Reads key as of snapshot_ts without blocking or being blocked by concurrent writers.
    pub fn get(&self, user_key: &[u8], snapshot_ts: HlcTimestamp) -> Result<Option<Vec<u8>>> {
        match self.storage.get_mvcc(user_key, snapshot_ts)? {
            Some(ValueState::Some(val)) => Ok(Some(val)),
            Some(ValueState::Tombstone) => Ok(None),
            Some(ValueState::Intent { txn_id, .. }) => {
                Err(AetherError::TxnConflict(
                    String::from_utf8_lossy(user_key).to_string(),
                    txn_id,
                ))
            }
            None => Ok(None),
        }
    }

    /// Phase 1 of 2PC: Acquires write lock by writing an Intent.
    pub fn prewrite(
        &self,
        txn_id: TxnId,
        user_key: &[u8],
        value: Option<Vec<u8>>,
        primary_key: &[u8],
        start_ts: HlcTimestamp,
    ) -> Result<()> {
        let mvcc_key = MvccKey::new(user_key, start_ts);
        let encoded_key = mvcc_key.encode();

        // Check if already locked
        if let Some(ValueState::Intent { txn_id: existing_id, .. }) = self.storage.get(&encoded_key)? {
            if existing_id != txn_id {
                return Err(AetherError::TxnConflict(
                    String::from_utf8_lossy(user_key).to_string(),
                    existing_id,
                ));
            }
        }

        let intent = ValueState::Intent {
            txn_id,
            primary_key: primary_key.to_vec(),
            commit_ts: None,
            value,
        };

        self.storage.put(encoded_key, intent)
    }

    /// Phase 2 of 2PC: Resolves Intent to committed value at commit_ts.
    pub fn commit(
        &self,
        user_key: &[u8],
        _start_ts: HlcTimestamp,
        commit_ts: HlcTimestamp,
        value: Option<Vec<u8>>,
    ) -> Result<()> {
        let committed_key = MvccKey::new(user_key, commit_ts).encode();
        let value_state = match value {
            Some(v) => ValueState::Some(v),
            None => ValueState::Tombstone,
        };

        self.storage.put(committed_key, value_state)
    }
}
