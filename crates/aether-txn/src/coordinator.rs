use crate::mvcc::MvccEngine;
use aether_core::error::Result;
use aether_core::hlc::{HlcTimestamp, HybridLogicalClock};
use aether_core::types::TxnId;
use std::collections::HashMap;
use std::sync::Arc;

pub struct Transaction {
    pub txn_id: TxnId,
    pub start_ts: HlcTimestamp,
    pub writes: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

pub struct TxnCoordinator {
    hlc: Arc<HybridLogicalClock>,
    mvcc: Arc<MvccEngine>,
}

impl TxnCoordinator {
    pub fn new(hlc: Arc<HybridLogicalClock>, mvcc: Arc<MvccEngine>) -> Self {
        Self { hlc, mvcc }
    }

    pub fn begin(&self, txn_id: TxnId) -> Transaction {
        let start_ts = self.hlc.now();
        Transaction {
            txn_id,
            start_ts,
            writes: HashMap::new(),
        }
    }

    pub fn get(&self, txn: &Transaction, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(buffered) = txn.writes.get(key) {
            return Ok(buffered.clone());
        }
        self.mvcc.get(key, txn.start_ts)
    }

    pub fn set(&self, txn: &mut Transaction, key: Vec<u8>, value: Vec<u8>) {
        txn.writes.insert(key, Some(value));
    }

    pub fn delete(&self, txn: &mut Transaction, key: Vec<u8>) {
        txn.writes.insert(key, None);
    }

    /// Executes Distributed Two-Phase Commit (2PC) with primary key coordination.
    pub fn commit(&self, txn: Transaction) -> Result<()> {
        if txn.writes.is_empty() {
            return Ok(());
        }

        let primary_key = txn.writes.keys().next().unwrap().clone();

        // 1. Phase 1: Prewrite all keys
        for (key, val) in &txn.writes {
            self.mvcc
                .prewrite(txn.txn_id, key, val.clone(), &primary_key, txn.start_ts)?;
        }

        // 2. Obtain Commit Timestamp
        let commit_ts = self.hlc.now();

        // 3. Phase 2: Commit Primary Key first (point of no return)
        let primary_val = txn.writes.get(&primary_key).unwrap().clone();
        self.mvcc
            .commit(&primary_key, txn.start_ts, commit_ts, primary_val)?;

        // 4. Commit Secondaries
        for (key, val) in &txn.writes {
            if key != &primary_key {
                self.mvcc
                    .commit(key, txn.start_ts, commit_ts, val.clone())?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aether_storage::StorageEngine;
    use tempfile::tempdir;

    #[test]
    fn test_distributed_2pc_transaction_lifecycle() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(StorageEngine::open(dir.path()).unwrap());
        let mvcc = Arc::new(MvccEngine::new(storage));
        let hlc = Arc::new(HybridLogicalClock::new(5000));
        let coordinator = TxnCoordinator::new(hlc, mvcc.clone());

        // Txn 1: Transfer $100 from Alice to Bob
        let mut txn1 = coordinator.begin(101);
        coordinator.set(&mut txn1, b"acc:alice".to_vec(), b"900".to_vec());
        coordinator.set(&mut txn1, b"acc:bob".to_vec(), b"1100".to_vec());
        coordinator.commit(txn1).unwrap();

        // Verify committed read
        let txn2 = coordinator.begin(102);
        assert_eq!(
            coordinator.get(&txn2, b"acc:alice").unwrap(),
            Some(b"900".to_vec())
        );
        assert_eq!(
            coordinator.get(&txn2, b"acc:bob").unwrap(),
            Some(b"1100".to_vec())
        );
    }
}
