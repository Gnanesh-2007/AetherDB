use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

use aether_core::hlc::HybridLogicalClock;
use aether_storage::StorageEngine;
use aether_txn::{MvccEngine, TxnCoordinator};

/// Differential State-Machine Fuzzer:
/// Maintains an in-memory oracle (reference HashMap) alongside AetherDB,
/// executes thousands of randomized mutations, flushes, and simulated process crashes,
/// and proves that the recovered engine state is 100% identical to the reference model.
#[test]
fn test_crash_consistency_state_machine_differential_fuzzing() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_path_buf();

    let mut reference_state: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
    let mut rng = rand::thread_rng();

    let mut hlc = Arc::new(HybridLogicalClock::new(5000));
    let mut storage = Arc::new(StorageEngine::open(&db_path).unwrap());
    let mut mvcc = Arc::new(MvccEngine::new(storage.clone()));
    let mut coordinator = Arc::new(TxnCoordinator::new(hlc.clone(), mvcc.clone()));

    let total_operations = 1000;
    let key_pool_size = 50;

    let keys: Vec<Vec<u8>> = (0..key_pool_size)
        .map(|i| format!("fuzz:key:{:04}", i).into_bytes())
        .collect();

    for step in 0..total_operations {
        let op_type = rng.gen_range(0..100);

        if op_type < 60 {
            // 60% PUT operations
            let key = keys[rng.gen_range(0..key_pool_size)].clone();
            let val = format!("val:step_{}", step).into_bytes();

            let mut txn = coordinator.begin(step as u64 + 1);
            coordinator.set(&mut txn, key.clone(), val.clone());
            coordinator.commit(txn).unwrap();

            reference_state.insert(key, Some(val));
        } else if op_type < 85 {
            // 25% DELETE operations
            let key = keys[rng.gen_range(0..key_pool_size)].clone();

            let mut txn = coordinator.begin(step as u64 + 1);
            coordinator.delete(&mut txn, key.clone());
            coordinator.commit(txn).unwrap();

            reference_state.insert(key, None);
        } else if op_type < 95 {
            // 10% Manual MemTable Flush to Disk
            storage.flush_active_memtable().unwrap();
        } else {
            // 5% Simulated Process Crash & Engine Recovery
            drop(coordinator);
            drop(mvcc);
            drop(storage);

            // Restart engine from disk & WAL
            hlc = Arc::new(HybridLogicalClock::new(5000));
            storage = Arc::new(StorageEngine::open(&db_path).unwrap());
            mvcc = Arc::new(MvccEngine::new(storage.clone()));
            coordinator = Arc::new(TxnCoordinator::new(hlc.clone(), mvcc.clone()));

            // Verify state against reference model after restart
            let read_txn = coordinator.begin(99999999);
            for (key, expected_val) in &reference_state {
                let actual_val = coordinator.get(&read_txn, key).unwrap();
                assert_eq!(
                    &actual_val,
                    expected_val,
                    "Crash consistency invariant violated at step {} for key {:?}",
                    step,
                    String::from_utf8_lossy(key)
                );
            }
        }
    }

    // Final comprehensive verification pass across all keys
    let final_txn = coordinator.begin(100000000);
    for (key, expected_val) in &reference_state {
        let actual_val = coordinator.get(&final_txn, key).unwrap();
        assert_eq!(
            &actual_val,
            expected_val,
            "Final state differential mismatch for key {:?}",
            String::from_utf8_lossy(key)
        );
    }
}
