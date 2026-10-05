use aether_core::types::ValueState;
use aether_storage::StorageEngine;
use tempfile::tempdir;

#[test]
fn test_storage_engine_crash_consistency_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path();

    // 1. Initial writes
    {
        let engine = StorageEngine::open(db_path).unwrap();
        for i in 0..100 {
            let k = format!("user:{:04}", i).into_bytes();
            let v = format!("payload:{:04}", i).into_bytes();
            engine.put(k, ValueState::Some(v)).unwrap();
        }
    } // Simulated crash / restart

    // 2. Recover from WAL and verify all 100 keys exist
    {
        let engine = StorageEngine::open(db_path).unwrap();
        for i in 0..100 {
            let k = format!("user:{:04}", i).into_bytes();
            let expected_v = format!("payload:{:04}", i).into_bytes();
            let val = engine.get(&k).unwrap();
            assert_eq!(val, Some(ValueState::Some(expected_v)));
        }
    }
}

#[test]
fn test_sstable_immutability_and_bloom_filtering() {
    let dir = tempdir().unwrap();
    let engine = StorageEngine::open(dir.path()).unwrap();

    // Insert 500 keys to trigger flush
    for i in 0..500 {
        let k = format!("metric:sensor:{:05}", i).into_bytes();
        let v = format!("val:{}", i * 10).into_bytes();
        engine.put(k, ValueState::Some(v)).unwrap();
    }

    engine.flush_active_memtable().unwrap();

    // Query existing and non-existing keys
    assert!(engine.get(b"metric:sensor:00250").unwrap().is_some());
    assert!(engine.get(b"metric:sensor:99999").unwrap().is_none());
}
