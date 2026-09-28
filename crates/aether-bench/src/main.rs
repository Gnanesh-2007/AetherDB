pub mod metrics;

use std::sync::Arc;
use std::thread;
use std::time::Instant;
use clap::Parser;
use rand::Rng;
use tempfile::tempdir;

use aether_core::hlc::HybridLogicalClock;
use aether_core::types::ValueState;
use aether_simd::cosine_similarity;
use aether_storage::StorageEngine;
use aether_txn::{MvccEngine, TxnCoordinator};
use metrics::{print_report, LatencyHistogram};

#[derive(Parser, Debug)]
#[command(author, version, about = "AetherDB Empirical Benchmarking Suite", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "10000")]
    num_ops: usize,

    #[arg(short, long, default_value = "8")]
    concurrency: usize,

    #[arg(short, long, default_value = "768")]
    vector_dim: usize,
}

fn bench_kv_writes(num_ops: usize) {
    let dir = tempdir().unwrap();
    let storage = StorageEngine::open(dir.path()).unwrap();
    let mut hist = LatencyHistogram::with_capacity(num_ops);

    let start = Instant::now();
    for i in 0..num_ops {
        let key = format!("bench:user:{:08}", i).into_bytes();
        let val = ValueState::Some(format!("payload_data_chunk_{:08}", i).into_bytes());

        let op_start = Instant::now();
        storage.put(key, val).unwrap();
        hist.record(op_start.elapsed());
    }
    let elapsed = start.elapsed();

    let report = hist.report(&format!("Workload A: Single-Thread Persistent Writes ({} ops)", num_ops), elapsed);
    print_report(&report);
}

fn bench_concurrent_kv_writes(num_ops: usize, concurrency: usize) {
    let dir = tempdir().unwrap();
    let storage = Arc::new(StorageEngine::open(dir.path()).unwrap());
    let ops_per_thread = num_ops / concurrency;

    let start = Instant::now();
    let mut handles = Vec::new();

    for t in 0..concurrency {
        let store = storage.clone();
        let handle = thread::spawn(move || {
            let mut local_hist = LatencyHistogram::with_capacity(ops_per_thread);
            for i in 0..ops_per_thread {
                let key = format!("thread_{}:user_{:08}", t, i).into_bytes();
                let val = ValueState::Some(b"concurrent_payload_value".to_vec());

                let op_start = Instant::now();
                store.put(key, val).unwrap();
                local_hist.record(op_start.elapsed());
            }
            local_hist
        });
        handles.push(handle);
    }

    let mut combined_hist = LatencyHistogram::with_capacity(num_ops);
    for h in handles {
        let local_hist = h.join().unwrap();
        combined_hist.extend(local_hist);
    }
    let elapsed = start.elapsed();

    let report = combined_hist.report(
        &format!("Workload A2: Concurrent Writes ({} threads, {} ops)", concurrency, num_ops),
        elapsed,
    );
    print_report(&report);
}

fn bench_kv_reads(num_ops: usize) {
    let dir = tempdir().unwrap();
    let storage = StorageEngine::open(dir.path()).unwrap();

    // Pre-populate 5,000 keys and flush to SSTable
    for i in 0..5000 {
        let key = format!("user:{:06}", i).into_bytes();
        let val = ValueState::Some(format!("data_{}", i).into_bytes());
        storage.put(key, val).unwrap();
    }
    storage.flush_active_memtable().unwrap();

    let mut hist = LatencyHistogram::with_capacity(num_ops);
    let mut rng = rand::thread_rng();

    let start = Instant::now();
    for _ in 0..num_ops {
        let target_id = rng.gen_range(0..5000);
        let key = format!("user:{:06}", target_id).into_bytes();

        let op_start = Instant::now();
        let res = storage.get(&key).unwrap();
        hist.record(op_start.elapsed());
        assert!(res.is_some());
    }
    let elapsed = start.elapsed();

    let report = hist.report(&format!("Workload B: Point Reads from SSTables/MemTable ({} ops)", num_ops), elapsed);
    print_report(&report);
}

fn bench_simd_vector_search(num_vectors: usize, num_queries: usize, dim: usize) {
    let mut rng = rand::thread_rng();

    // Generate random normalized vectors
    let mut dataset: Vec<Vec<f32>> = Vec::with_capacity(num_vectors);
    for _ in 0..num_vectors {
        let v: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
        dataset.push(v);
    }

    let query: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
    let mut hist = LatencyHistogram::with_capacity(num_queries);

    let start = Instant::now();
    for _ in 0..num_queries {
        let op_start = Instant::now();

        // Exact Flat SIMD Scan over dataset
        let mut top_score = -1.0f32;
        let mut top_idx = 0;

        for (idx, vec) in dataset.iter().enumerate() {
            let score = cosine_similarity(&query, vec);
            if score > top_score {
                top_score = score;
                top_idx = idx;
            }
        }

        hist.record(op_start.elapsed());
        let _ = (top_idx, top_score);
    }
    let elapsed = start.elapsed();

    let report = hist.report(
        &format!(
            "Workload C: SIMD Cosine Search ({} vectors, {}-dim, {} queries)",
            num_vectors, dim, num_queries
        ),
        elapsed,
    );
    print_report(&report);
}

fn bench_acid_transactions(num_txns: usize) {
    let dir = tempdir().unwrap();
    let storage = Arc::new(StorageEngine::open(dir.path()).unwrap());
    let hlc = Arc::new(HybridLogicalClock::new(5000));
    let mvcc = Arc::new(MvccEngine::new(storage));
    let coordinator = TxnCoordinator::new(hlc, mvcc);

    let mut hist = LatencyHistogram::with_capacity(num_txns);
    let start = Instant::now();

    for i in 0..num_txns {
        let op_start = Instant::now();

        let mut txn = coordinator.begin(i as u64 + 1);
        let from_acc = format!("acc:{:04}", i % 100).into_bytes();
        let to_acc = format!("acc:{:04}", (i + 1) % 100).into_bytes();

        coordinator.set(&mut txn, from_acc, b"900".to_vec());
        coordinator.set(&mut txn, to_acc, b"1100".to_vec());
        coordinator.commit(txn).unwrap();

        hist.record(op_start.elapsed());
    }
    let elapsed = start.elapsed();

    let report = hist.report(
        &format!("Workload D: Distributed 2PC Multi-Key ACID Transactions ({} txns)", num_txns),
        elapsed,
    );
    print_report(&report);
}

fn main() {
    let args = Args::parse();

    println!("\n╔════════════════════════════════════════════════════════════════════════╗");
    println!("║              ⚡ AetherDB Empirical Benchmarking Suite ⚡              ║");
    println!("╚════════════════════════════════════════════════════════════════════════╝\n");

    bench_kv_writes(args.num_ops);
    bench_concurrent_kv_writes(args.num_ops, args.concurrency);
    bench_kv_reads(args.num_ops);
    bench_simd_vector_search(10000, 100, args.vector_dim);
    bench_acid_transactions(args.num_ops / 2);

    println!("✔ All Phase 2 Benchmarks completed successfully.\n");
}
