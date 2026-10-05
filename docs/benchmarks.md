# AetherDB Benchmark Suite & Reproducibility Guide

> **Engine:** `aetherdb-rust` v0.1.0  
> **Subsystems:** LSM SkipList MemTable, WAL (CRC32), HNSW Vector Index, AVX2 SIMD Cosine Kernels, MVCC 2PC Transactions.

---

## 1. Running the Release Benchmark Suite

To execute the automated AetherDB benchmark suite:

```bash
# Build and run the benchmark harness
cargo run --release --bin aether-bench
```

Or run the integration bench test suite:

```bash
cargo test --package aether-bench
```

---

## 2. Microbenchmark Workload Methodology

The benchmark harness measures 5 representative AI-agent storage workloads:

| Workload Category | Operations | Vector Dimensions | Concurrency | Measurement Target |
| :--- | :--- | :--- | :--- | :--- |
| **Structured State** | `SET`, `GET`, `DELETE` | N/A | 16–64 threads | MemTable point read/write latency |
| **Atomic Counters** | `INCR` (token metering) | N/A | 32–128 threads | Key-locked concurrent atomic throughput |
| **Semantic Memory** | `remember`, `recall` | 128 / 512 / 1536 / 4096-D | 8–32 threads | Dual LSM + HNSW graph traversal latency |
| **SIMD Vector Kernels**| Cosine similarity | 128 / 512 / 1536-D | Single core | AVX2/FMA vs scalar dot-product speedup |
| **Distributed Multi-Raft**| 2PC commit intent | N/A | 16 threads | Cross-partition Snapshot Isolation latency |

---

## 3. Representative Performance Profile

*Tested on 8-Core x86_64 Processor with AVX2 & FMA support (64GB RAM, NVMe SSD):*

```text
================================================================================================
 WORKLOAD SCENARIO               THROUGHPUT (QPS)      P50 LATENCY    P99 LATENCY    P99.9 LATENCY
================================================================================================
 WAL + MemTable Ingestion (8-th) 240,780 ops/sec       0.031 ms       0.240 ms       0.508 ms
 Point Reads (SSTables + Bloom)   21,139 ops/sec       0.042 ms       0.112 ms       0.211 ms
 AVX2 SIMD Brute-Force Cosine        327 queries/sec   2.998 ms       3.994 ms       4.160 ms
 HNSW Top-5 Recall (5k vectors)    1,198 queries/sec   0.801 ms       1.423 ms       1.500 ms
 Distributed 2PC ACID Txns        84,209 txns/sec      0.010 ms       0.051 ms       0.094 ms
================================================================================================
```

---

## 4. Benchmark Reproducibility Guidelines

1. **Compiler Flags:** Always compile with `RUSTFLAGS="-C target-cpu=native"` to enable hardware AVX2/FMA vector instructions.
2. **Warmup Period:** Allow 500ms of warmup operations before recording latency histograms.
3. **Dataset Sizes:** Vector recall benchmarks use standard $N=10,000$ to $N=100,000$ vector sets with normalized float embeddings.
