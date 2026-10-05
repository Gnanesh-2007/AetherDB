# ⚡ AetherDB Empirical Benchmark & Technical Validation Report

> **Generated on:** `UNIX_1791215187` | **OS:** `windows` (`x86_64`) | **CPUs:** `16` | **Build:** `release (optimized)`

## 1. Executive Summary

| Benchmark Domain | Key Metric | Measured Result |
| :--- | :--- | :---: |
| **End-to-End Agent Workflow (64 Concurrent)** | Throughput | **45244.8 turns/sec** (p50: `0.76 ms`, p99: `2.96 ms`) |
| **Atomic INCR (64 Threads Concurrent)** | Throughput | **212246.7 ops/sec** (0 Lost Updates) |
| **Cold Recovery Speed (1K Records)** | Recovery Duration | **121.72 ms** (100% Integrity) |
| **Multi-Agent Isolation** | Cross-Agent Leakage | **0 leaks** (Strict Isolation) |
| **100 Concurrent Agents Sustained** | Aggregate QPS | **104109.6 ops/sec** (0 Errors) |

## 2. Agent State Latency Profile

| Operation | Total Ops | Throughput | p50 Latency | p95 Latency | p99 Latency | Max Latency |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Agent State: SET** | 5000 | 470699.0 ops/sec | 0.001 ms | 0.002 ms | 0.002 ms | 0.318 ms |
| **Agent State: GET** | 5000 | 4026413.3 ops/sec | 0.000 ms | 0.000 ms | 0.000 ms | 0.000 ms |
| **Agent State: DELETE** | 5000 | 506349.6 ops/sec | 0.001 ms | 0.001 ms | 0.002 ms | 0.121 ms |

## 3. Atomic Token INCR Scaling

| Threads | Operations | Elapsed (ms) | Throughput (ops/sec) | p50 Latency | p99 Latency | Zero Lost Updates |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | 5000 | 10.9 | **457808.4** | 0.001 ms | 0.003 ms | ✅ TRUE |
| **8** | 5000 | 23.2 | **215468.0** | 0.005 ms | 0.261 ms | ✅ TRUE |
| **32** | 4992 | 23.3 | **214369.5** | 0.004 ms | 1.050 ms | ✅ TRUE |
| **64** | 4992 | 23.5 | **212246.7** | 0.005 ms | 1.887 ms | ✅ TRUE |

## 4. Semantic Memory Vector Scaling

| Dataset Size | Dimensions | Ingestion QPS | Search QPS | Search p50 | Search p95 | Search p99 |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1000 vectors** | 128-dim | 5555.6 vec/s | **11692.0 QPS** | 0.08 ms | 0.09 ms | 0.09 ms |
| **1000 vectors** | 384-dim | 2233.8 vec/s | **5089.0 QPS** | 0.19 ms | 0.23 ms | 0.31 ms |
| **1000 vectors** | 768-dim | 1276.2 vec/s | **2809.7 QPS** | 0.35 ms | 0.41 ms | 0.44 ms |
| **10000 vectors** | 128-dim | 2623.4 vec/s | **7919.0 QPS** | 0.12 ms | 0.14 ms | 0.22 ms |
| **10000 vectors** | 384-dim | 1116.6 vec/s | **2608.1 QPS** | 0.36 ms | 0.49 ms | 0.65 ms |
| **10000 vectors** | 768-dim | 612.9 vec/s | **1868.8 QPS** | 0.50 ms | 0.71 ms | 0.80 ms |
| **50000 vectors** | 128-dim | 1816.6 vec/s | **4849.5 QPS** | 0.20 ms | 0.27 ms | 0.37 ms |
| **50000 vectors** | 384-dim | 709.2 vec/s | **2523.5 QPS** | 0.35 ms | 0.64 ms | 0.74 ms |

## 5. End-to-End Agent Workflow Scaling

| Concurrent Agents | Total Turns | Elapsed (ms) | Workflow QPS | p50 Latency | p95 Latency | p99 Latency |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1 agents** | 1000 | 129.9 | **7697.5 turns/sec** | 0.12 ms | 0.16 ms | 0.24 ms |
| **8 agents** | 1000 | 28.5 | **35126.8 turns/sec** | 0.19 ms | 0.36 ms | 0.77 ms |
| **32 agents** | 992 | 20.0 | **49610.7 turns/sec** | 0.45 ms | 1.18 ms | 1.55 ms |
| **64 agents** | 960 | 21.2 | **45244.8 turns/sec** | 0.76 ms | 2.18 ms | 2.96 ms |

## 6. Persistence & Crash Recovery

- **Records Written:** `1000` state records + `1000` vector embeddings
- **Cold Reopen / Recovery Time:** `121.72 ms`
- **Recovered Records:** `1000` state records (`100%`), `1000` memories (`100%`)
- **Token Counter Integrity:** Expected `10000`, Restored `10000` (Invariant Maintained: `true`)

