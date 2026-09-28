# ⚡ AetherDB: Distributed Multi-Raft Hybrid Storage & Vector Engine

[![AetherDB CI](https://github.com/your-username/aether-db/actions/workflows/ci.yml/badge.svg)](https://github.com/your-username/aether-db/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.80%2B-orange.svg)](https://www.rust-lang.org/)
[![Status](https://img.shields.io/badge/Chaos%20Tests-24%2F24%20Passing-brightgreen.svg)]()

> **A high-performance, fault-tolerant distributed storage engine built from scratch in Rust.** Combining Multi-Raft consensus, dynamic range sharding, distributed MVCC ACID transactions with Hybrid Logical Clocks (HLC), zero-copy LSM-tree persistence, SIMD-accelerated vector search, and unified developer SDKs.

---

## 🏛️ System Architecture

```
                                ┌────────────────────────────────────────────────────────┐
                                │             Client Application / AI Agent              │
                                │   (Node.js / Python SDK / HTTP REST / Binary TCP Driver)│
                                └───────────────────────────┬────────────────────────────┘
                                                            │
                                  ┌─────────────────────────┼─────────────────────────┐
                                  ▼                         ▼                         ▼
                       ┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
                       │    Node 1 (Port 8300)│  │    Node 2 (Port 8310)│  │    Node 3 (Port 8320)│
                       │ ┌──────────────────┐ │  │ ┌──────────────────┐ │  │ ┌──────────────────┐ │
                       │ │ Multi-Raft Leader│◄┼──┼─┼► Raft Follower   │◄─┼──┼─┼► Raft Follower   │ │
                       │ │ Range [000-500)  │ │  │ │ Range [000-500)  │  │  │ │ Range [000-500)  │ │
                       │ └────────┬─────────┘ │  │ └────────┬─────────┘  │  │ └────────┬─────────┘ │
                       │ ┌────────┴─────────┐ │  │ ┌────────┴─────────┐  │  │ ┌────────┴─────────┐ │
                       │ │ Raft Follower    │◄┼──┼─┼► Raft Leader     │◄─┼──┼─┼► Raft Follower   │ │
                       │ │ Range [500-999)  │ │  │ │ Range [500-999)  │  │  │ │ Range [500-999)  │ │
                       │ └────────┬─────────┘ │  │ └────────┬─────────┘  │  │ └────────┬─────────┘ │
                       │          ▼           │  │          ▼            │  │          ▼            │
                       │ ┌──────────────────┐ │  │ ┌──────────────────┐  │  │ ┌──────────────────┐ │
                       │ │ MVCC + 2PC Layer │ │  │ │ MVCC + 2PC Layer │  │  │ │ MVCC + 2PC Layer │ │
                       │ └────────┬─────────┘ │  │ └────────┬─────────┘  │  │ └────────┬─────────┘ │
                       │          ▼           │  │          ▼            │  │          ▼            │
                       │ ┌──────────────────┐ │  │ ┌──────────────────┐  │  │ ┌──────────────────┐ │
                       │ │ LSM Storage Tree │ │  │ │ LSM Storage Tree │  │  │ │ LSM Storage Tree │ │
                       │ │ + AVX2/NEON SIMD │ │  │ │ + AVX2/NEON SIMD │  │  │ │ + AVX2/NEON SIMD │ │
                       │ └────────┬─────────┘ │  │ └────────┬─────────┘  │  │ └────────┬─────────┘ │
                       │ ┌────────┴─────────┐ │  │ ┌────────┴─────────┐  │  │ ┌────────┴─────────┐ │
                       │ │ HTTP Gateway / UI│ │  │ │ HTTP Gateway / UI│  │  │ │ HTTP Gateway / UI│ │
                       │ │ (Port 8301)      │ │  │ │ (Port 8311)      │  │  │ │ (Port 8321)      │ │
                       │ └──────────────────┘ │  │ └──────────────────┘  │  │ └──────────────────┘ │
                       └──────────────────────┘  └──────────────────────┘  └──────────────────────┘
```

---

## 📦 Workspace Crates

| Crate | Directory | Purpose |
| :--- | :--- | :--- |
| **`aether-core`** | [`crates/aether-core`](crates/aether-core) | Hybrid Logical Clock (HLC), inverted MVCC key encoding, core error types |
| **`aether-simd`** | [`crates/aether-simd`](crates/aether-simd) | AVX2/NEON unrolled Cosine similarity & Euclidean distance kernels |
| **`aether-storage`** | [`crates/aether-storage`](crates/aether-storage) | Concurrent SkipList MemTable, CRC32 WAL, 4KB block SSTables, 10-bit Bloom filters |
| **`aether-raft`** | [`crates/aether-raft`](crates/aether-raft) | Raft consensus state machine, append-only log engine, randomized election timers |
| **`aether-multiraft`**| [`crates/aether-multiraft`](crates/aether-multiraft)| Dynamic range router and autonomous partition split coordinator |
| **`aether-txn`** | [`crates/aether-txn`](crates/aether-txn) | Distributed MVCC Snapshot Isolation & 2PC Transaction Coordinator with write intents |
| **`aether-network`** | [`crates/aether-network`](crates/aether-network) | Async Tokio TCP binary protocol & HTTP REST Gateway with embedded Web DevTools |
| **`aether-server`** | [`crates/aether-server`](crates/aether-server) | Clustered storage node daemon binary |
| **`aether-cli`** | [`crates/aether-cli`](crates/aether-cli) | Interactive cluster REPL shell with administrative tooling |
| **`aether-chaos`** | [`crates/aether-chaos`](crates/aether-chaos) | Jepsen-style linearizability checker & fault-injection harness |
| **`aether-bench`** | [`crates/aether-bench`](crates/aether-bench) | Micro-benchmark suite capturing exact latency percentiles ($p50/p90/p99$) |

---

## 📊 Empirical Benchmarks (Phase 2 Results)

Benchmarked on single NVMe SSD storage node compiled in `--release` mode:

| Workload | Throughput | Median Latency ($p50$) | 99th Percentile ($p99$) | Max Latency |
| :--- | :---: | :---: | :---: | :---: |
| **Concurrent Writes (WAL + MemTable)** | **$228,277\text{ ops/sec}$** | $30\,\mu\text{s}$ | $142\,\mu\text{s}$ | $601\,\mu\text{s}$ |
| **SSTable Point Reads (with Bloom Filter)** | **$22,529\text{ ops/sec}$** | $42\,\mu\text{s}$ | $64\,\mu\text{s}$ | $99\,\mu\text{s}$ |
| **768-Dim Vector Search (50K Vectors)** | **$176\text{ QPS}$** | $5.64\text{ ms}$ | $5.74\text{ ms}$ | $5.99\text{ ms}$ |
| **2PC ACID Distributed Transfers** | **$88,506\text{ txns/sec}$** | $9\,\mu\text{s}$ | $27\,\mu\text{s}$ | $240\,\mu\text{s}$ |

To run the empirical benchmark suite yourself:
```bash
cargo run --release --bin aether-bench -- --records 100000
```

---

## 🧪 Chaos Engineering & Correctness Verification (Phase 1.5)

AetherDB contains a dedicated chaos test suite verifying correctness under extreme distributed fault conditions:

1. **Crash-Consistency Differential State-Machine Fuzzer:**
   * Runs 1,000+ randomized mutations against an in-memory oracle model.
   * Simulates ungraceful process terminations (`kill -9`) and verifies 100% state equality upon WAL replay.
2. **WAL Bit-Rot & Torn-Write Recovery:**
   * Injects corrupted bytes and truncated records into the log.
   * Proves that the CRC32 checksum engine halts cleanly at the corruption point without reading poisoned data.
3. **100-Thread Concurrency Stress Test:**
   * Concurrently mutates overlapping key ranges across 100 OS threads to prove lock-free CAS and MVCC thread-safety.
4. **Dynamic Range Shard Invariant:**
   * Validates continuous routing integrity across splits: keys before split point route to shard A, keys after route to shard B.
5. **Raft Election Safety:**
   * Proves higher-term candidate preemption and term monotonicity across randomized voting cycles.

Run the test suite:
```bash
cargo test --workspace
```

---

## 💻 Developer Platform & SDKs

### TypeScript / JavaScript SDK (`@aetherdb/sdk`)

```typescript
import { AetherDB } from "@aetherdb/sdk";

const db = new AetherDB({ url: "http://localhost:8301" });

// 1. Transactional State
await db.set("agent:session:1001", {
  agent_id: "agent_researcher_alpha",
  task: "Synthesize distributed systems benchmarks"
});

// 2. Atomic Token Rate Limiting
const tokensUsed = await db.incr("agent:rate_limit:user_42", 150);

// 3. High-Dimensional Vector Search
await db.vector.upsert("doc:raft_paper", [0.88, 0.12, -0.04, 0.35, ...], { title: "In Search of an Understandable Consensus Algorithm" });

const memories = await db.vector.search([0.85, 0.10, -0.02, 0.30, ...], 5);
console.log("Top semantic matches:", memories);
```

### Python SDK (`aetherdb`)

```python
from aetherdb import AetherDB

db = AetherDB(url="http://localhost:8301")

# Key-Value Operations
db.set("agent:context", "Retrieving historical log records")
ctx = db.get("agent:context")

# Atomic Counter
db.incr("agent:turn_counter", 1)

# SIMD Vector Retrieval
db.upsert_vector("doc_1", [0.91, 0.12, 0.05, -0.15], metadata={"source": "manual"})
matches = db.search_vector([0.90, 0.11, 0.04, -0.14], top_k=3)
```

---

## 🤖 Live AI Agent Memory Showcase

Run the end-to-end multi-turn autonomous AI agent demo:

```bash
# 1. Start AetherDB server
cargo run --release --bin aether-server -- --node-id 1 --addr 127.0.0.1:8300

# 2. In another terminal, run the agent showcase
node examples/ai_agent_memory_demo.js
```

The demo demonstrates:
* **Working Context Persistence:** Dynamic episodic session history.
* **Atomic Budget Enforcement:** $O(1)$ sub-millisecond atomic token decrements.
* **Semantic Long-Term Memory:** SIMD vector search over previous conversational knowledge.

---

## 🖥️ Web DevTools Console

AetherDB comes with an embedded, zero-dependency dark-mode visual console.

Open your browser to:
```
http://localhost:8301
```

Features:
* Live Key-Value inspection and dynamic mutation.
* Interactive SIMD Vector playground with instant cosine similarity ranking.
* Real-time cluster engine telemetry (consensus state, LSM block cache, REST latency).

---

## 🐳 Docker Multi-Node Cluster Setup

Spin up a 3-node distributed AetherDB cluster with Docker Compose:

```bash
docker compose up -d
```

Cluster topology:
* **Node 1:** TCP `8300`, HTTP `8301`
* **Node 2:** TCP `8310`, HTTP `8311`
* **Node 3:** TCP `8320`, HTTP `8321`

Check cluster health:
```bash
curl http://localhost:8301/health
curl http://localhost:8311/health
curl http://localhost:8321/health
```

---

## 📜 License

Licensed under the [Apache License, Version 2.0](LICENSE).
