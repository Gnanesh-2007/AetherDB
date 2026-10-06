# ⚡ AetherDB

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.80%2B-orange.svg)](https://www.rust-lang.org/)
[![Automated Tests](https://img.shields.io/badge/Tests-102%2F102%20PASS-brightgreen.svg)]()
[![Python SDK](https://img.shields.io/badge/Python%20SDK-0.1.0-blue.svg)](sdks/python)
[![TypeScript SDK](https://img.shields.io/badge/%40aetherdb%2Fsdk-0.1.0-brightgreen.svg)](sdks/js)
[![Release Stage](https://img.shields.io/badge/Status-v0.1.0--alpha%20%7C%20Developer%20Preview-yellow.svg)](ROADMAP.md)

> **AetherDB is an open-source, AI-native state and semantic memory engine written in Rust.**  
> It provides autonomous AI agents with durable structured state, long-term semantic memory, concurrent atomic counters, SIMD vector search, and distributed persistence through a unified developer API.

---

## 🎯 Why AetherDB?

Standard AI agent stacks stitch together 3 to 4 disparate databases:
- **Redis** for fast ephemeral scratchpad memory and step counters.
- **PostgreSQL** for durable session state, task queues, and user records.
- **Pinecone / Qdrant / Milvus** for semantic vector embeddings.

This composite architecture introduces **dual-write drift** (where vector indexes update but relational metadata fails), complex relational joins, and high operational overhead.

AetherDB solves this by unifying **epistemic agent state**, **atomic accounting**, and **SIMD vector search** into a single, high-performance Rust storage engine:

```text
       ┌────────────────────────────────────────────────────────┐
       │              Autonomous AI Agent / Application         │
       │   Python SDK · TypeScript SDK · LangChain · LlamaIndex │
       └───────────────────────────┬────────────────────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
       ┌────────────────────────┐    ┌────────────────────────┐
       │ Structured State & KV  │    │ Long-Term Vector Memory│
       │ • Epistemic Sessions   │    │ • Dense HNSW Index     │
       │ • Step Checkpoints     │    │ • AVX2 SIMD Cosine     │
       │ • Atomic Token INCR    │    │ • Text & Metadata      │
       └────────────┬───────────┘    └────────────┬───────────┘
                    └──────────────┬──────────────┘
                                   ▼
       ┌────────────────────────────────────────────────────────┐
       │                AetherDB Unified Rust Engine            │
       │ LSM Storage (SkipList + WAL + SSTables) + Multi-Raft   │
       └────────────────────────────────────────────────────────┘
```

---

## 🏗️ Architecture Overview

```text
                               ┌────────────────────────────────────────────────────────┐
                               │             AI Agent Application / SDK                 │
                               │  Python SDK · TypeScript SDK · LangChain · LlamaIndex  │
                               └───────────────────────────┬────────────────────────────┘
                                                           │
                                 ┌─────────────────────────┼─────────────────────────┐
                                 ▼                         ▼                         ▼
                      ┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
                      │    Node 1 (Port 8300)│  │    Node 2 (Port 8310)│  │    Node 3 (Port 8320)│
                      │ ┌──────────────────┐ │  │ ┌──────────────────┐ │  │ ┌──────────────────┐ │
                      │ │ Multi-Raft Leader│◄┼──┼─┼► Raft Follower   │◄─┼──┼─┼► Raft Follower   │ │
                      │ └────────┬─────────┘ │  │ └────────┬─────────┘  │  │ └────────┬─────────┘ │
                      │          ▼           │  │          ▼            │  │          ▼            │
                      │ ┌──────────────────┐ │  │ ┌──────────────────┐  │  │ ┌──────────────────┐ │
                      │ │ MVCC + 2PC Layer │ │  │ │ MVCC + 2PC Layer │  │  │ │ MVCC + 2PC Layer │ │
                      │ └────────┬─────────┘ │  │ └────────┬─────────┘  │  │ └────────┬─────────┘ │
                      │          ▼           │  │          ▼            │  │          ▼            │
                      │ ┌──────────────────┐ │  │ ┌──────────────────┐  │  │ ┌──────────────────┐ │
                      │ │ LSM Tree + AVX2  │ │  │ │ LSM Tree + AVX2  │  │  │ │ LSM Tree + AVX2  │ │
                      │ └────────┬─────────┘ │  │ └────────┬─────────┘  │  │ └────────┬─────────┘ │
                      │ ┌────────┴─────────┐ │  │ ┌────────┴─────────┐  │  │ ┌────────┴─────────┐ │
                      │ │ HTTP / Port 8301 │ │  │ │ HTTP / Port 8311 │  │  │ │ HTTP / Port 8321 │ │
                      │ └──────────────────┘ │  │ └──────────────────┘  │  │ └──────────────────┘ │
                      └──────────────────────┘  └──────────────────────┘  └──────────────────────┘
```

### Core Engine Primitives

- **Storage Engine:** Concurrent SkipList MemTable + append-only CRC32 Write-Ahead Log (WAL) + immutable SSTables with Block Bloom Filters and LRU block cache.
- **Hardware-Accelerated SIMD Vectors:** High-throughput cosine distance computations using x86-64 **AVX2 / FMA** instructions with scalar fallback.
- **Vector Graph Search:** Hierarchical Navigable Small World (**HNSW**) in-memory graph index with deterministic LSM recovery.
- **Concurrency & Accounting:** Atomic concurrent `INCR` operations for token quotas, step counters, and usage accounting.
- **Distributed Consensus Prototype:** Multi-Raft key-range sharding, Monotonic Hybrid Logical Clocks (**HLC**), and MVCC Two-Phase Commit (**2PC**) coordinator.

📖 **Technical Deep Dives:**
- [LSM Storage & WAL Internals](docs/architecture/lsm_and_storage.md)
- [AVX2 SIMD Math & HNSW Vector Graph](docs/architecture/simd_and_hnsw.md)
- [Multi-Raft Consensus & Distributed MVCC](docs/architecture/distributed_consensus.md)

---

## ⚡ Quickstart

### 1. Build & Run from Source

```bash
# Clone the repository
git clone https://github.com/Gnanesh-2007/AetherDB.git
cd AetherDB

# Build release server binary
cargo build --release --bin aether-server

# Run a local AetherDB node
./target/release/aether-server --node-id 1 --addr 127.0.0.1:8300 --http-addr 127.0.0.1:8301 --data-dir data_node1
```

---

## 💻 SDK & Integration Usage

### Python SDK

Install the local development package:
```bash
pip install -e sdks/python
```

```python
from aetherdb import AetherDB

# Connect to local or remote AetherDB node
db = AetherDB("http://127.0.0.1:8301", api_key="aether_sk_tenantA_secret")
agent = db.agent("research-agent")

# 1. Structured Persistent State
agent.state.set("session", {
    "task": "Distributed consensus research",
    "status": "running",
    "step": 4
})
state = agent.state.get("session")

# 2. Concurrent Atomic Counters
tokens = agent.state.incr("tokens", 150)

# 3. Store Long-Term Semantic Memory
agent.memory.remember(
    id="raft-001",
    text="Raft provides replicated state machine consensus across nodes.",
    embedding=[0.92, 0.08, 0.0, 0.0],
    metadata={"domain": "consensus"}
)

# 4. Recall Memories via SIMD Cosine Search
results = agent.memory.recall(
    query="How does Raft maintain consistency?",
    embedding=[0.95, 0.05, 0.0, 0.0],
    top_k=5
)
```

### TypeScript / JavaScript SDK

Install the local package:
```bash
npm install ./sdks/js
```

```typescript
import { AetherDB } from "@aetherdb/sdk";

const db = new AetherDB("http://127.0.0.1:8301", {
  apiKey: "aether_sk_tenantA_secret"
});
const agent = db.agent("research-agent");

// Store state and increment counter
await agent.state.set("checkpoint", { step: 10, memoryUsage: "128MB" });
const tokens = await agent.state.incr("tokens", 250);

// Remember and Recall semantic memory
await agent.memory.remember({
  id: "hnsw-01",
  text: "HNSW allows approximate nearest neighbor search in logarithmic time.",
  embedding: [0.15, 0.85, 0.2, 0.0]
});

const memories = await agent.memory.recall({
  embedding: [0.12, 0.88, 0.18, 0.0],
  topK: 3
});
```

### LangChain Integration

```python
from aetherdb_langchain import AetherDBChatMessageHistory

# Persistent chat history stored directly in AetherDB Agent State
history = AetherDBChatMessageHistory(
    agent_id="support-agent-42",
    session_key="customer-thread-109",
    endpoint="http://127.0.0.1:8301"
)

history.add_user_message("Can you explain Raft consensus?")
history.add_ai_message("Raft is a leader-based consensus algorithm...")

# Automatically restored across agent restarts
print(history.messages)
```

### LlamaIndex Integration

```python
from aetherdb_llamaindex import AetherDBKVStore

# Persistent document store for LlamaIndex retrieval pipelines
kvstore = AetherDBKVStore(
    agent_id="indexer-agent",
    endpoint="http://127.0.0.1:8301"
)

kvstore.put("doc-chunk-01", {"text": "AetherDB LSM architecture...", "source": "docs"})
doc = kvstore.get("doc-chunk-01")
```

---

## 🛠️ CLI Tooling

AetherDB includes an operator CLI (`aether`):

```bash
# Check cluster status and health
cargo run --release --bin aether -- health

# Manage agent state from terminal
cargo run --release --bin aether -- agent state set --agent-id research-agent --key session --value '{"status":"active"}'
cargo run --release --bin aether -- agent state get --agent-id research-agent --key session
cargo run --release --bin aether -- agent state incr --agent-id research-agent --key tokens --delta 100

# Remember and recall semantic memory
cargo run --release --bin aether -- agent memory remember --agent-id research-agent --memory-id mem-1 --text "Vector recall test" --embedding "[0.1, 0.2, 0.3, 0.4]"
cargo run --release --bin aether -- agent memory recall --agent-id research-agent --embedding "[0.1, 0.2, 0.3, 0.4]" --top-k 3
```

---

## 📊 Developer Console & Observability

- **Developer Console:** Open `http://localhost:8301/dashboard` in your browser for real-time agent fleet observability, cluster topology, memory trace inspection, and interactive API playgrounds.
- **Liveness Probe:** `GET /health` (`{"status":"healthy","engine":"aetherdb-rust","version":"0.1.0"}`)
- **Readiness Probe:** `GET /readiness` (`{"status":"ready","node_id":1,"ready":true}`)
- **Telemetry Snapshot:** `GET /v1/telemetry` (Live JSON telemetry for cluster health, RPS, latency, and vector counts)
- **Prometheus Metrics:** `GET /metrics` (Prometheus exposition format)

---

## 🔬 Local Engine Benchmarks

Empirical microbenchmark results on bare-metal local storage (`release` build, AVX2 SIMD enabled):

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

## 🗺️ Project Status & Roadmap

AetherDB is currently in **Developer Preview (v0.1.0-alpha)**. While core single-node LSM storage, AVX2 SIMD kernels, and SDKs are tested with 102/102 automated tests, multi-node clustering and distributed recovery are under active development.

Check out our full roadmap: [ROADMAP.md](ROADMAP.md)

- **v0.2.0:** PyPI / npm package distribution & automated multi-arch Docker images.
- **v0.3.0:** Disk-backed vector storage (mmap / DiskANN-inspired indexing) & TTL memory decay.
- **v0.4.0:** Formal Jepsen-style chaos testing and automated network partition resilience.
- **v0.5.0:** Native adapters for LangGraph, CrewAI, AutoGen, and Semantic Kernel.

---

## 🤝 Contributing

We welcome contributions from developers, researchers, and systems enthusiasts! Please check out [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, test execution, and pull request guidelines.

---

## 📜 License

Licensed under the [Apache License, Version 2.0](LICENSE).
