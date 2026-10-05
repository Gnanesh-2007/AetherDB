# ⚡ AetherDB

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.80%2B-orange.svg)](https://www.rust-lang.org/)
[![Python SDK](https://img.shields.io/badge/Python%20SDK-0.1.0-blue.svg)](sdks/python)
[![TypeScript SDK](https://img.shields.io/badge/%40aetherdb%2Fsdk-0.1.0-brightgreen.svg)](sdks/js)
[![Release](https://img.shields.io/badge/Release-v0.1.0-brightgreen.svg)]()
[![Deployment Validation](https://img.shields.io/badge/Public%20Deployment-21%2F21%20PASS-success.svg)]()

> **AetherDB is an AI-native persistent storage system for autonomous agents.**  
> AetherDB provides autonomous AI agents with durable state, long-term semantic memory, atomic operations, vector search, transactions, and distributed persistence through a unified developer API.

---

## 🌐 Public Deployment & Production Validation

AetherDB v0.1.0 has been deployed and validated in a live public environment over TLS/HTTPS:

- **Public Endpoint:** `https://d60f5382278503.lhr.life` *(Public TLS edge with reverse proxy terminating to internal REST gateway)*
- **Developer Console:** `https://d60f5382278503.lhr.life/dashboard`
- **Validation Result:** `21 / 21 Tests Passed (100%)`

### Security Perimeter & Architectural Safeguards
- **Strict Cryptographic Authentication:** All data endpoints require `Bearer aether_sk_<tenant>_<secret>` evaluated via constant-time token comparison (`ct_eq`). Forged tokens and header-only bypasses (`X-Aether-Tenant`) are rejected with HTTP 401.
- **Tenant & Agent Isolation:** Memory recall and state lookups are cryptographically and namespace-isolated. Tenant B cannot access Tenant A data; Agent Y cannot recall memories from Agent X.
- **Private Consensus:** Internal Multi-Raft consensus (port `8300`) is bound strictly to `127.0.0.1` and is never exposed to the public internet.
- **Crash & Restart Resilience:** Validated against a clean production storage directory (`data_production`). State, atomic counters, semantic memories, and SIMD vectors survive full process restarts.
- **Protected Metrics & CORS:** Internal Prometheus `/metrics` are inaccessible without valid authentication, and wildcard CORS policies (`*`) are disallowed under strict mode.

> **Latency Notice:** Public external roundtrips over TLS reverse proxies showed `p50 = 1,445 ms`, `p95 = 3,475 ms`, and `max = 6,019 ms`. These figures reflect wide-area network latency and TLS handshakes, whereas local engine benchmarks achieve sub-millisecond execution (`p50 = 0.031 ms` / `31 µs` for WAL + MemTable writes).

---

## 🎯 Why AI Agents Need Persistent State & Memory

Standard web architectures separate databases, vector indices, cache tiers, and task queues across different systems. Autonomous AI agents have unique requirements that break down when using disparate infrastructure:

1. **Epistemic State vs. Ephemeral Caches:** Agents need structured scratchpad memory and step checkpoints that survive process crashes and orchestrator restarts.
2. **Atomic Concurrent Accounting:** Tracking token quotas, step counters, and usage metrics across concurrent tools requires atomic INCR primitives with key-level isolation and zero lost updates.
3. **Unified Semantic Memory:** Agent memories consist of structured metadata (timestamps, domains, parent task IDs) coupled with dense vector embeddings. Storing them together ensures atomic updates and eliminates dual-write drift.
4. **Tenant & Agent Namespace Isolation:** Multiple agents operating within multi-tenant organizations must remain strictly isolated without complex relational join filtering.

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
- **Storage Subsystem:** Concurrent SkipList MemTable + CRC32 Write-Ahead Log (WAL) + SSTables with Block Bloom Filters and LRU caching.
- **Consistency & Transactions:** Monotonic Hybrid Logical Clocks (HLC) + Multi-Version Concurrency Control (MVCC) + Distributed 2-Phase Commit (2PC).
- **Consensus:** Multi-Raft consensus groups with dynamic key-range routing.
- **Vector Memory:** Hierarchical Navigable Small World (HNSW) graph indexing with AVX2 SIMD cosine distance kernels.

---

## ⚡ Quickstart

### 1. Build & Run from Source

```bash
# Clone the repository
git clone https://github.com/Gnanesh-2007/AetherDB.git
cd AetherDB

# Build release server binary
cargo build --release --bin aether-server

# Run single node
./target/release/aether-server --node-id 1 --addr 127.0.0.1:8300 --http-addr 127.0.0.1:8301 --data-dir data_node1
```

---

## 💻 SDK & Integration Usage

### Python SDK

Install the Python client:
```bash
pip install -e sdks/python
```

```python
from aetherdb import AetherDB

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

Install the JavaScript SDK:
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
from langchain_core.messages import HumanMessage, AIMessage

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

## 🐳 Docker Deployment

AetherDB provides production Docker and Docker Compose configurations:

```bash
# 1. Single Node container
docker build -t aetherdb:v0.1.0 .
docker run -p 8300:8300 -p 8301:8301 -e AETHERDB_REQUIRE_AUTH=false aetherdb:v0.1.0

# 2. 3-Node Multi-Raft Cluster
docker compose -f docker-compose.cluster.yml up -d
```

*(Note: Docker Compose configuration is provided and statically validated. The 3-node cluster runtime deployment was NOT executed during testing because the local development host did not have an active Docker daemon).*

---

## 📊 Observability & Metrics

- **Liveness Probe:** `GET /health` (`{"status":"healthy","engine":"aetherdb-rust","version":"0.1.0"}`)
- **Readiness Probe:** `GET /readiness` (`{"status":"ready","node_id":1,"ready":true}`)
- **Prometheus Metrics:** `GET /metrics` (Guarded by API key in strict authentication mode)
- **Developer Console:** Open `http://localhost:8301/dashboard` in your browser for real-time agent fleet observability, cluster topology, and vector search inspector.

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

### Local Engine vs. Public Deployment Latency

- **Local Storage Engine:** Direct in-process / local storage benchmarks execute in microseconds (`p50 = 0.031 ms` / `31 µs` for writes, `p50 = 0.010 ms` / `10 µs` for 2PC transactions).
- **Public Deployment Round-Trip Latency:** External client requests to the public HTTPS endpoint (`https://d60f5382278503.lhr.life`) include wide-area network latency, TLS handshakes, and reverse proxy forwarding (`p50 = 1,445 ms`, `p95 = 3,475 ms`, `max = 6,019 ms`).

---

## ⚠️ Current Scope & Limitations

- **Specialized Workloads:** AetherDB is designed specifically for autonomous agent memory, state, and vector retrieval. It does not provide general SQL joins or multi-table OLAP aggregation engines.
- **Memory Scaling:** HNSW graph structures are held in memory for sub-millisecond traversal and backed by the LSM Write-Ahead Log for persistent recovery.
- **Embedding Dimensions:** SIMD cosine kernels support arbitrary vector dimensions (recommended $\le 4096$).

---

## 📜 License

Licensed under the [Apache License, Version 2.0](LICENSE).
