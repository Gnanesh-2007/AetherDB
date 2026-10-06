# 🗺️ AetherDB Project Roadmap

AetherDB is an open-source, AI-native state and semantic memory engine designed specifically for autonomous agents. This roadmap outlines the strategic milestones from early alpha to production-grade distributed infrastructure.

---

## 📍 Phase 1 — Foundation & Developer Preview (v0.1.0 — Current)

- [x] **Core LSM Engine:** Concurrent SkipList MemTable, append-only CRC32 Write-Ahead Log (WAL), and immutable SSTables with Block Bloom Filters.
- [x] **Vector Search Subsystem:** In-memory Hierarchical Navigable Small World (HNSW) graph index with AVX2 SIMD-accelerated cosine distance kernels.
- [x] **Agent-Native Primitives:** `agent.state.set/get/incr/delete` and `agent.memory.remember/recall` APIs.
- [x] **Concurrency & Atomicity:** Atomic INCR operations for token quotas, step counters, and usage tracking.
- [x] **Distributed Consensus Prototype:** Multi-Raft key-range routing, Hybrid Logical Clock (HLC), and MVCC Two-Phase Commit (2PC) coordinator.
- [x] **Client Libraries:** Python SDK, TypeScript/JavaScript SDK, and Operator CLI (`aether`).
- [x] **AI Framework Adapters:** Official LangChain `ChatMessageHistory` and LlamaIndex `KVStore` integrations.
- [x] **Developer Console:** Real-time Web UI for cluster topology, agent fleet inspection, and vector search debugging.
- [x] **Automated Test Suite:** 102/102 automated unit and integration tests passing across Rust, Python, and TypeScript.

---

## 📦 Phase 2 — Packaging, Distribution & DX (v0.2.0)

- [ ] **PyPI Publishing:** Official `pip install aetherdb` package with pre-compiled wheels for Linux, macOS, and Windows.
- [ ] **npm Publishing:** Official `@aetherdb/sdk` TypeScript package distributed on npm.
- [ ] **Docker Hub Automated Builds:** Official `aetherdb/aetherdb:latest` and multi-arch container images (`linux/amd64`, `linux/arm64`).
- [ ] **Pre-Compiled Binaries:** Automated GitHub Releases with standalone binary artifacts for `aether-server` and `aether-cli`.
- [ ] **SDK Parity Enhancements:** Async/await support and connection pooling across all SDKs.

---

## 💾 Phase 3 — Storage Scaling & Disk-Backed Vectors (v0.3.0)

- [ ] **Disk-Backed Vector Indexing:** Implement memory-mapped (`mmap`) vector storage and DiskANN-inspired graph paging to support millions of vectors beyond RAM capacity.
- [ ] **Dynamic Leveled Compaction:** Size-tiered and leveled SSTable compaction policies with asynchronous background workers.
- [ ] **Block Cache Tuning:** Configurable LRU block cache with direct I/O support for NVMe storage devices.
- [ ] **Memory Decay & TTL:** First-class time-to-live (TTL) and episodic memory decay functions for agent memory retrieval.

---

## 🛡️ Phase 4 — Chaos Hardening & Jepsen Verification (v0.4.0)

- [ ] **Containerized Cluster Test Harness:** Automated 3-node and 5-node distributed cluster integration suite running in GitHub Actions.
- [ ] **Automated Chaos Injection:** Simulated network partitions, packet loss, leader preemption, and sudden node termination.
- [ ] **Jepsen-Style Linearizability Verification:** Formal state machine validation ensuring zero lost updates or split-brain inconsistencies under network anomalies.
- [ ] **Distributed Snapshotting & Restore:** Raft log compaction with incremental point-in-time snapshot replication.

---

## 🤖 Phase 5 — Agent Ecosystem Expansion (v0.5.0)

- [ ] **Native LangGraph Checkpointer:** First-class state saving and time-travel debugging for LangGraph agent workflows.
- [ ] **CrewAI & AutoGen Adapters:** Native multi-agent memory backend for CrewAI crews and Microsoft AutoGen conversations.
- [ ] **Semantic Kernel Connector:** Official C# and Python connector for Microsoft Semantic Kernel.
- [ ] **Hybrid Search:** Combined BM25 full-text search and SIMD dense vector search with Reciprocal Rank Fusion (RRF).

---

## 🚀 Phase 6 — Production & Enterprise Readiness (v1.0.0)

- [ ] **High-Availability Multi-Region Replication:** Cross-datacenter Raft learner nodes with asynchronous replication.
- [ ] **Role-Based Access Control (RBAC):** Fine-grained tenant permissions and audit logging.
- [ ] **Prometheus & OpenTelemetry:** Distributed tracing integration with Jaeger and Prometheus alerting templates.
- [ ] **SOC2 & Compliance Readiness:** Encryption-at-rest for SSTables and WAL segments using AES-256-GCM.

---

## 💬 Community & Feedback

Have ideas, feature requests, or want to contribute?
- **GitHub Issues:** [File a bug or feature proposal](https://github.com/Gnanesh-2007/AetherDB/issues)
- **Discussions:** Share your agent architecture and memory requirements!
