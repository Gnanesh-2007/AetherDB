# Contributing to AetherDB

Thank you for your interest in contributing to **AetherDB**! AetherDB is an open-source, AI-native storage engine written in Rust designed to give autonomous agents durable state and long-term semantic memory.

Whether you're fixing a bug, adding a new SDK feature, improving documentation, or writing benchmarks, we welcome your contributions!

---

## 🛠️ Development Setup

### Prerequisites

- **Rust:** 1.80+ (`rustup default stable`)
- **C/C++ Build Tools:** Required for compiling native dependencies (`gcc`/`clang` on Linux/macOS, MSVC C++ Build Tools on Windows).
- **Python:** 3.10+ (for Python SDK and LangChain/LlamaIndex adapters)
- **Node.js:** 18+ (for TypeScript SDK)

### Clone & Build

```bash
# Clone the repository
git clone https://github.com/Gnanesh-2007/AetherDB.git
cd AetherDB

# Build the complete workspace (debug build)
cargo build

# Build release binaries
cargo build --release --bin aether-server --bin aether-cli
```

---

## 🧪 Running Tests

AetherDB has comprehensive automated test coverage across the Rust workspace, Python SDK, TypeScript SDK, and AI framework adapters.

### Rust Workspace Tests (44 tests)

```bash
# Run all workspace unit, integration, and chaos tests
cargo test --workspace

# Run specific crate tests
cargo test -p aether-storage
cargo test -p aether-simd
cargo test -p aether-network
```

### Python SDK & Framework Tests

```bash
cd sdks/python
pip install -e .
python -m unittest discover tests

# LangChain integration tests
cd ../../integrations/langchain
pip install -e .
python -m unittest discover tests

# LlamaIndex integration tests
cd ../llamaindex
pip install -e .
python -m unittest discover tests
```

### TypeScript SDK Tests

```bash
cd sdks/js
npm install
npm test
```

---

## 📁 Repository Structure

```text
AetherDB/
├── crates/
│   ├── aether-core/        # Shared primitives, HLC timestamping, MVCC keys
│   ├── aether-storage/     # LSM engine, SkipList MemTable, WAL, SSTables, Bloom filters
│   ├── aether-simd/        # AVX2/FMA hardware-accelerated cosine & dot product math
│   ├── aether-vector/      # HNSW vector graph indexing & nearest neighbor search
│   ├── aether-raft/        # Raft consensus state machine & log replication
│   ├── aether-multiraft/   # Dynamic key-range partitioning & Raft group routing
│   ├── aether-txn/         # MVCC snapshot isolation & distributed 2PC coordinator
│   ├── aether-network/     # HTTP REST gateway, binary protocol, authentication, console
│   ├── aether-server/      # Standalone database daemon binary
│   ├── aether-cli/         # Developer and operator terminal CLI
│   └── aether-chaos/       # Fault injection, fuzzing, and linearizability verification
├── sdks/
│   ├── python/             # Python client library (`aetherdb`)
│   └── js/                 # TypeScript/JavaScript client library (`@aetherdb/sdk`)
├── integrations/
│   ├── langchain/          # Official LangChain ChatMessageHistory integration
│   └── llamaindex/         # Official LlamaIndex KVStore integration
├── docs/                   # Architecture specs, deep dives, and API references
├── examples/               # End-to-end autonomous agent workflow examples
└── benchmarks/             # Storage, vector search, and transaction benchmarks
```

---

## 📐 Code Style & Guidelines

1. **Rust:** Ensure your code is formatted with `cargo fmt` and passes `cargo clippy` without warnings.
2. **Deterministic Tests:** Avoid flaky or timing-dependent tests. Use structured clocks or controlled mock ticks.
3. **Safety First:** Unsafe code is strictly limited to SIMD intrinsics in `aether-simd` with explicit runtime CPU feature detection (`is_x86_feature_detected!("avx2")`).
4. **Documentation:** Include Rustdoc comments on all public types, structs, and functions.

---

## 🚀 Pull Request Workflow

1. **Fork** the repository and create your branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
2. **Write code and tests** covering new functionality.
3. **Verify all tests pass:**
   ```bash
   cargo test --workspace
   ```
4. **Commit** with clear, semantic commit messages (e.g. `feat(vector): add disk-backed mmap index`, `fix(storage): resolve WAL header alignment`).
5. **Open a Pull Request** describing the motivation, implementation details, and verification steps.

---

## 📜 License

By contributing to AetherDB, you agree that your contributions will be licensed under the [Apache License, Version 2.0](LICENSE).
