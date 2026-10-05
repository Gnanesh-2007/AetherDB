# 📊 AetherDB Empirical Benchmark Suite

This directory contains the reproducible benchmark harness, synthetic workloads, and empirical results for AetherDB.

---

## 🎯 Benchmark Objectives

To evaluate AetherDB under realistic AI-Agent operational loads:
1. **Agent State Latency Profile:** Single-key and namespaced `SET`, `GET`, and `DELETE`.
2. **Multi-Thread Atomic INCR Scaling:** Lock-free atomic counter throughput and verification of zero lost updates.
3. **Semantic Memory & Vector Scaling:** Ingestion QPS and retrieval latency across dimensions (128, 384, 768) and dataset sizes (1K, 10K, 50K vectors).
4. **End-to-End Agent Workflow Latency:** Multi-step loop (`GET state` → `RECALL memories` → `INCR tokens` → `SET state`) under 1 to 64 concurrent workers.
5. **Persistence & Crash Recovery:** Reopen time and 100% data integrity verification across process termination.
6. **Agent & Tenant Isolation:** Proving 0 cross-agent or cross-tenant leaks under concurrent search.
7. **100 Concurrent Agents Sustained Stress:** Aggregate throughput and mathematical token conservation.

---

## 🚀 Running the Benchmarks

### 1. Run the AI-Agent Workload Suite

```bash
# Run agent benchmarks in optimized release mode
cargo run --release --bin aether-agent-bench -- \
  --agents 100 \
  --num-ops 5000 \
  --dimensions 768 \
  --concurrency 8
```

Outputs:
- Machine-readable JSON: `benchmarks/results/latest.json`
- Human-readable Markdown summary: `benchmarks/results/latest.md`

### 2. Run the Low-Level Storage Engine Benchmarks

```bash
# Run baseline storage engine benchmarks (Workloads A, A2, B, C, D, E)
cargo run --release --bin aether-bench -- --num-ops 10000 --concurrency 8 --vector-dim 768
```

### 3. Run All Benchmarks via Automation Script

```bash
# Windows PowerShell
.\benchmarks\scripts\run_all.ps1

# Linux / macOS Bash
./benchmarks/scripts/run_all.sh
```

---

## 📁 Directory Structure

```
benchmarks/
├── README.md                # Benchmark guide & instructions
├── results/                 # Output directory for benchmark artifacts
│   ├── latest.md            # Markdown summary of the most recent benchmark run
│   ├── latest.json          # Machine-readable JSON metrics of the most recent run
│   └── agent_benchmark_*.json # Historical benchmark runs with environment metadata
└── scripts/
    ├── run_all.ps1          # Windows runner script
    └── run_all.sh           # Unix runner script
```

---

## 📄 Full Technical Validation Report

For deep architectural comparisons, hardware specifications, and honest limitations, see:
[`docs/aetherdb-technical-validation.md`](../docs/aetherdb-technical-validation.md).
