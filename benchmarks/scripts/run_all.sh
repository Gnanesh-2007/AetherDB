#!/usr/bin/env bash
set -e

echo "⚡ Running AetherDB Low-Level Storage Engine Benchmarks..."
cargo run --release --bin aether-bench -- --num-ops 10000 --concurrency 8 --vector-dim 768

echo ""
echo "⚡ Running AetherDB AI-Agent Workload & Memory Benchmarks..."
cargo run --release --bin aether-agent-bench -- --agents 100 --num-ops 5000 --dimensions 768 --concurrency 8

echo ""
echo "✔ Benchmark run completed. Results written to benchmarks/results/latest.md and latest.json"
