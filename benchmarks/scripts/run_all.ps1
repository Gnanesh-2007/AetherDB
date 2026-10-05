# AetherDB Benchmark Runner (PowerShell)
Write-Host "⚡ Running AetherDB Low-Level Storage Engine Benchmarks..." -ForegroundColor Cyan
cargo run --release --bin aether-bench -- --num-ops 10000 --concurrency 8 --vector-dim 768

Write-Host "`n⚡ Running AetherDB AI-Agent Workload & Memory Benchmarks..." -ForegroundColor Cyan
cargo run --release --bin aether-agent-bench -- --agents 100 --num-ops 5000 --dimensions 768 --concurrency 8

Write-Host "`n✔ Benchmark run completed. Results written to benchmarks/results/latest.md and latest.json" -ForegroundColor Green
