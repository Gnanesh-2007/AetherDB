use aether_bench::metrics::{BenchmarkReport, EnvironmentMetadata, LatencyHistogram};
use std::time::Duration;

#[test]
fn test_latency_histogram_percentiles() {
    let mut hist = LatencyHistogram::with_capacity(101);
    for i in 0..=100 {
        hist.record(Duration::from_micros(i * 10)); // 0us to 1000us (101 values)
    }

    let report = hist.report("test_workload", Duration::from_millis(100));

    assert_eq!(report.total_ops, 101);
    assert_eq!(report.min_us, 0);
    assert_eq!(report.max_us, 1000);
    assert_eq!(report.p50_us, 500);
    assert_eq!(report.p90_us, 900);
    assert_eq!(report.p95_us, 950);
    assert_eq!(report.p99_us, 990);
    assert_eq!(report.p999_us, 1000);
    assert!((report.avg_us - 500.0).abs() < 1e-6);
}

#[test]
fn test_environment_metadata_collection() {
    let env = EnvironmentMetadata::collect();
    assert!(!env.os.is_empty());
    assert!(!env.arch.is_empty());
    assert!(env.num_cpus >= 1);
    assert!(!env.aether_version.is_empty());
}

#[test]
fn test_benchmark_report_serialization() {
    let report = BenchmarkReport {
        workload_name: "Agent State GET".to_string(),
        total_ops: 1000,
        elapsed_ms: 50.0,
        ops_per_sec: 20000.0,
        min_us: 20,
        p50_us: 45,
        p90_us: 70,
        p95_us: 85,
        p99_us: 120,
        p999_us: 200,
        max_us: 350,
        avg_us: 48.5,
    };

    let json = serde_json::to_string(&report).unwrap();
    let parsed: BenchmarkReport = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed.workload_name, "Agent State GET");
    assert_eq!(parsed.total_ops, 1000);
    assert_eq!(parsed.p50_us, 45);
    assert_eq!(parsed.p99_us, 120);
}
