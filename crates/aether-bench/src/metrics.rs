use std::time::Duration;

#[derive(Debug, Clone)]
pub struct BenchmarkReport {
    pub workload_name: String,
    pub total_ops: usize,
    pub elapsed: Duration,
    pub ops_per_sec: f64,
    pub min_us: u64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
    pub avg_us: f64,
}

pub struct LatencyHistogram {
    latencies_us: Vec<u64>,
}

impl LatencyHistogram {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            latencies_us: Vec::with_capacity(capacity),
        }
    }

    pub fn record(&mut self, duration: Duration) {
        self.latencies_us.push(duration.as_micros() as u64);
    }

    pub fn extend(&mut self, other: LatencyHistogram) {
        self.latencies_us.extend(other.latencies_us);
    }

    pub fn report(&mut self, workload_name: &str, elapsed: Duration) -> BenchmarkReport {
        let total_ops = self.latencies_us.len();
        if total_ops == 0 {
            return BenchmarkReport {
                workload_name: workload_name.to_string(),
                total_ops: 0,
                elapsed,
                ops_per_sec: 0.0,
                min_us: 0,
                p50_us: 0,
                p90_us: 0,
                p95_us: 0,
                p99_us: 0,
                p999_us: 0,
                max_us: 0,
                avg_us: 0.0,
            };
        }

        self.latencies_us.sort_unstable();

        let sum: u64 = self.latencies_us.iter().sum();
        let avg_us = (sum as f64) / (total_ops as f64);
        let ops_per_sec = (total_ops as f64) / elapsed.as_secs_f64();

        let percentile = |p: f64| -> u64 {
            let idx = (((total_ops as f64) * p / 100.0).round() as usize).min(total_ops - 1);
            self.latencies_us[idx]
        };

        BenchmarkReport {
            workload_name: workload_name.to_string(),
            total_ops,
            elapsed,
            ops_per_sec,
            min_us: self.latencies_us[0],
            p50_us: percentile(50.0),
            p90_us: percentile(90.0),
            p95_us: percentile(95.0),
            p99_us: percentile(99.0),
            p999_us: percentile(99.9),
            max_us: *self.latencies_us.last().unwrap(),
            avg_us,
        }
    }
}

pub fn print_report(report: &BenchmarkReport) {
    println!("┌────────────────────────────────────────────────────────────────────────┐");
    println!("│ Workload: {:<60} │", report.workload_name);
    println!("├────────────────────────────────────────────────────────────────────────┤");
    println!("│ Total Operations:  {:<12} Elapsed Time: {:<20} │", report.total_ops, format!("{:.2?}", report.elapsed));
    println!("│ Throughput:        {:<12.2} ops/sec                               │", report.ops_per_sec);
    println!("├────────────────────────────────────────────────────────────────────────┤");
    println!("│ Latency Profile:                                                       │");
    println!("│   • Min:      {:>8.3} ms                                           │", report.min_us as f64 / 1000.0);
    println!("│   • p50:      {:>8.3} ms  (Median)                                 │", report.p50_us as f64 / 1000.0);
    println!("│   • p90:      {:>8.3} ms                                           │", report.p90_us as f64 / 1000.0);
    println!("│   • p95:      {:>8.3} ms                                           │", report.p95_us as f64 / 1000.0);
    println!("│   • p99:      {:>8.3} ms  (Tail)                                   │", report.p99_us as f64 / 1000.0);
    println!("│   • p99.9:    {:>8.3} ms                                           │", report.p999_us as f64 / 1000.0);
    println!("│   • Max:      {:>8.3} ms                                           │", report.max_us as f64 / 1000.0);
    println!("│   • Average:  {:>8.3} ms                                           │", report.avg_us / 1000.0);
    println!("└────────────────────────────────────────────────────────────────────────┘\n");
}
