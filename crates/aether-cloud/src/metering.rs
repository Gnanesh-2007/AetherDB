use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProjectUsage {
    pub kv_reads: u64,
    pub kv_writes: u64,
    pub vector_upserts: u64,
    pub vector_searches: u64,
    pub token_operations: u64,
    pub storage_bytes: u64,
}

impl ProjectUsage {
    pub fn total_operations(&self) -> u64 {
        self.kv_reads
            + self.kv_writes
            + self.vector_upserts
            + self.vector_searches
            + self.token_operations
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageReport {
    pub project_id: String,
    pub usage: ProjectUsage,
    pub max_operations: u64,
    pub max_vectors: u64,
    pub max_storage_bytes: u64,
    pub ops_percentage: f64,
    pub vectors_percentage: f64,
    pub storage_percentage: f64,
}

pub struct MeteringEngine {
    usage_map: RwLock<HashMap<String, ProjectUsage>>,
}

impl MeteringEngine {
    pub fn new() -> Self {
        let mut engine = Self {
            usage_map: RwLock::new(HashMap::new()),
        };

        // Seed initial realistic activity for default project
        let default_usage = ProjectUsage {
            kv_reads: 48_210,
            kv_writes: 19_400,
            vector_upserts: 1_250,
            vector_searches: 8_420,
            token_operations: 14_300,
            storage_bytes: 48_500_000, // 48.5 MB
        };
        engine
            .usage_map
            .get_mut()
            .insert("proj_live_01".to_string(), default_usage);

        engine
    }

    pub fn record_operation(&self, project_id: &str, op_type: &str, count: u64) {
        let mut map = self.usage_map.write();
        let usage = map.entry(project_id.to_string()).or_default();
        match op_type {
            "GET" => usage.kv_reads += count,
            "SET" => usage.kv_writes += count,
            "DEL" => usage.kv_writes += count,
            "INCR" => usage.token_operations += count,
            "UPSERT_VECTOR" => usage.vector_upserts += count,
            "VECTOR_SEARCH" => usage.vector_searches += count,
            _ => {}
        }
    }

    pub fn set_storage_bytes(&self, project_id: &str, bytes: u64) {
        let mut map = self.usage_map.write();
        let usage = map.entry(project_id.to_string()).or_default();
        usage.storage_bytes = bytes;
    }

    pub fn get_usage(&self, project_id: &str) -> ProjectUsage {
        self.usage_map
            .read()
            .get(project_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn generate_report(
        &self,
        project_id: &str,
        max_ops: u64,
        max_vectors: u64,
        max_storage_bytes: u64,
    ) -> UsageReport {
        let usage = self.get_usage(project_id);
        let total_ops = usage.total_operations();

        let ops_pct = if max_ops > 0 {
            ((total_ops as f64 / max_ops as f64) * 100.0).min(100.0)
        } else {
            0.0
        };

        let vec_pct = if max_vectors > 0 {
            ((usage.vector_upserts as f64 / max_vectors as f64) * 100.0).min(100.0)
        } else {
            0.0
        };

        let storage_pct = if max_storage_bytes > 0 {
            ((usage.storage_bytes as f64 / max_storage_bytes as f64) * 100.0).min(100.0)
        } else {
            0.0
        };

        UsageReport {
            project_id: project_id.to_string(),
            usage,
            max_operations: max_ops,
            max_vectors,
            max_storage_bytes,
            ops_percentage: (ops_pct * 10.0).round() / 10.0,
            vectors_percentage: (vec_pct * 10.0).round() / 10.0,
            storage_percentage: (storage_pct * 10.0).round() / 10.0,
        }
    }
}
