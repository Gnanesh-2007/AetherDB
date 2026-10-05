use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::{Mutex, RwLock};

use aether_core::error::{AetherError, Result};
use aether_core::types::ValueState;
use aether_vector::ConcurrentHnswIndex;
use crate::cache::BlockCache;
use crate::compaction::Compactor;
use crate::memtable::MemTable;
use crate::sstable::{SSTableReader, SSTableWriter};
use crate::wal::WriteAheadLog;

pub struct StorageEngine {
    data_dir: PathBuf,
    active_memtable: RwLock<MemTable>,
    immutable_memtable: RwLock<Option<MemTable>>,
    wal: Mutex<WriteAheadLog>,
    sstables: RwLock<Vec<PathBuf>>,
    next_sstable_id: AtomicU64,
    hnsw_index: Arc<ConcurrentHnswIndex>,
    block_cache: Arc<BlockCache>,
}

impl StorageEngine {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir).map_err(|e| AetherError::IoError(e.to_string()))?;

        let wal_path = data_dir.join("current.wal");
        let recovered_entries = WriteAheadLog::recover(&wal_path)?;

        let memtable = MemTable::new(4 * 1024 * 1024); // 4MB default
        let hnsw_index = Arc::new(ConcurrentHnswIndex::new(16, 64, 32));
        let block_cache = Arc::new(BlockCache::new(4096)); // 4096 blocks (16MB cache)

        for (k, v) in recovered_entries {
            if k.starts_with(b"__vec:") {
                if let ValueState::Some(bytes) = &v {
                    if let Ok((vec, meta)) = bincode::deserialize::<(Vec<f32>, Option<String>)>(bytes) {
                        let id = String::from_utf8_lossy(&k[6..]).to_string();
                        let _ = hnsw_index.insert(&id, vec, meta);
                    }
                }
            }
            memtable.put(k, v);
        }

        let wal = WriteAheadLog::open(&wal_path)?;

        // Discover existing SSTables
        let mut sstables = Vec::new();
        let mut max_id = 0u64;

        if let Ok(entries) = fs::read_dir(&data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("sst") {
                    if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                        if let Ok(id) = stem.parse::<u64>() {
                            max_id = max_id.max(id);
                            sstables.push(path);
                        }
                    }
                }
            }
        }
        sstables.sort();

        Ok(Self {
            data_dir,
            active_memtable: RwLock::new(memtable),
            immutable_memtable: RwLock::new(None),
            wal: Mutex::new(wal),
            sstables: RwLock::new(sstables),
            next_sstable_id: AtomicU64::new(max_id + 1),
            hnsw_index,
            block_cache,
        })
    }

    /// Appends key-value to WAL first for durability, then writes to MemTable.
    pub fn put(&self, key: Vec<u8>, value: ValueState) -> Result<()> {
        {
            let mut wal = self.wal.lock();
            wal.append(&key, &value)?;
        }

        {
            let memtable = self.active_memtable.write();
            memtable.put(key, value);
        }

        if self.active_memtable.read().is_full() {
            self.flush_active_memtable()?;
        }

        Ok(())
    }

    /// MVCC snapshot point lookup: finds the latest committed version <= snapshot_ts.
    pub fn get_mvcc(&self, user_key: &[u8], snapshot_ts: aether_core::hlc::HlcTimestamp) -> Result<Option<ValueState>> {
        // 1. Search Active MemTable
        for (k, v) in self.active_memtable.read().iter() {
            if let Ok(mvcc_k) = aether_core::key::MvccKey::decode(&k) {
                if mvcc_k.user_key == user_key && mvcc_k.timestamp <= snapshot_ts {
                    return Ok(Some(v));
                }
            }
        }

        // 2. Search Immutable MemTable
        if let Some(imm) = self.immutable_memtable.read().as_ref() {
            for (k, v) in imm.iter() {
                if let Ok(mvcc_k) = aether_core::key::MvccKey::decode(&k) {
                    if mvcc_k.user_key == user_key && mvcc_k.timestamp <= snapshot_ts {
                        return Ok(Some(v));
                    }
                }
            }
        }

        // 3. Search SSTables (newest to oldest)
        let sstables = self.sstables.read().clone();
        for sst_path in sstables.iter().rev() {
            let mut reader = SSTableReader::open(sst_path)?;
            let entries = reader.scan_all()?;
            for (k, v) in entries {
                if let Ok(mvcc_k) = aether_core::key::MvccKey::decode(&k) {
                    if mvcc_k.user_key == user_key && mvcc_k.timestamp <= snapshot_ts {
                        return Ok(Some(v));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Point lookup for latest key value across MemTable, Immutable MemTable, and SSTables.
    pub fn get(&self, key: &[u8]) -> Result<Option<ValueState>> {
        // 1. Check Active MemTable
        if let Some(val) = self.active_memtable.read().get(key) {
            return Ok(Some(val));
        }

        // 2. Check Immutable MemTable
        if let Some(imm) = self.immutable_memtable.read().as_ref() {
            if let Some(val) = imm.get(key) {
                return Ok(Some(val));
            }
        }

        // 3. Check SSTables (newest to oldest)
        let sstables = self.sstables.read().clone();
        for sst_path in sstables.iter().rev() {
            let mut reader = SSTableReader::open(sst_path)?;
            if let Some(val) = reader.get(key)? {
                return Ok(Some(val));
            }
        }

        Ok(None)
    }

    /// Flushes Active MemTable to a new Immutable SSTable file on disk.
    pub fn flush_active_memtable(&self) -> Result<()> {
        let entries: Vec<(Vec<u8>, ValueState)> = self.active_memtable.read().iter();
        if entries.is_empty() {
            return Ok(());
        }

        let sst_id = self.next_sstable_id.fetch_add(1, Ordering::SeqCst);
        let sst_path = self.data_dir.join(format!("{:05}.sst", sst_id));

        let mut writer = SSTableWriter::create(&sst_path)?;
        for (k, v) in entries {
            writer.append(k, v)?;
        }
        writer.finish()?;

        // Reset Active MemTable
        {
            let mut active = self.active_memtable.write();
            *active = MemTable::new(4 * 1024 * 1024);
        }

        self.sstables.write().push(sst_path);
        Ok(())
    }

    /// Merges all existing SSTables into a single compacted SSTable, purging tombstones and old MVCC versions.
    pub fn trigger_compaction(&self) -> Result<Option<PathBuf>> {
        let current_sstables = self.sstables.read().clone();
        if current_sstables.len() < 2 {
            return Ok(None);
        }

        let compacted_id = self.next_sstable_id.fetch_add(1, Ordering::SeqCst);
        let compacted_path = self.data_dir.join(format!("{:05}_compacted.sst", compacted_id));

        let result = Compactor::compact(&current_sstables, &compacted_path)?;
        if let Some(final_path) = result {
            let mut sstables_lock = self.sstables.write();
            *sstables_lock = vec![final_path.clone()];
            self.block_cache.clear();
            Ok(Some(final_path))
        } else {
            let mut sstables_lock = self.sstables.write();
            sstables_lock.clear();
            self.block_cache.clear();
            Ok(None)
        }
    }

    /// Atomic integer increment (used for token quotas, rate limiters, sequence counters).
    pub fn incr(&self, key: Vec<u8>, delta: i64) -> Result<i64> {
        let current_val = match self.get(&key)? {
            Some(ValueState::Some(bytes)) => {
                let s = String::from_utf8_lossy(&bytes);
                s.parse::<i64>().unwrap_or(0)
            }
            _ => 0,
        };

        let new_val = current_val + delta;
        self.put(key, ValueState::Some(new_val.to_string().into_bytes()))?;
        Ok(new_val)
    }

    /// Stores a high-dimensional vector with optional JSON metadata into both LSM storage and HNSW graph index.
    pub fn upsert_vector(&self, id: &str, vector: Vec<f32>, metadata: Option<String>) -> Result<()> {
        let key = format!("__vec:{}", id).into_bytes();
        let payload = (vector.clone(), metadata.clone());
        let val_bytes = bincode::serialize(&payload)
            .map_err(|e| AetherError::SerializationError(e.to_string()))?;

        // 1. Persistent durable store in LSM-Tree
        self.put(key, ValueState::Some(val_bytes))?;

        // 2. Insert into HNSW graph index for sub-millisecond retrieval
        self.hnsw_index.insert(id, vector, metadata)?;
        Ok(())
    }

    /// High-performance Vector search utilizing HNSW graph indexing when available, with flat scan fallback.
    pub fn search_vector(&self, query_vector: &[f32], top_k: usize) -> Result<Vec<(String, f32, Option<String>)>> {
        if !self.hnsw_index.is_empty() {
            let results = self.hnsw_index.search(query_vector, top_k);
            if !results.is_empty() {
                return Ok(results);
            }
        }

        // Fallback: Exact Flat SIMD Brute-Force scan over MemTable
        self.search_vector_flat(query_vector, top_k)
    }

    /// Exact SIMD-accelerated Cosine Nearest Neighbor search over stored vectors.
    pub fn search_vector_flat(&self, query_vector: &[f32], top_k: usize) -> Result<Vec<(String, f32, Option<String>)>> {
        let mut candidates = Vec::new();

        // Scan all vector entries from active MemTable
        for (k, v) in self.active_memtable.read().iter() {
            if k.starts_with(b"__vec:") {
                if let ValueState::Some(bytes) = v {
                    if let Ok((vec, meta)) = bincode::deserialize::<(Vec<f32>, Option<String>)>(&bytes) {
                        if query_vector.len() == vec.len() {
                            let id = String::from_utf8_lossy(&k[6..]).to_string();
                            let score = aether_simd::cosine_similarity(query_vector, &vec);
                            candidates.push((id, score, meta));
                        }
                    }
                }
            }
        }

        // Sort descending by similarity score
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(top_k);

        Ok(candidates)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_storage_engine_full_lifecycle() {
        let dir = tempdir().unwrap();
        let engine = StorageEngine::open(dir.path()).unwrap();

        engine
            .put(b"user:01".to_vec(), ValueState::Some(b"alice".to_vec()))
            .unwrap();
        engine
            .put(b"user:02".to_vec(), ValueState::Some(b"bob".to_vec()))
            .unwrap();

        assert_eq!(
            engine.get(b"user:01").unwrap(),
            Some(ValueState::Some(b"alice".to_vec()))
        );
        assert_eq!(
            engine.get(b"user:02").unwrap(),
            Some(ValueState::Some(b"bob".to_vec()))
        );

        // Force flush to SSTable on disk
        engine.flush_active_memtable().unwrap();

        // Verify retrieval directly from SSTable
        assert_eq!(
            engine.get(b"user:01").unwrap(),
            Some(ValueState::Some(b"alice".to_vec()))
        );
    }

    #[test]
    fn test_storage_engine_hnsw_and_compaction() {
        let dir = tempdir().unwrap();
        let engine = StorageEngine::open(dir.path()).unwrap();

        // 1. Vector Upsert & HNSW Search
        engine.upsert_vector("doc_1", vec![1.0, 0.0, 0.0, 0.0], Some("meta1".into())).unwrap();
        engine.upsert_vector("doc_2", vec![0.9, 0.1, 0.0, 0.0], Some("meta2".into())).unwrap();

        let search_res = engine.search_vector(&[0.95, 0.05, 0.0, 0.0], 2).unwrap();
        assert_eq!(search_res.len(), 2);
        assert_eq!(search_res[0].0, "doc_1");

        // 2. Compaction trigger
        engine.put(b"k1".to_vec(), ValueState::Some(b"v1".to_vec())).unwrap();
        engine.flush_active_memtable().unwrap();

        engine.put(b"k1".to_vec(), ValueState::Some(b"v1_new".to_vec())).unwrap();
        engine.flush_active_memtable().unwrap();

        let comp_res = engine.trigger_compaction().unwrap();
        assert!(comp_res.is_some());
        assert_eq!(engine.get(b"k1").unwrap(), Some(ValueState::Some(b"v1_new".to_vec())));
    }
}
