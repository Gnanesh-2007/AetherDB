use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::Mutex;
use aether_core::types::ValueState;

type BlockData = Arc<Vec<(Vec<u8>, ValueState)>>;

/// In-memory LRU Block Cache for 4KB SSTable pages.
pub struct BlockCache {
    capacity_blocks: usize,
    entries: Mutex<HashMap<(u64, u64), BlockData>>, // (sstable_id, block_offset) -> BlockData
    access_order: Mutex<Vec<(u64, u64)>>,
}

impl BlockCache {
    pub fn new(capacity_blocks: usize) -> Self {
        Self {
            capacity_blocks,
            entries: Mutex::new(HashMap::new()),
            access_order: Mutex::new(Vec::new()),
        }
    }

    pub fn get(&self, sstable_id: u64, block_offset: u64) -> Option<BlockData> {
        let key = (sstable_id, block_offset);
        let entries = self.entries.lock();
        if let Some(data) = entries.get(&key) {
            let mut order = self.access_order.lock();
            if let Some(pos) = order.iter().position(|&x| x == key) {
                order.remove(pos);
            }
            order.push(key);
            Some(data.clone())
        } else {
            None
        }
    }

    pub fn insert(&self, sstable_id: u64, block_offset: u64, block: BlockData) {
        let key = (sstable_id, block_offset);
        let mut entries = self.entries.lock();
        let mut order = self.access_order.lock();

        if entries.len() >= self.capacity_blocks && !entries.contains_key(&key) {
            if !order.is_empty() {
                let oldest = order.remove(0);
                entries.remove(&oldest);
            }
        }

        entries.insert(key, block);
        if let Some(pos) = order.iter().position(|&x| x == key) {
            order.remove(pos);
        }
        order.push(key);
    }

    pub fn clear(&self) {
        self.entries.lock().clear();
        self.access_order.lock().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_cache_lru_eviction() {
        let cache = BlockCache::new(2);
        let b1 = Arc::new(vec![(b"k1".to_vec(), ValueState::Some(b"v1".to_vec()))]);
        let b2 = Arc::new(vec![(b"k2".to_vec(), ValueState::Some(b"v2".to_vec()))]);
        let b3 = Arc::new(vec![(b"k3".to_vec(), ValueState::Some(b"v3".to_vec()))]);

        cache.insert(1, 0, b1.clone());
        cache.insert(1, 4096, b2.clone());
        assert!(cache.get(1, 0).is_some());

        // Insert 3rd block, should evict oldest (1, 4096) since (1, 0) was accessed
        cache.insert(1, 8192, b3.clone());
        assert!(cache.get(1, 0).is_some());
        assert!(cache.get(1, 4096).is_none());
        assert!(cache.get(1, 8192).is_some());
    }
}
