pub mod skiplist;

use aether_core::types::ValueState;
use skiplist::ConcurrentSkipList;

pub struct MemTable {
    pub list: ConcurrentSkipList<Vec<u8>, ValueState>,
    pub max_size_bytes: usize,
}

impl MemTable {
    pub fn new(max_size_bytes: usize) -> Self {
        Self {
            list: ConcurrentSkipList::new(),
            max_size_bytes,
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<ValueState> {
        self.list.get(&key.to_vec())
    }

    pub fn put(&self, key: Vec<u8>, value: ValueState) {
        let size_bytes = key.len() + match &value {
            ValueState::Some(v) => v.len(),
            ValueState::Tombstone => 1,
            ValueState::Intent { value, .. } => value.as_ref().map_or(0, |v| v.len()) + 32,
        };
        self.list.insert(key, value, size_bytes);
    }

    pub fn is_full(&self) -> bool {
        self.list.memory_usage() >= self.max_size_bytes
    }

    pub fn memory_usage(&self) -> usize {
        self.list.memory_usage()
    }

    pub fn iter(&self) -> Vec<(Vec<u8>, ValueState)> {
        self.list.iter()
    }
}
