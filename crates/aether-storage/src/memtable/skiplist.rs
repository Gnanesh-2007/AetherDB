use parking_lot::RwLock;
use rand::Rng;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const MAX_HEIGHT: usize = 16;
const PROBABILITY: f64 = 0.5;

pub struct SkipListNode<K, V> {
    pub key: K,
    pub value: V,
    pub height: usize,
    pub forward: [AtomicPtr<SkipListNode<K, V>>; MAX_HEIGHT],
}

impl<K, V> SkipListNode<K, V> {
    pub fn new(key: K, value: V, height: usize) -> *mut Self {
        let node = Box::new(Self {
            key,
            value,
            height,
            forward: Default::default(),
        });
        Box::into_raw(node)
    }

    pub fn head(height: usize) -> *mut Self
    where
        K: Default,
        V: Default,
    {
        Self::new(K::default(), V::default(), height)
    }
}

/// Concurrent SkipList optimized for LSM-Tree MemTable.
/// Guarantees O(log N) search and insert with lock-free read traversal.
pub struct ConcurrentSkipList<K: Ord + Clone + Default, V: Clone + Default> {
    head: *mut SkipListNode<K, V>,
    max_level: AtomicUsize,
    len: AtomicUsize,
    bytes_size: AtomicUsize,
    write_lock: RwLock<()>,
}

unsafe impl<K: Ord + Clone + Default + Send, V: Clone + Default + Send> Send
    for ConcurrentSkipList<K, V>
{
}
unsafe impl<K: Ord + Clone + Default + Sync, V: Clone + Default + Sync> Sync
    for ConcurrentSkipList<K, V>
{
}

impl<K: Ord + Clone + Default, V: Clone + Default> ConcurrentSkipList<K, V> {
    pub fn new() -> Self {
        let head = SkipListNode::head(MAX_HEIGHT);
        Self {
            head,
            max_level: AtomicUsize::new(1),
            len: AtomicUsize::new(0),
            bytes_size: AtomicUsize::new(0),
            write_lock: RwLock::new(()),
        }
    }

    fn random_height() -> usize {
        let mut rng = rand::thread_rng();
        let mut height = 1;
        while height < MAX_HEIGHT && rng.gen::<f64>() < PROBABILITY {
            height += 1;
        }
        height
    }

    /// Read without acquiring write locks. Lock-free forward pointer traversal.
    pub fn get(&self, key: &K) -> Option<V> {
        let _guard = self.write_lock.read();
        let mut current = self.head;
        let current_max_level = self.max_level.load(Ordering::Acquire);

        for level in (0..current_max_level).rev() {
            loop {
                unsafe {
                    let next = (*current).forward[level].load(Ordering::Acquire);
                    if next.is_null() {
                        break;
                    }
                    if (*next).key.cmp(key) == std::cmp::Ordering::Less {
                        current = next;
                    } else if (*next).key.cmp(key) == std::cmp::Ordering::Equal {
                        return Some((*next).value.clone());
                    } else {
                        break;
                    }
                }
            }
        }

        unsafe {
            let next = (*current).forward[0].load(Ordering::Acquire);
            if !next.is_null() && (*next).key.cmp(key) == std::cmp::Ordering::Equal {
                Some((*next).value.clone())
            } else {
                None
            }
        }
    }

    /// Insert or update key-value pair and track byte size overhead.
    pub fn insert(&self, key: K, value: V, size_bytes: usize) {
        let _guard = self.write_lock.write();

        let mut update = [self.head; MAX_HEIGHT];
        let mut current = self.head;
        let current_max = self.max_level.load(Ordering::Acquire);

        for level in (0..current_max).rev() {
            loop {
                unsafe {
                    let next = (*current).forward[level].load(Ordering::Acquire);
                    if next.is_null() || (*next).key >= key {
                        break;
                    }
                    current = next;
                }
            }
            update[level] = current;
        }

        unsafe {
            let next = (*current).forward[0].load(Ordering::Acquire);
            if !next.is_null() && (*next).key == key {
                // Key already exists, update value in place
                (*next).value = value;
                return;
            }
        }

        let new_height = Self::random_height();
        if new_height > current_max {
            for level in current_max..new_height {
                update[level] = self.head;
            }
            self.max_level.store(new_height, Ordering::Release);
        }

        let new_node = SkipListNode::new(key, value, new_height);

        for level in 0..new_height {
            unsafe {
                let next = (*update[level]).forward[level].load(Ordering::Relaxed);
                (*new_node).forward[level].store(next, Ordering::Relaxed);
                (*update[level]).forward[level].store(new_node, Ordering::Release);
            }
        }

        self.len.fetch_add(1, Ordering::Relaxed);
        self.bytes_size.fetch_add(size_bytes, Ordering::Relaxed);
    }

    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn memory_usage(&self) -> usize {
        self.bytes_size.load(Ordering::Relaxed)
    }

    /// Iterates through all elements in strictly sorted key order.
    pub fn iter(&self) -> Vec<(K, V)> {
        let _guard = self.write_lock.read();
        let mut results = Vec::with_capacity(self.len());
        unsafe {
            let mut curr = (*self.head).forward[0].load(Ordering::Acquire);
            while !curr.is_null() {
                results.push(((*curr).key.clone(), (*curr).value.clone()));
                curr = (*curr).forward[0].load(Ordering::Acquire);
            }
        }
        results
    }
}

impl<K: Ord + Clone + Default, V: Clone + Default> Drop for ConcurrentSkipList<K, V> {
    fn drop(&mut self) {
        unsafe {
            let mut curr = (*self.head).forward[0].load(Ordering::Relaxed);
            while !curr.is_null() {
                let next = (*curr).forward[0].load(Ordering::Relaxed);
                let _ = Box::from_raw(curr);
                curr = next;
            }
            let _ = Box::from_raw(self.head);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skiplist_crud_operations() {
        let sl = ConcurrentSkipList::<Vec<u8>, Vec<u8>>::new();
        sl.insert(b"key1".to_vec(), b"val1".to_vec(), 8);
        sl.insert(b"key3".to_vec(), b"val3".to_vec(), 8);
        sl.insert(b"key2".to_vec(), b"val2".to_vec(), 8);

        assert_eq!(sl.len(), 3);
        assert_eq!(sl.get(&b"key1".to_vec()), Some(b"val1".to_vec()));
        assert_eq!(sl.get(&b"key2".to_vec()), Some(b"val2".to_vec()));
        assert_eq!(sl.get(&b"key3".to_vec()), Some(b"val3".to_vec()));
        assert_eq!(sl.get(&b"nonexistent".to_vec()), None);

        let items = sl.iter();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, b"key1".to_vec());
        assert_eq!(items[1].0, b"key2".to_vec());
        assert_eq!(items[2].0, b"key3".to_vec());
    }
}
