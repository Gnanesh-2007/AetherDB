use std::collections::BTreeMap;
use parking_lot::RwLock;
use aether_core::error::{AetherError, Result};
use aether_core::types::RangeKey;

#[derive(Debug, Clone)]
pub struct RangeDescriptor {
    pub range_id: u64,
    pub key_range: RangeKey,
    pub leader_node_id: Option<u64>,
    pub peers: Vec<u64>,
}

pub struct RangeRouter {
    ranges: RwLock<BTreeMap<Vec<u8>, RangeDescriptor>>,
}

impl RangeRouter {
    pub fn new() -> Self {
        let mut map = BTreeMap::new();
        // Initial default root range covering the entire keyspace [b"", b""]
        let root_range = RangeDescriptor {
            range_id: 1,
            key_range: RangeKey::new(vec![], vec![]),
            leader_node_id: Some(1),
            peers: vec![1, 2, 3],
        };
        map.insert(vec![], root_range);

        Self {
            ranges: RwLock::new(map),
        }
    }

    /// Finds the range responsible for a given key in O(log R) time.
    pub fn route_key(&self, key: &[u8]) -> Result<RangeDescriptor> {
        let ranges = self.ranges.read();
        
        // Find the range whose start_key <= key
        let mut candidate = None;
        for (start_key, desc) in ranges.iter() {
            if start_key.as_slice() <= key {
                candidate = Some(desc.clone());
            } else {
                break;
            }
        }

        candidate.ok_or(AetherError::KeyNotFound)
    }

    /// Splits a range at `split_key` atomically into two child ranges.
    pub fn split_range(&self, old_range_id: u64, split_key: Vec<u8>, new_range_id: u64) -> Result<()> {
        let mut ranges = self.ranges.write();

        let target_start = ranges
            .iter()
            .find(|(_, desc)| desc.range_id == old_range_id)
            .map(|(k, _)| k.clone())
            .ok_or(AetherError::RangeSplitConflict(old_range_id))?;

        let old_desc = ranges.get(&target_start).unwrap().clone();

        // Update old range end_key
        let updated_old = RangeDescriptor {
            range_id: old_range_id,
            key_range: RangeKey::new(old_desc.key_range.start_key.clone(), split_key.clone()),
            leader_node_id: old_desc.leader_node_id,
            peers: old_desc.peers.clone(),
        };

        // Create new range [split_key, old_end)
        let new_desc = RangeDescriptor {
            range_id: new_range_id,
            key_range: RangeKey::new(split_key.clone(), old_desc.key_range.end_key),
            leader_node_id: old_desc.leader_node_id,
            peers: old_desc.peers,
        };

        ranges.insert(target_start, updated_old);
        ranges.insert(split_key, new_desc);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dynamic_range_routing_and_split() {
        let router = RangeRouter::new();

        let desc1 = router.route_key(b"user:alice").unwrap();
        assert_eq!(desc1.range_id, 1);

        // Split at "m"
        router.split_range(1, b"m".to_vec(), 2).unwrap();

        let desc_a = router.route_key(b"apple").unwrap();
        assert_eq!(desc_a.range_id, 1);

        let desc_z = router.route_key(b"zebra").unwrap();
        assert_eq!(desc_z.range_id, 2);
    }
}
