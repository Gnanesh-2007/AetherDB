use crate::router::RangeRouter;
use aether_core::error::{AetherError, Result};
use aether_raft::RaftNode;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub struct MultiRaftManager {
    pub node_id: u64,
    pub router: Arc<RangeRouter>,
    pub raft_groups: RwLock<HashMap<u64, Arc<RaftNode>>>,
}

impl MultiRaftManager {
    pub fn new(node_id: u64, router: Arc<RangeRouter>) -> Self {
        let mut groups = HashMap::new();
        let default_node = Arc::new(RaftNode::new(node_id, vec![1, 2, 3]));
        groups.insert(1, default_node);

        Self {
            node_id,
            router,
            raft_groups: RwLock::new(groups),
        }
    }

    pub fn get_raft_group(&self, range_id: u64) -> Result<Arc<RaftNode>> {
        self.raft_groups
            .read()
            .get(&range_id)
            .cloned()
            .ok_or_else(|| {
                AetherError::RaftError(format!("Range {} not hosted on this node", range_id))
            })
    }
}
