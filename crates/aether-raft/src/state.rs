use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftState {
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub commit_index: u64,
    pub last_applied: u64,
}

impl Default for RaftState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            commit_index: 0,
            last_applied: 0,
        }
    }
}
