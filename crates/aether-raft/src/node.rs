use crate::log::{RaftLog, RaftLogEntry};
use crate::state::{RaftRole, RaftState};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesArgs {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<RaftLogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

pub struct RaftNode {
    pub node_id: u64,
    pub peers: Vec<u64>,
    pub role: RwLock<RaftRole>,
    pub state: RwLock<RaftState>,
    pub log: RwLock<RaftLog>,
}

impl RaftNode {
    pub fn new(node_id: u64, peers: Vec<u64>) -> Self {
        Self {
            node_id,
            peers,
            role: RwLock::new(RaftRole::Follower),
            state: RwLock::new(RaftState::default()),
            log: RwLock::new(RaftLog::new()),
        }
    }

    /// Handles incoming RequestVote RPC from candidate nodes.
    pub fn handle_request_vote(&self, args: &RequestVoteArgs) -> RequestVoteReply {
        let mut state = self.state.write();
        let log = self.log.read();

        // 1. Term check
        if args.term > state.current_term {
            state.current_term = args.term;
            state.voted_for = None;
            *self.role.write() = RaftRole::Follower;
        }

        let mut vote_granted = false;
        let last_log_term = log.last_term();
        let last_log_index = log.last_index();

        let log_ok = args.last_log_term > last_log_term
            || (args.last_log_term == last_log_term && args.last_log_index >= last_log_index);

        if args.term == state.current_term
            && (state.voted_for.is_none() || state.voted_for == Some(args.candidate_id))
            && log_ok
        {
            vote_granted = true;
            state.voted_for = Some(args.candidate_id);
        }

        RequestVoteReply {
            term: state.current_term,
            vote_granted,
        }
    }

    /// Handles incoming AppendEntries RPC (heartbeats and log synchronization).
    pub fn handle_append_entries(&self, args: &AppendEntriesArgs) -> AppendEntriesReply {
        let mut state = self.state.write();
        let mut log = self.log.write();

        if args.term < state.current_term {
            return AppendEntriesReply {
                term: state.current_term,
                success: false,
                match_index: log.last_index(),
            };
        }

        if args.term > state.current_term || *self.role.read() != RaftRole::Follower {
            state.current_term = args.term;
            state.voted_for = None;
            *self.role.write() = RaftRole::Follower;
        }

        // Verify prevLogIndex and prevLogTerm match
        if args.prev_log_index > 0 {
            match log.get_entry(args.prev_log_index) {
                Some(entry) if entry.term == args.prev_log_term => {}
                _ => {
                    return AppendEntriesReply {
                        term: state.current_term,
                        success: false,
                        match_index: log.last_index(),
                    };
                }
            }
        }

        // Append new entries not already in the log
        log.truncate_from(args.prev_log_index + 1);
        for entry in &args.entries {
            log.append(entry.term, entry.data.clone());
        }

        // Update commit index
        if args.leader_commit > state.commit_index {
            state.commit_index = args.leader_commit.min(log.last_index());
        }

        AppendEntriesReply {
            term: state.current_term,
            success: true,
            match_index: log.last_index(),
        }
    }
}
