use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftLogEntry {
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct RaftLog {
    entries: Vec<RaftLogEntry>,
}

impl RaftLog {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn last_index(&self) -> u64 {
        self.entries.last().map_or(0, |e| e.index)
    }

    pub fn last_term(&self) -> u64 {
        self.entries.last().map_or(0, |e| e.term)
    }

    pub fn append(&mut self, term: u64, data: Vec<u8>) -> u64 {
        let index = self.last_index() + 1;
        self.entries.push(RaftLogEntry { term, index, data });
        index
    }

    pub fn get_entry(&self, index: u64) -> Option<&RaftLogEntry> {
        if index == 0 || index > self.last_index() {
            None
        } else {
            self.entries.get((index - 1) as usize)
        }
    }

    pub fn entries_from(&self, index: u64) -> Vec<RaftLogEntry> {
        if index == 0 {
            return self.entries.clone();
        }
        let start = (index - 1) as usize;
        if start >= self.entries.len() {
            Vec::new()
        } else {
            self.entries[start..].to_vec()
        }
    }

    pub fn truncate_from(&mut self, index: u64) {
        if index > 0 && (index - 1) < self.entries.len() as u64 {
            self.entries.truncate((index - 1) as usize);
        }
    }
}
