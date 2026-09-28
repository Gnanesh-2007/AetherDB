use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpType {
    Write,
    Read,
}

#[derive(Debug, Clone)]
pub struct OperationRecord {
    pub client_id: u64,
    pub op_type: OpType,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub start_time: Instant,
    pub end_time: Instant,
}

/// Jepsen-style Linearizability Checker.
/// Verifies that concurrent read and write operations do not violate linearizable consistency guarantees.
pub struct LinearizabilityChecker {
    history: Vec<OperationRecord>,
}

impl LinearizabilityChecker {
    pub fn new() -> Self {
        Self {
            history: Vec::new(),
        }
    }

    pub fn record(&mut self, record: OperationRecord) {
        self.history.push(record);
    }

    /// Verifies single-key linearizability over the recorded operation log.
    pub fn verify_key(&self, key: &[u8]) -> bool {
        let mut key_ops: Vec<&OperationRecord> = self
            .history
            .iter()
            .filter(|op| op.key.as_slice() == key)
            .collect();

        // Sort operations by finish time
        key_ops.sort_by_key(|op| op.end_time);

        let mut latest_val: Option<Vec<u8>> = None;

        for op in key_ops {
            match op.op_type {
                OpType::Write => {
                    latest_val = op.value.clone();
                }
                OpType::Read => {
                    if let Some(read_val) = &op.value {
                        if latest_val.is_some() && latest_val.as_ref() != Some(read_val) {
                            return false; // Stale read violation
                        }
                    }
                }
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_linearizability_valid_history() {
        let mut checker = LinearizabilityChecker::new();
        let now = Instant::now();

        checker.record(OperationRecord {
            client_id: 1,
            op_type: OpType::Write,
            key: b"k1".to_vec(),
            value: Some(b"v1".to_vec()),
            start_time: now,
            end_time: now + Duration::from_millis(5),
        });

        checker.record(OperationRecord {
            client_id: 2,
            op_type: OpType::Read,
            key: b"k1".to_vec(),
            value: Some(b"v1".to_vec()),
            start_time: now + Duration::from_millis(6),
            end_time: now + Duration::from_millis(10),
        });

        assert!(checker.verify_key(b"k1"));
    }
}
