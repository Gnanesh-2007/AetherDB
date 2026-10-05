use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::cmp::{max, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{AetherError, Result};

/// Hybrid Logical Clock timestamp.
/// Orders all distributed events across nodes deterministically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HlcTimestamp {
    /// Physical time in milliseconds since UNIX epoch.
    pub physical: u64,
    /// Monotonic logical counter for sub-millisecond causal events.
    pub logical: u32,
}

impl HlcTimestamp {
    pub const MIN: HlcTimestamp = HlcTimestamp {
        physical: 0,
        logical: 0,
    };
    pub const MAX: HlcTimestamp = HlcTimestamp {
        physical: u64::MAX,
        logical: u32::MAX,
    };

    pub fn new(physical: u64, logical: u32) -> Self {
        Self { physical, logical }
    }

    /// Converts timestamp into a 96-bit compact binary representation for key encoding.
    pub fn to_bytes(&self) -> [u8; 12] {
        let mut buf = [0u8; 12];
        buf[0..8].copy_from_slice(&self.physical.to_be_bytes());
        buf[8..12].copy_from_slice(&self.logical.to_be_bytes());
        buf
    }

    /// Decodes a 96-bit compact binary representation into an HlcTimestamp.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 12 {
            return Err(AetherError::InvalidKeyFormat(
                "HlcTimestamp requires at least 12 bytes".to_string(),
            ));
        }
        let physical = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let logical = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        Ok(Self { physical, logical })
    }
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.physical.cmp(&other.physical) {
            Ordering::Equal => self.logical.cmp(&other.logical),
            ord => ord,
        }
    }
}

/// Thread-safe Hybrid Logical Clock generator.
pub struct HybridLogicalClock {
    max_drift_ms: u64,
    state: Mutex<HlcTimestamp>,
}

impl HybridLogicalClock {
    pub fn new(max_drift_ms: u64) -> Self {
        let physical_now = Self::get_physical_time();
        Self {
            max_drift_ms,
            state: Mutex::new(HlcTimestamp::new(physical_now, 0)),
        }
    }

    /// Read the current physical system time in milliseconds.
    fn get_physical_time() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time before UNIX epoch")
            .as_millis() as u64
    }

    /// Generates a strictly increasing timestamp for a local event.
    pub fn now(&self) -> HlcTimestamp {
        let physical_now = Self::get_physical_time();
        let mut state = self.state.lock();

        if physical_now > state.physical {
            state.physical = physical_now;
            state.logical = 0;
        } else {
            state.logical += 1;
        }

        *state
    }

    /// Updates the local HLC upon receiving a timestamp from a remote node.
    /// Ensures causality is preserved across distributed message exchanges.
    pub fn update(&self, remote: HlcTimestamp) -> Result<HlcTimestamp> {
        let physical_now = Self::get_physical_time();
        let mut state = self.state.lock();

        // Check for catastrophic clock drift
        if remote.physical > physical_now && remote.physical - physical_now > self.max_drift_ms {
            return Err(AetherError::ClockDriftExceeded(self.max_drift_ms));
        }

        let new_physical = max(max(state.physical, physical_now), remote.physical);

        if new_physical == state.physical && new_physical == remote.physical {
            state.logical = max(state.logical, remote.logical) + 1;
        } else if new_physical == state.physical {
            state.logical += 1;
        } else if new_physical == remote.physical {
            state.logical = remote.logical + 1;
        } else {
            state.logical = 0;
        }

        state.physical = new_physical;
        Ok(*state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hlc_monotonicity() {
        let hlc = HybridLogicalClock::new(500);
        let t1 = hlc.now();
        let t2 = hlc.now();
        let t3 = hlc.now();

        assert!(t2 > t1);
        assert!(t3 > t2);
    }

    #[test]
    fn test_hlc_causality_update() {
        let hlc1 = HybridLogicalClock::new(5000);
        let hlc2 = HybridLogicalClock::new(5000);

        let t1 = hlc1.now();
        let t2 = hlc2.update(t1).unwrap();

        assert!(t2 > t1);
    }

    #[test]
    fn test_timestamp_serialization() {
        let ts = HlcTimestamp::new(1700000000000, 42);
        let bytes = ts.to_bytes();
        let decoded = HlcTimestamp::from_bytes(&bytes).unwrap();
        assert_eq!(ts, decoded);
    }
}
