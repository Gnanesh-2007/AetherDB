use crate::error::{AetherError, Result};
use crate::hlc::HlcTimestamp;

/// Represents an MVCC physical key stored in the LSM-Tree.
///
/// Encoded format guarantees:
/// 1. Keys with the same user prefix are adjacent.
/// 2. Newer versions of the same user key sort BEFORE older versions (descending timestamp order).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MvccKey {
    pub user_key: Vec<u8>,
    pub timestamp: HlcTimestamp,
}

impl MvccKey {
    pub fn new(user_key: impl Into<Vec<u8>>, timestamp: HlcTimestamp) -> Self {
        Self {
            user_key: user_key.into(),
            timestamp,
        }
    }

    /// Latest possible version of a key (used for point lookups).
    pub fn latest(user_key: impl Into<Vec<u8>>) -> Self {
        Self {
            user_key: user_key.into(),
            timestamp: HlcTimestamp::MAX,
        }
    }

    /// Oldest possible version of a key.
    pub fn oldest(user_key: impl Into<Vec<u8>>) -> Self {
        Self {
            user_key: user_key.into(),
            timestamp: HlcTimestamp::MIN,
        }
    }

    /// Encodes the MVCC key into bytes for LSM-Tree storage.
    /// Uses inverted timestamps for descending version sorting.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.user_key.len() + 1 + 12);
        
        // Escape \x00 bytes in user key to prevent delimiter collision
        for &b in &self.user_key {
            if b == 0x00 {
                buf.push(0x00);
                buf.push(0xFF);
            } else {
                buf.push(b);
            }
        }
        
        // Delimiter
        buf.push(0x00);
        buf.push(0x01);

        // Inverted timestamp for descending order
        let inv_physical = !self.timestamp.physical;
        let inv_logical = !self.timestamp.logical;

        buf.extend_from_slice(&inv_physical.to_be_bytes());
        buf.extend_from_slice(&inv_logical.to_be_bytes());
        buf
    }

    /// Decodes raw bytes back into an MvccKey.
    pub fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.len() < 14 {
            return Err(AetherError::InvalidKeyFormat(
                "Encoded MVCC key too short".to_string(),
            ));
        }

        // The last 12 bytes are the inverted timestamp
        let ts_split_idx = encoded.len() - 12;
        let key_portion = &encoded[..ts_split_idx];
        let ts_portion = &encoded[ts_split_idx..];

        // Delimiter must be 0x00, 0x01 before timestamp
        if key_portion.len() < 2 || &key_portion[key_portion.len() - 2..] != [0x00, 0x01] {
            return Err(AetherError::InvalidKeyFormat(
                "Invalid MVCC key delimiter".to_string(),
            ));
        }

        let user_escaped = &key_portion[..key_portion.len() - 2];
        let mut user_key = Vec::with_capacity(user_escaped.len());
        
        let mut i = 0;
        while i < user_escaped.len() {
            if user_escaped[i] == 0x00 && i + 1 < user_escaped.len() && user_escaped[i + 1] == 0xFF {
                user_key.push(0x00);
                i += 2;
            } else {
                user_key.push(user_escaped[i]);
                i += 1;
            }
        }

        let inv_physical = u64::from_be_bytes(ts_portion[0..8].try_into().unwrap());
        let inv_logical = u32::from_be_bytes(ts_portion[8..12].try_into().unwrap());

        let physical = !inv_physical;
        let logical = !inv_logical;

        Ok(Self {
            user_key,
            timestamp: HlcTimestamp::new(physical, logical),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mvcc_key_roundtrip() {
        let key = MvccKey::new(b"account:balance:9920", HlcTimestamp::new(1700000000000, 5));
        let encoded = key.encode();
        let decoded = MvccKey::decode(&encoded).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn test_mvcc_key_with_zero_bytes() {
        let key = MvccKey::new(b"foo\x00bar\x00\x00baz", HlcTimestamp::new(1700000000000, 1));
        let encoded = key.encode();
        let decoded = MvccKey::decode(&encoded).unwrap();
        assert_eq!(key, decoded);
    }

    #[test]
    fn test_mvcc_key_descending_ordering() {
        let k_old = MvccKey::new(b"user_100", HlcTimestamp::new(1000, 0));
        let k_new = MvccKey::new(b"user_100", HlcTimestamp::new(2000, 0));

        let enc_old = k_old.encode();
        let enc_new = k_new.encode();

        // enc_new MUST sort BEFORE enc_old
        assert!(enc_new < enc_old);
    }
}
