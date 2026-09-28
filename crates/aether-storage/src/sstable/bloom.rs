use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

/// Probabilistic Bloom Filter to prevent unnecessary disk block reads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    bits: Vec<u8>,
    k_hashes: usize,
}

impl BloomFilter {
    /// Builds a Bloom filter from a slice of keys with ~1% false positive rate (10 bits/key).
    pub fn build(keys: &[&[u8]], bits_per_key: usize) -> Self {
        let bits_per_key = bits_per_key.max(1);
        let num_bits = (keys.len() * bits_per_key).max(64);
        let num_bytes = (num_bits + 7) / 8;
        let mut bits = vec![0u8; num_bytes];

        // Optimal k = (m/n) * ln(2) ~= bits_per_key * 0.69
        let k_hashes = ((bits_per_key as f64 * 0.693).round() as usize).clamp(1, 30);

        for &key in keys {
            let hash1 = Self::hash1(key);
            let hash2 = Self::hash2(key);

            for i in 0..k_hashes {
                let combined_hash = hash1.wrapping_add((i as u32).wrapping_mul(hash2));
                let bit_pos = (combined_hash as usize) % (num_bytes * 8);
                bits[bit_pos / 8] |= 1 << (bit_pos % 8);
            }
        }

        Self { bits, k_hashes }
    }

    /// Checks if a key might be in the set.
    /// False positives possible; false negatives are NEVER possible.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.bits.is_empty() {
            return true;
        }

        let total_bits = self.bits.len() * 8;
        let hash1 = Self::hash1(key);
        let hash2 = Self::hash2(key);

        for i in 0..self.k_hashes {
            let combined_hash = hash1.wrapping_add((i as u32).wrapping_mul(hash2));
            let bit_pos = (combined_hash as usize) % total_bits;
            if (self.bits[bit_pos / 8] & (1 << (bit_pos % 8))) == 0 {
                return false;
            }
        }

        true
    }

    fn hash1(data: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }

    fn hash2(data: &[u8]) -> u32 {
        let mut hasher = Hasher::new_with_initial(0x9747b28c);
        hasher.update(data);
        hasher.finalize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter() {
        let keys = vec![b"apple".as_slice(), b"banana".as_slice(), b"cherry".as_slice()];
        let filter = BloomFilter::build(&keys, 10);

        assert!(filter.may_contain(b"apple"));
        assert!(filter.may_contain(b"banana"));
        assert!(filter.may_contain(b"cherry"));
        assert!(!filter.may_contain(b"watermelon"));
    }
}
