use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use tracing::info;

use aether_core::error::Result;
use aether_core::types::ValueState;
use crate::sstable::{SSTableReader, SSTableWriter};

pub struct Compactor;

impl Compactor {
    /// Merges multiple SSTables into a single compacted SSTable at `output_path`.
    /// Deduplicates duplicate keys (keeping the newest committed version) and drops tombstones.
    pub fn compact(
        sstable_paths: &[PathBuf],
        output_path: impl AsRef<Path>,
    ) -> Result<Option<PathBuf>> {
        if sstable_paths.is_empty() {
            return Ok(None);
        }

        let output_path = output_path.as_ref().to_path_buf();
        let mut merged_map: BTreeMap<Vec<u8>, ValueState> = BTreeMap::new();

        // Scan all inputs in chronological order so newer versions overwrite older ones
        for path in sstable_paths {
            if !path.exists() {
                continue;
            }
            let mut reader = SSTableReader::open(path)?;
            for (key, val) in reader.scan_all()? {
                merged_map.insert(key, val);
            }
        }

        // Purge tombstone markers during major compaction
        let live_entries: Vec<(Vec<u8>, ValueState)> = merged_map
            .into_iter()
            .filter(|(_, val)| !matches!(val, ValueState::Tombstone))
            .collect();

        if live_entries.is_empty() {
            // All entries were tombstones, simply remove old files
            for path in sstable_paths {
                let _ = fs::remove_file(path);
            }
            return Ok(None);
        }

        // Write live entries into compacted SSTable
        let mut writer = SSTableWriter::create(&output_path)?;
        for (key, val) in live_entries {
            writer.append(key, val)?;
        }
        writer.finish()?;

        // Clean up compacted input SSTables
        for path in sstable_paths {
            if path != &output_path {
                let _ = fs::remove_file(path);
            }
        }

        info!("🧹 Compaction completed: merged {} SSTables into {:?}", sstable_paths.len(), output_path);
        Ok(Some(output_path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_sstable_compaction_and_tombstone_purging() {
        let dir = tempdir().unwrap();
        let sst1_path = dir.path().join("00001.sst");
        let sst2_path = dir.path().join("00002.sst");
        let compacted_path = dir.path().join("compacted.sst");

        // SSTable 1: k1="v1", k2="v2", k3="v3"
        {
            let mut w1 = SSTableWriter::create(&sst1_path).unwrap();
            w1.append(b"k1".to_vec(), ValueState::Some(b"v1".to_vec())).unwrap();
            w1.append(b"k2".to_vec(), ValueState::Some(b"v2".to_vec())).unwrap();
            w1.append(b"k3".to_vec(), ValueState::Some(b"v3".to_vec())).unwrap();
            w1.finish().unwrap();
        }

        // SSTable 2: k1="v1_updated", k2=Tombstone (deleted)
        {
            let mut w2 = SSTableWriter::create(&sst2_path).unwrap();
            w2.append(b"k1".to_vec(), ValueState::Some(b"v1_updated".to_vec())).unwrap();
            w2.append(b"k2".to_vec(), ValueState::Tombstone).unwrap();
            w2.finish().unwrap();
        }

        // Compact SSTable 1 + SSTable 2
        let res = Compactor::compact(&[sst1_path.clone(), sst2_path.clone()], &compacted_path).unwrap();
        assert!(res.is_some());

        // Verify compacted SSTable contents
        let mut reader = SSTableReader::open(&compacted_path).unwrap();
        
        // k1 should have updated value
        assert_eq!(
            reader.get(b"k1").unwrap(),
            Some(ValueState::Some(b"v1_updated".to_vec()))
        );

        // k2 was deleted with tombstone, so should be purged completely
        assert_eq!(reader.get(b"k2").unwrap(), None);

        // k3 is retained
        assert_eq!(
            reader.get(b"k3").unwrap(),
            Some(ValueState::Some(b"v3".to_vec()))
        );

        // Old SSTable files must be deleted
        assert!(!sst1_path.exists());
        assert!(!sst2_path.exists());
    }
}
