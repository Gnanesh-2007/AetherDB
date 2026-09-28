pub mod bloom;

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use serde::{Deserialize, Serialize};

use aether_core::error::{AetherError, Result};
use aether_core::types::ValueState;
use bloom::BloomFilter;

const SSTABLE_MAGIC: &[u8; 8] = b"AETHER01";
const BLOCK_SIZE_BYTES: usize = 4096;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMeta {
    pub last_key: Vec<u8>,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableFooter {
    pub index_offset: u64,
    pub index_len: u64,
    pub bloom_offset: u64,
    pub bloom_len: u64,
}

pub struct SSTableWriter {
    file: File,
    _path: PathBuf,
    block_metas: Vec<BlockMeta>,
    current_block: Vec<(Vec<u8>, ValueState)>,
    current_block_size: usize,
    keys: Vec<Vec<u8>>,
    written_bytes: u64,
}

impl SSTableWriter {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        Ok(Self {
            file,
            _path: path,
            block_metas: Vec::new(),
            current_block: Vec::new(),
            current_block_size: 0,
            keys: Vec::new(),
            written_bytes: 0,
        })
    }

    pub fn append(&mut self, key: Vec<u8>, value: ValueState) -> Result<()> {
        let entry_size = key.len() + 32;
        self.keys.push(key.clone());
        self.current_block.push((key, value));
        self.current_block_size += entry_size;

        if self.current_block_size >= BLOCK_SIZE_BYTES {
            self.flush_current_block()?;
        }

        Ok(())
    }

    fn flush_current_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }

        let last_key = self.current_block.last().unwrap().0.clone();
        let block_data = bincode::serialize(&self.current_block)
            .map_err(|e| AetherError::SerializationError(e.to_string()))?;

        let offset = self.written_bytes;
        let length = block_data.len() as u64;

        self.file
            .write_all(&block_data)
            .map_err(|e| AetherError::IoError(e.to_string()))?;
        self.written_bytes += length;

        self.block_metas.push(BlockMeta {
            last_key,
            offset,
            length,
        });

        self.current_block.clear();
        self.current_block_size = 0;
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.flush_current_block()?;

        // 1. Write Block Index
        let index_data = bincode::serialize(&self.block_metas)
            .map_err(|e| AetherError::SerializationError(e.to_string()))?;
        let index_offset = self.written_bytes;
        let index_len = index_data.len() as u64;
        self.file
            .write_all(&index_data)
            .map_err(|e| AetherError::IoError(e.to_string()))?;
        self.written_bytes += index_len;

        // 2. Write Bloom Filter
        let key_refs: Vec<&[u8]> = self.keys.iter().map(|k| k.as_slice()).collect();
        let bloom = BloomFilter::build(&key_refs, 10);
        let bloom_data = bincode::serialize(&bloom)
            .map_err(|e| AetherError::SerializationError(e.to_string()))?;
        let bloom_offset = self.written_bytes;
        let bloom_len = bloom_data.len() as u64;
        self.file
            .write_all(&bloom_data)
            .map_err(|e| AetherError::IoError(e.to_string()))?;
        self.written_bytes += bloom_len;

        // 3. Write Table Footer + Magic
        let footer = TableFooter {
            index_offset,
            index_len,
            bloom_offset,
            bloom_len,
        };
        let footer_data = bincode::serialize(&footer)
            .map_err(|e| AetherError::SerializationError(e.to_string()))?;
        let footer_len = footer_data.len() as u32;

        self.file
            .write_all(&footer_data)
            .map_err(|e| AetherError::IoError(e.to_string()))?;
        self.file
            .write_all(&footer_len.to_be_bytes())
            .map_err(|e| AetherError::IoError(e.to_string()))?;
        self.file
            .write_all(SSTABLE_MAGIC)
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        self.file
            .flush()
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        Ok(())
    }
}

pub struct SSTableReader {
    file: File,
    index: Vec<BlockMeta>,
    bloom: BloomFilter,
}

impl SSTableReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut file = File::open(path).map_err(|e| AetherError::IoError(e.to_string()))?;
        let file_len = file
            .metadata()
            .map_err(|e| AetherError::IoError(e.to_string()))?
            .len();

        if file_len < 16 {
            return Err(AetherError::Corruption("SSTable file too small".to_string()));
        }

        // Read Magic & Footer Size from the end of file
        file.seek(SeekFrom::End(-12))
            .map_err(|e| AetherError::IoError(e.to_string()))?;
        let mut footer_meta = [0u8; 12];
        file.read_exact(&mut footer_meta)
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        if &footer_meta[4..12] != SSTABLE_MAGIC {
            return Err(AetherError::Corruption(
                "Invalid SSTable magic header".to_string(),
            ));
        }

        let footer_len = u32::from_be_bytes(footer_meta[0..4].try_into().unwrap()) as i64;
        file.seek(SeekFrom::End(-(12 + footer_len)))
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        let mut footer_data = vec![0u8; footer_len as usize];
        file.read_exact(&mut footer_data)
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        let footer: TableFooter = bincode::deserialize(&footer_data)
            .map_err(|e| AetherError::SerializationError(e.to_string()))?;

        // Read Index
        file.seek(SeekFrom::Start(footer.index_offset))
            .map_err(|e| AetherError::IoError(e.to_string()))?;
        let mut index_data = vec![0u8; footer.index_len as usize];
        file.read_exact(&mut index_data)
            .map_err(|e| AetherError::IoError(e.to_string()))?;
        let index: Vec<BlockMeta> = bincode::deserialize(&index_data)
            .map_err(|e| AetherError::SerializationError(e.to_string()))?;

        // Read Bloom Filter
        file.seek(SeekFrom::Start(footer.bloom_offset))
            .map_err(|e| AetherError::IoError(e.to_string()))?;
        let mut bloom_data = vec![0u8; footer.bloom_len as usize];
        file.read_exact(&mut bloom_data)
            .map_err(|e| AetherError::IoError(e.to_string()))?;
        let bloom: BloomFilter = bincode::deserialize(&bloom_data)
            .map_err(|e| AetherError::SerializationError(e.to_string()))?;

        Ok(Self { file, index, bloom })
    }

    /// Point lookup for a key using Bloom filter + Binary Search on Sparse Block Index.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<ValueState>> {
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }

        // Binary search the block index to find the candidate block
        let block_idx = match self.index.binary_search_by(|bm| bm.last_key.as_slice().cmp(key)) {
            Ok(idx) => idx,
            Err(idx) => {
                if idx < self.index.len() {
                    idx
                } else {
                    return Ok(None);
                }
            }
        };

        let block_meta = &self.index[block_idx];
        self.file
            .seek(SeekFrom::Start(block_meta.offset))
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        let mut block_data = vec![0u8; block_meta.length as usize];
        self.file
            .read_exact(&mut block_data)
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        let entries: Vec<(Vec<u8>, ValueState)> = bincode::deserialize(&block_data)
            .map_err(|e| AetherError::SerializationError(e.to_string()))?;

        for (k, v) in entries {
            if k == key {
                return Ok(Some(v));
            }
        }

        Ok(None)
    }

    /// Reads all key-value entries sequentially (used during compaction).
    pub fn scan_all(&mut self) -> Result<Vec<(Vec<u8>, ValueState)>> {
        let mut all_entries = Vec::new();
        for block_meta in &self.index {
            self.file
                .seek(SeekFrom::Start(block_meta.offset))
                .map_err(|e| AetherError::IoError(e.to_string()))?;

            let mut block_data = vec![0u8; block_meta.length as usize];
            self.file
                .read_exact(&mut block_data)
                .map_err(|e| AetherError::IoError(e.to_string()))?;

            let entries: Vec<(Vec<u8>, ValueState)> = bincode::deserialize(&block_data)
                .map_err(|e| AetherError::SerializationError(e.to_string()))?;
            all_entries.extend(entries);
        }
        Ok(all_entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_sstable_write_and_read() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        {
            let mut writer = SSTableWriter::create(path).unwrap();
            writer
                .append(b"k1".to_vec(), ValueState::Some(b"val1".to_vec()))
                .unwrap();
            writer
                .append(b"k2".to_vec(), ValueState::Some(b"val2".to_vec()))
                .unwrap();
            writer
                .append(b"k3".to_vec(), ValueState::Tombstone)
                .unwrap();
            writer.finish().unwrap();
        }

        let mut reader = SSTableReader::open(path).unwrap();
        assert_eq!(
            reader.get(b"k1").unwrap(),
            Some(ValueState::Some(b"val1".to_vec()))
        );
        assert_eq!(
            reader.get(b"k2").unwrap(),
            Some(ValueState::Some(b"val2".to_vec()))
        );
        assert_eq!(reader.get(b"k3").unwrap(), Some(ValueState::Tombstone));
        assert_eq!(reader.get(b"nonexistent").unwrap(), None);
    }
}
