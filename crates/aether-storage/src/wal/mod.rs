use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use crc32fast::Hasher;
use aether_core::error::{AetherError, Result};
use aether_core::types::ValueState;

pub const WAL_RECORD_PUT: u8 = 1;
pub const WAL_RECORD_DELETE: u8 = 2;
pub const WAL_RECORD_INTENT: u8 = 3;

pub struct WriteAheadLog {
    _path: PathBuf,
    writer: BufWriter<File>,
}

impl WriteAheadLog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        Ok(Self {
            _path: path,
            writer: BufWriter::new(file),
        })
    }

    /// Appends a raw key and ValueState to the WAL with CRC32 integrity verification.
    pub fn append(&mut self, key: &[u8], value: &ValueState) -> Result<()> {
        let payload = bincode::serialize(&(key, value))
            .map_err(|e| AetherError::SerializationError(e.to_string()))?;

        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let checksum = hasher.finalize();

        // Write header: Checksum (4B) + Length (4B)
        self.writer
            .write_all(&checksum.to_be_bytes())
            .map_err(|e| AetherError::IoError(e.to_string()))?;
        self.writer
            .write_all(&(payload.len() as u32).to_be_bytes())
            .map_err(|e| AetherError::IoError(e.to_string()))?;
        // Write body
        self.writer
            .write_all(&payload)
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        self.writer
            .flush()
            .map_err(|e| AetherError::IoError(e.to_string()))?;

        Ok(())
    }

    /// Recovers all valid transactions from the WAL file upon system restart.
    pub fn recover(path: impl AsRef<Path>) -> Result<Vec<(Vec<u8>, ValueState)>> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(path).map_err(|e| AetherError::IoError(e.to_string()))?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut header = [0u8; 8];
            match reader.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(AetherError::IoError(e.to_string())),
            }

            let expected_checksum = u32::from_be_bytes(header[0..4].try_into().unwrap());
            let payload_len = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;

            let mut payload = vec![0u8; payload_len];
            reader
                .read_exact(&mut payload)
                .map_err(|e| AetherError::IoError(e.to_string()))?;

            let mut hasher = Hasher::new();
            hasher.update(&payload);
            let found_checksum = hasher.finalize();

            if expected_checksum != found_checksum {
                return Err(AetherError::ChecksumMismatch {
                    expected: expected_checksum,
                    found: found_checksum,
                });
            }

            let (key, value): (Vec<u8>, ValueState) = bincode::deserialize(&payload)
                .map_err(|e| AetherError::SerializationError(e.to_string()))?;

            entries.push((key, value));
        }

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_wal_write_and_recovery() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path();

        {
            let mut wal = WriteAheadLog::open(path).unwrap();
            wal.append(b"key1", &ValueState::Some(b"val1".to_vec()))
                .unwrap();
            wal.append(b"key2", &ValueState::Tombstone).unwrap();
        }

        let recovered = WriteAheadLog::recover(path).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].0, b"key1");
        assert_eq!(recovered[0].1, ValueState::Some(b"val1".to_vec()));
        assert_eq!(recovered[1].0, b"key2");
        assert_eq!(recovered[1].1, ValueState::Tombstone);
    }
}
