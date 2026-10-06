# Deep Dive: LSM Storage Subsystem

AetherDB’s primary storage subsystem is a Log-Structured Merge-tree (LSM-tree) engineered in Rust for high write throughput, crash durability, and sub-millisecond point lookups.

---

## 1. Subsystem Architecture

```text
               Write Request (Key, Value)
                           │
             ┌─────────────┴─────────────┐
             ▼                           ▼
 ┌───────────────────────┐   ┌───────────────────────┐
 │ Append-Only WAL       │   │ Concurrent SkipList   │
 │ (CRC32 Checksummed)   │   │ MemTable (In-Memory)  │
 └───────────────────────┘   └───────────┬───────────┘
                                         │
                             (When MemTable Full)
                                         │
                                         ▼
                             ┌───────────────────────┐
                             │ Immutable SSTable     │
                             │ (Level 0 on Disk)     │
                             │ ┌───────────────────┐ │
                             │ │ Data Blocks (4KB) │ │
                             │ ├───────────────────┤ │
                             │ │ Index Block       │ │
                             │ ├───────────────────┤ │
                             │ │ Bloom Filter      │ │
                             │ └───────────────────┘ │
                             └───────────┬───────────┘
                                         │
                              (Background Compaction)
                                         ▼
                             ┌───────────────────────┐
                             │ Leveled SSTables (L1) │
                             └───────────────────────┘
```

---

## 2. Write Path: WAL & SkipList MemTable

Every write operation executes through a two-phase in-memory commit:

1. **Write-Ahead Log (WAL):**
   - Sequential, append-only disk write.
   - Each entry contains a magic header, timestamp, operation type (Put/Delete/Incr), key length, value length, and a **CRC32 checksum** verifying data integrity.
   - Guarantees crash recovery even under sudden power loss or process kill.

2. **Concurrent SkipList MemTable:**
   - Lock-free / fine-grained concurrency SkipList maintaining keys in lexicographical descending MVCC order.
   - Provides $O(\log N)$ inserts, updates, and range scans.
   - When the MemTable exceeds the configured threshold (e.g. 64 MB), it is frozen into an immutable MemTable and scheduled for disk flush.

---

## 3. Read Path: Bloom Filters & Block Cache

Reads follow a tiered multi-level lookup hierarchy to minimize disk I/O:

1. **Active MemTable:** Checked first. If found, returns the latest value.
2. **Immutable MemTables:** Checked if present during active flush.
3. **Block Cache (LRU):** Frequently accessed 4KB data blocks are cached in memory.
4. **SSTable Bloom Filters:** Each SSTable carries a **10-bit-per-key Bloom filter** ($<1\%$ false positive rate). If the Bloom filter returns `false`, disk reads for that SSTable are bypassed entirely.
5. **SSTable Index Block:** Binary search on two-level index blocks to locate the exact 4KB data block containing the key.

---

## 4. SSTable Format on Disk

Each SSTable file consists of structured contiguous blocks:

```text
┌─────────────────────────────────────────────────────────────┐
│ Data Block 0 (Key/Value pairs, Snappy/Raw encoded)          │
├─────────────────────────────────────────────────────────────┤
│ Data Block 1 ...                                            │
├─────────────────────────────────────────────────────────────┤
│ Data Block N                                                │
├─────────────────────────────────────────────────────────────┤
│ Index Block (Maps key ranges to Data Block byte offsets)   │
├─────────────────────────────────────────────────────────────┤
│ Filter Block (Serialized Block Bloom Filter bits)           │
├─────────────────────────────────────────────────────────────┤
│ Footer (Index Offset, Filter Offset, Magic Number, CRC32)   │
└─────────────────────────────────────────────────────────────┘
```

---

## 5. Background Compaction

As SSTables accumulate in Level 0, overlapping key ranges can degrade read performance. AetherDB's background compaction worker:
- Merges overlapping Level 0 SSTables into sorted Level 1 SSTables.
- Purges overwritten MVCC versions older than the active snapshot horizon.
- Reclaims disk space by physically discarding tombstone records.
