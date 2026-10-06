# Deep Dive: Multi-Raft Consensus & Distributed MVCC

AetherDB is designed to scale horizontally across multi-node clusters while providing ACID guarantees for autonomous agent operations.

---

## 1. Multi-Raft Consensus Architecture

Rather than maintaining a single monolithic Raft log (which creates a CPU/disk I/O bottleneck), AetherDB employs **Multi-Raft key-range sharding** (inspired by TiKV and CockroachDB):

```text
Key Space: [ 0x00 ───────────────────────────────────────────────► 0xFF ]
           ├──────── Range 1 ────────┤├──────── Range 2 ────────┤
           │ [Group 1, Leader: N1]   ││ [Group 2, Leader: N2]   │
           │ Followers: N2, N3       ││ Followers: N1, N3       │
```

- **Dynamic Range Partitioning:** The key space is divided into contiguous ranges (shards).
- **Independent Raft Groups:** Each range is an independent Raft consensus group with its own leader, followers, and replicated state machine.
- **Dynamic Range Splits:** When a range exceeds size thresholds, it splits automatically into two child ranges without global cluster disruption.

---

## 2. Monotonic Hybrid Logical Clocks (HLC)

To ensure causally consistent timestamps across distributed nodes without requiring synchronized atomic/GPS clocks (like Google Spanner TrueTime), AetherDB implements **Hybrid Logical Clocks (HLC)** (`aether-core`):

$$\text{HLC} = \langle \text{Physical Time } (l), \text{ Logical Counter } (c) \rangle$$

### Monotonicity Invariants
1. **Local Writes:** Advancing the clock always yields a timestamp strictly greater than any previously assigned local timestamp.
2. **Message Propagation:** When receiving messages from peer nodes, the local HLC updates to:
   $$l' = \max(l, \text{msg}.l, \text{physical\_now()})$$
   If physical times match, logical counter $c$ increments monotonically.

---

## 3. Distributed MVCC & Two-Phase Commit (2PC)

AetherDB supports cross-range transactions via Multi-Version Concurrency Control (MVCC) combined with a distributed Two-Phase Commit (2PC) coordinator (`aether-txn`):

```text
Client / Agent                     2PC Coordinator                       Participant Nodes
      │                                  │                                       │
      ├──── Begin Txn (Read HLC) ───────►│                                       │
      │                                  │                                       │
      ├──── Execute Mutations ──────────►│                                       │
      │                                  │                                       │
      ├──── Commit Request ─────────────►│                                       │
      │                                  ├────── Phase 1: Prepare (Lock Keys) ──►│
      │                                  │◄───── Prepared (OK / Conflict) ───────┤
      │                                  │                                       │
      │                                  ├────── Phase 2: Commit (Apply WAL) ───►│
      │                                  │◄───── Committed ──────────────────────┤
      │◄─── Txn Success (Commit HLC) ────┤                                       │
```

- **Snapshot Isolation (SI):** Readers execute lock-free point-in-time reads against historical MVCC versions based on their read timestamp.
- **Write Intent Locks:** Pending mutations write temporary intent records to the MemTable. Conflicting transactions detect intent locks and back off or abort.
- **Atomic 2PC Commit:** Once all participants vote `Prepared`, the coordinator writes a permanent `Commit` marker to the Raft log, finalizing the transaction atomically.
