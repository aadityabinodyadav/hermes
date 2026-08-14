# LSM-tree storage engine — design notes

> First-principles notes written while implementing `pkg/storage`. For how this fits into the running cluster (this is what a Raft `commitIndex` advance actually applies to), see [`../../../docs/ARCHITECTURE.md`](../../../docs/ARCHITECTURE.md).

## The fundamental problem

You need to store billions of key-value pairs. Clients write and read them constantly. The machine can crash at any moment.

The naive approach — just write to a file — breaks immediately:

```
write("balance=100\n")   ← what if the machine crashes mid-write?
write("balance=150\n")   ← now the file has partial data
read()                   ← which value is correct? there's no way to know.
```

What's actually needed:

1. **Crash safety** — data survives power cuts (→ WAL)
2. **Fast writes** — no random disk seeks (→ LSM-tree)
3. **Fast reads** — no full-dataset scans (→ bloom filter + index)
4. **Concurrency** — readers don't block writers (→ MVCC)
5. **Compaction** — old versions don't fill the disk forever

## The write path

```
Client PUT(key="balance", value="100")
         │
         ▼
┌───────────────────────────────────┐
│ 1. WAL (Write-Ahead Log)           │
│    Append entry to disk file       │
│    fsync() ← blocks until durable  │
│    Data now survives crashes       │
└────────────────┬───────────────────┘
                  │ (only after fsync succeeds)
                  ▼
┌───────────────────────────────────┐
│ 2. MemTable (in-memory sorted tree)│
│    Insert key → value into skip list│
│    O(log n) insert, all in RAM     │
└────────────────┬───────────────────┘
                  │ (when MemTable is full, ~64MB)
                  ▼
┌───────────────────────────────────┐
│ 3. Flush: MemTable → SSTable       │
│    Write sorted key-value pairs    │
│    to an immutable on-disk file    │
│    Sequential write = fast         │
└────────────────┬───────────────────┘
                  │ (background)
                  ▼
┌───────────────────────────────────┐
│ 4. Compaction                      │
│    Merge multiple SSTables         │
│    Remove deleted/old versions     │
│    Keep storage size manageable    │
└───────────────────────────────────┘
```

## The read path

```
Client GET(key="balance")
         │
         ▼
┌──────────────────┐
│ 1. MemTable       │ ← check here first (most recent writes)
│    O(log n)       │   if found → return
└────────┬──────────┘
         │ not found
         ▼
┌──────────────────┐
│ 2. Bloom filter   │ ← "is this key DEFINITELY NOT in this SSTable?"
│    O(1)           │   if the filter says no → skip the SSTable entirely
└────────┬──────────┘
         │ maybe present
         ▼
┌──────────────────┐
│ 3. SSTable index  │ ← binary search for the key's block
│    O(log n)       │   finds the approximate location on disk
└────────┬──────────┘
         │
         ▼
┌──────────────────┐
│ 4. Block cache    │ ← is this block already in memory?
│    O(1)           │   cache hit → no disk read
└────────┬──────────┘
         │ cache miss
         ▼
┌──────────────────┐
│ 5. Disk read      │ ← read the 4KB block from the SSTable file
│    ~100μs–10ms     │   scan the block for the key → return the value
└──────────────────┘
```

## LSM-tree level structure

```
Level 0: [SSTable1][SSTable2][SSTable3]    ← from MemTable flushes
         (may have overlapping key ranges)  ← max 4 files

Level 1: [SSTable A][SSTable B][SSTable C] ← compacted from L0
         (non-overlapping key ranges)       ← max 10MB total

Level 2: [        ][        ][        ]    ← larger, non-overlapping
         (non-overlapping key ranges)       ← max 100MB total

Level 3: [                              ]  ← max 1GB total
```

Each level is roughly 10x larger than the one above it. Compaction merges level *n* files into level *n+1*.

- **Read**: check L0 (all files, since ranges overlap), then binary-search L1, L2, L3, ...
- **Write amplification**: each byte written is rewritten roughly 10–30x total across compactions.
- **Read amplification**: a single read may need to check multiple SSTables.
- **Space amplification**: roughly 1.1x — about 10% overhead from compaction bookkeeping.

This is the standard LSM tradeoff: writes are fast and sequential, at the cost of read and write amplification that grows with the number of levels. It's the same tradeoff RocksDB, LevelDB, and Cassandra's storage engines make.
