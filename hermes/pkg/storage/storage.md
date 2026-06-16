THE FUNDAMENTAL PROBLEM:

You need to store billions of key-value pairs.
Clients write and read them constantly.
The machine can crash at ANY moment.

Naive approach: just write to a file
  write("balance=100\n")     ← what if machine crashes mid-write?
  write("balance=150\n")     ← now file has partial data
  read()                     ← which value is correct? WHO KNOWS.

You need:
  1. Crash safety    → data survives power cuts (WAL)
  2. Fast writes     → don't seek disk randomly (LSM-Tree)  
  3. Fast reads      → don't scan entire dataset (Bloom Filter + Index)
  4. Concurrency     → readers don't block writers (MVCC)
  5. Compaction      → old versions don't fill disk forever

─────────────────────────────────────────────────────────────────

THE WRITE PATH (follow this precisely):

Client PUT(key="balance", value="100")
         │
         ▼
    ┌─────────────────────────────────────────┐
    │  1. WAL (Write-Ahead Log)               │
    │     Append entry to disk file           │
    │     fsync() ← blocks until durable      │
    │     NOW data survives crashes           │
    └────────────────┬────────────────────────┘
                     │ (only AFTER fsync succeeds)
                     ▼
    ┌─────────────────────────────────────────┐
    │  2. MemTable (in-memory sorted tree)    │
    │     Insert key → value into skip list   │
    │     O(log n) insert, all in RAM         │
    │     FAST                                │
    └────────────────┬────────────────────────┘
                     │ (when MemTable is full, ~64MB)
                     ▼
    ┌─────────────────────────────────────────┐
    │  3. Flush: MemTable → SSTable           │
    │     Write sorted key-value pairs        │
    │     to immutable on-disk file           │
    │     SEQUENTIAL write = fast             │
    └────────────────┬────────────────────────┘
                     │ (background compaction)
                     ▼
    ┌─────────────────────────────────────────┐
    │  4. Compaction                          │
    │     Merge multiple SSTables             │
    │     Remove deleted/old versions         │
    │     Keep storage manageable            │
    └─────────────────────────────────────────┘

THE READ PATH:

Client GET(key="balance")
         │
         ▼
    ┌──────────────────┐
    │ 1. MemTable      │ ← check here first (most recent writes)
    │    O(log n)      │   if found → return
    └────────┬─────────┘
             │ not found
             ▼
    ┌──────────────────┐
    │ 2. Bloom Filter  │ ← "is key DEFINITELY NOT in this SSTable?"
    │    O(1)          │   if filter says NO → skip SSTable entirely
    └────────┬─────────┘
             │ maybe in SSTable
             ▼
    ┌──────────────────┐
    │ 3. SSTable Index │ ← binary search for key's block
    │    O(log n)      │   find approximate location on disk
    └────────┬─────────┘
             │
             ▼
    ┌──────────────────┐
    │ 4. Block Cache   │ ← is this block already in memory?
    │    O(1)          │   cache hit → no disk read
    └────────┬─────────┘
             │ cache miss
             ▼
    ┌──────────────────┐
    │ 5. Disk Read     │ ← read the 4KB block from SSTable file
    │    ~100μs-10ms   │   scan block for key → return value
    └──────────────────┘

─────────────────────────────────────────────────────────────────

THE LSM-TREE SHAPE (Level Structure):

  Level 0: [SSTable1][SSTable2][SSTable3]    ← from MemTable flushes
           (may have overlapping key ranges)  ← max 4 files

  Level 1: [SSTable A][SSTable B][SSTable C] ← compacted from L0
           (non-overlapping key ranges)       ← max 10MB total

  Level 2: [        ][        ][        ]    ← larger, non-overlapping
           (non-overlapping key ranges)       ← max 100MB total

  Level 3: [                              ]  ← max 1GB total
  
  Each level is 10x larger than previous.
  Compaction merges L(n) files into L(n+1).
  
  Read: check L0 (all), then L1 (binary search), L2, L3...
  Write amplification: each byte written ~10-30x to disk total
  Read amplification: check multiple SSTables per read
  Space amplification: ~1.1x (10% overhead from compaction)