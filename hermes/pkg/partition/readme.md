THE SCALING PROBLEM:

  Single Raft group:
  ┌─────────────────────────────────────────────────┐
  │  Leader handles ALL writes                       │
  │  All 5 nodes store ALL data                      │
  │  Throughput limited by ONE leader's CPU/disk/net │
  │  Storage limited by ONE machine's disk           │
  └─────────────────────────────────────────────────┘
  
  At some point (let's say 10TB data, 100K writes/sec):
    → Single node can't store it all    (storage wall)
    → Single leader can't process all   (throughput wall)
    → Single Raft group replicates ALL  (bandwidth wall)
  
  Solution: PARTITION the data across multiple Raft groups
  
  Multi-Raft (what we build):
  ┌──────────────────────────────────────────────────────────────┐
  │ Shard 0: keys[""...,"m")   → Raft Group 0 [n1,n2,n3]        │
  │ Shard 1: keys["m"..."z"]   → Raft Group 1 [n2,n3,n4]        │
  │ Shard 2: keys["z"...]      → Raft Group 2 [n3,n4,n5]        │
  │                                                               │
  │ Each shard has its own leader, its own log                   │
  │ Total throughput = sum of all shard throughputs              │
  │ Total storage   = sum of all node disks / replication factor │
  └──────────────────────────────────────────────────────────────┘

─────────────────────────────────────────────────────────────────

THE THREE PARTITIONING STRATEGIES:

  1. HASH PARTITIONING
     partition = hash(key) mod N
     
     ┌─────────────────────────────────────────────┐
     │  "alice"  → hash=0x4a2f → 0x4a2f % 3 = 1   │
     │  "bob"    → hash=0x1c8b → 0x1c8b % 3 = 0   │
     │  "charlie"→ hash=0x7f3a → 0x7f3a % 3 = 2   │
     └─────────────────────────────────────────────┘
     
     ✅ Even distribution (if hash is good)
     ✅ Simple routing
     ❌ Range scans are TERRIBLE (alice,bob,charlie on different nodes)
     ❌ Changing N = reshuffle almost ALL data (mod N problem)
  
  2. RANGE PARTITIONING  
     Shard 0: ["" .. "m")
     Shard 1: ["m" .. "z")
     
     ┌─────────────────────────────────────────────┐
     │  "alice"  → shard 0 (a < m)                 │
     │  "bob"    → shard 0 (b < m)                 │
     │  "zebra"  → shard 1 (z >= m)                │
     └─────────────────────────────────────────────┘
     
     ✅ Range scans are efficient (contiguous keys on same shard)
     ✅ Adding shards is easy (split a range)
     ❌ Hot spots (all "user:*" keys on one shard)
     ❌ Requires a "split key" to be chosen carefully
     
     Hermes uses RANGE PARTITIONING (like CockroachDB, TiKV, Spanner)
  
  3. CONSISTENT HASHING
     Nodes are placed on a ring. Key's position on ring → nearest node.
     
     ┌─────────────────────────────────────────────┐
     │         0                                    │
     │    315    45                                 │
     │  270  [N1]  90                               │
     │    225  [N2] 135                             │
     │       180  [N3]                              │
     │                                              │
     │  hash("alice") = 60° → routes to N2         │
     │  N2 crashes: "alice" routes to N3 (next)    │
     │  Only 1/N data moves when node joins/leaves  │
     └─────────────────────────────────────────────┘
     
     ✅ Adding/removing nodes: minimum data movement
     ✅ No central metadata needed (algorithm determines owner)
     ❌ Non-uniform distribution (fixed with virtual nodes)
     ❌ Range scans still don't work
     
     Used by: Dynamo, Cassandra, Riak

─────────────────────────────────────────────────────────────────

HERMES PARTITION ARCHITECTURE:

  Routing Layer (knows shard map):
  ┌────────────────────────────────────────────────┐
  │  ShardMap:                                      │
  │    Shard 0: ["", "m") → RaftGroup{n1,n2,n3}   │
  │    Shard 1: ["m","z") → RaftGroup{n2,n3,n4}   │
  │    Shard 2: ["z", ∞ ) → RaftGroup{n3,n4,n5}   │
  └───────────────────┬────────────────────────────┘
                      │ route based on key
            ┌─────────┴──────────┐
            ▼                    ▼
    ┌──────────────┐    ┌──────────────┐
    │  Raft Group 0│    │  Raft Group 1│
    │  (owns ""~m) │    │  (owns m~z)  │
    │              │    │              │
    │  n1(LEADER)  │    │  n3(LEADER)  │
    │  n2(follower)│    │  n2(follower)│
    │  n3(follower)│    │  n4(follower)│
    └──────────────┘    └──────────────┘
    
  When "alice" comes in (a < m → Shard 0):
    Route to n1 (Shard 0's leader)
    n1 proposes to Raft Group 0
    Committed → applied to n1,n2,n3
  
  When "zoo" comes in (z >= z → Shard 2):
    Route to n3 (Shard 2's leader)
    n3 proposes to Raft Group 2
    Committed → applied to n3,n4,n5

─────────────────────────────────────────────────────────────────

SHARD SPLITTING (dynamic):

  Normal:   Shard 0 ["" .. "z")  ← gets too big / too hot
  
  Split at "m":
    Shard 0: ["" .. "m")         ← existing Raft group continues
    Shard 3: ["m" .. "z")        ← NEW Raft group created
  
  Migration steps:
    1. Find split key (midpoint or most frequent prefix)
    2. Create new Raft group
    3. Snapshot Shard 0's data and transfer to Shard 3
    4. Update ShardMap atomically
    5. Redirect traffic to correct shard
    6. Old shard deletes migrated data (GC)

─────────────────────────────────────────────────────────────────

CONSISTENT HASHING MATH:

  Ring size = 2^32 (4 billion positions)
  
  For each physical node, create V virtual nodes:
    hash("node-1-vnode-0") → position 0x1a2b3c4d
    hash("node-1-vnode-1") → position 0x5e6f7a8b
    ...
    hash("node-1-vnode-V") → position 0x...
  
  More virtual nodes = better load distribution
  V=150 gives ~5% imbalance (vs ~200% with V=1)
  
  For a key:
    hash(key) → position on ring
    Find nearest virtual node clockwise
    That virtual node's physical node owns the key
  
  Node leaves:
    Its virtual nodes are removed
    Keys route to NEXT virtual node clockwise
    Only the leaving node's data moves