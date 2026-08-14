# Partitioning — design notes

> First-principles notes written while implementing `pkg/partition`. For how this fits into the running cluster (range partitioning is the primary strategy; shard splitting is implemented but not yet auto-triggered), see [`../../../docs/ARCHITECTURE.md`](../../../docs/ARCHITECTURE.md).

## The scaling problem

A single Raft group looks like this:

```
┌─────────────────────────────────────────────────┐
│  Leader handles ALL writes                       │
│  All 5 nodes store ALL data                      │
│  Throughput limited by ONE leader's CPU/disk/net │
│  Storage limited by ONE machine's disk           │
└─────────────────────────────────────────────────┘
```

Past a certain scale — say, 10TB of data or 100K writes/sec — three walls show up at once: a single node can't store it all (storage wall), a single leader can't process it all (throughput wall), and a single Raft group can't replicate it all (bandwidth wall).

The fix is to **partition** the data across multiple independent Raft groups (multi-Raft):

```
┌──────────────────────────────────────────────────────────────┐
│ Shard 0: keys["",  "m")   → Raft Group 0 [n1, n2, n3]         │
│ Shard 1: keys["m", "z")   → Raft Group 1 [n2, n3, n4]         │
│ Shard 2: keys["z", ∞ )    → Raft Group 2 [n3, n4, n5]         │
│                                                                 │
│ Each shard has its own leader and its own log.                │
│ Total throughput = sum of all shard throughputs.               │
│ Total storage   = sum of all node disks / replication factor.  │
└──────────────────────────────────────────────────────────────┘
```

This is the design Hermes uses — see [`../server/node.go`](../server/node.go) for how each node starts its shard map and router alongside its Raft node.

## Three partitioning strategies, compared

### 1. Hash partitioning

`partition = hash(key) mod N`

```
"alice"   → hash=0x4a2f → 0x4a2f % 3 = 1
"bob"     → hash=0x1c8b → 0x1c8b % 3 = 0
"charlie" → hash=0x7f3a → 0x7f3a % 3 = 2
```

- ✅ Even distribution, if the hash function is good.
- ✅ Simple routing.
- ❌ Range scans are effectively useless — adjacent keys land on unrelated shards.
- ❌ Changing `N` reshuffles almost all data (the classic `mod N` problem).

### 2. Range partitioning — **what Hermes uses**

```
Shard 0: ["", "m")
Shard 1: ["m", "z")
```

```
"alice" → shard 0 (a < m)
"bob"   → shard 0 (b < m)
"zebra" → shard 1 (z >= m)
```

- ✅ Range scans are efficient — contiguous keys stay on the same shard.
- ✅ Adding shards is straightforward — split an existing range.
- ❌ Hot spots are possible (e.g. every `user:*` key landing on one shard).
- ❌ Requires deliberately choosing good split keys.

Same choice CockroachDB, TiKV, and Spanner make, for the same reason: range scans matter more than perfectly even load in most real workloads.

### 3. Consistent hashing

Nodes are placed on a ring; a key's position on the ring maps to the nearest node.

```
        0
   315    45
 270  [N1]  90
   225  [N2] 135
      180  [N3]

hash("alice") = 60° → routes to N2
N2 crashes: "alice" now routes to N3 (next clockwise)
Only ~1/N of the data moves when a node joins or leaves
```

- ✅ Minimal data movement when nodes join/leave.
- ✅ No central metadata needed — the algorithm alone determines ownership.
- ❌ Non-uniform distribution unless virtual nodes are used.
- ❌ Range scans still don't work.

Used by Dynamo, Cassandra, Riak. In Hermes this is implemented (`consistent_hash.go`) but used for virtual-node assignment within the cluster, not as the primary keyspace strategy — see the [architecture doc](../../../docs/ARCHITECTURE.md#5-partitioning-pkgpartition).

## Hermes's partition architecture

```
Routing layer (knows the shard map):
┌────────────────────────────────────────────────┐
│  ShardMap:                                      │
│    Shard 0: ["", "m")  → RaftGroup{n1,n2,n3}    │
│    Shard 1: ["m","z")  → RaftGroup{n2,n3,n4}    │
│    Shard 2: ["z", ∞ )  → RaftGroup{n3,n4,n5}    │
└───────────────────┬────────────────────────────┘
                     │ route based on key
           ┌─────────┴──────────┐
           ▼                    ▼
   ┌──────────────┐    ┌──────────────┐
   │  Raft Group 0│    │  Raft Group 1│
   │  (owns "" ~m)│    │  (owns m ~z) │
   │  n1 (leader) │    │  n3 (leader) │
   │  n2 (follower)│   │  n2 (follower)│
   │  n3 (follower)│   │  n4 (follower)│
   └──────────────┘    └──────────────┘
```

`"alice"` (a < m → Shard 0) routes to n1, n1 proposes to Raft Group 0, and once committed it's applied to n1/n2/n3.
`"zoo"` (z >= z → Shard 2) routes to n3, n3 proposes to Raft Group 2, and once committed it's applied to n3/n4/n5.

## Shard splitting

```
Normal:   Shard 0 [""..z)   ← gets too big / too hot

Split at "m":
  Shard 0: ["" .. "m")      ← existing Raft group continues
  Shard 3: ["m" .. "z")     ← new Raft group created
```

Migration steps:
1. Choose a split key (midpoint, or the most frequent prefix boundary).
2. Create a new Raft group for the new range.
3. Snapshot Shard 0's affected data and transfer it to Shard 3.
4. Update the shard map atomically.
5. Redirect traffic to the correct shard.
6. Old shard garbage-collects the migrated data.

This logic is fully implemented in `rebalancer.go`. **Nothing currently monitors shard load and triggers it automatically** — it runs today only via its own demo path. Automating this is the next concrete step for the partitioning layer (see the roadmap in the top-level README).

## Consistent hashing, in detail

```
Ring size = 2^32 (about 4 billion positions)

For each physical node, create V virtual nodes:
  hash("node-1-vnode-0") → position 0x1a2b3c4d
  hash("node-1-vnode-1") → position 0x5e6f7a8b
  ...

More virtual nodes = better load distribution.
V=150 gives roughly 5% imbalance (vs. roughly 200% with V=1).

For a key:
  hash(key) → position on the ring
  find the nearest virtual node clockwise
  that virtual node's physical node owns the key

When a node leaves:
  its virtual nodes are removed
  affected keys route to the next virtual node clockwise
  only the leaving node's data moves
```
