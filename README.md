# Hermes Architecture

This document describes what actually runs when a Hermes node starts, how a request moves through the system, and where the honest edges of the implementation are. It is written for a reader evaluating engineering judgment, not for a first introduction to distributed systems — for that, see the per-package design notes linked at the bottom.

## 1. System shape

Hermes is a multi-Raft key-value store: the keyspace is range-partitioned into shards, and each shard is an independent Raft group replicated across a subset of nodes. This is the same high-level shape as CockroachDB and TiKV, and the tradeoff it buys is horizontal write throughput — no single Raft leader has to serialize every write in the cluster, only the writes for its shard.

```
                     ┌────────────┐
   client ──gRPC/────▶  Router    │  (pkg/partition)
           HTTP      │ (shard map)│
                     └─────┬──────┘
                           │ routes by key range
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │ Raft grp0│ │ Raft grp1│ │ Raft grp2│
        │ (shard0) │ │ (shard1) │ │ (shard2) │
        └────┬─────┘ └────┬─────┘ └────┬─────┘
             │             │             │
             ▼             ▼             ▼
        ┌─────────────────────────────────────┐
        │   per-node LSM storage engine        │
        │   WAL → MemTable → SSTable → compact │
        └─────────────────────────────────────┘

  Cluster-wide, independent of shard membership:
  SWIM gossip (pkg/membership) — who's alive
  Rate limiting + circuit breaker (pkg/ratelimit) — overload protection
  Observability (pkg/observability) — metrics/logs/traces/health
```

Every node runs the full stack — there's no separate "coordinator" node type. A node can simultaneously be the leader for shard 0 and a follower for shard 1.

## 2. Node startup sequence (`pkg/server/node.go: Start()`)

The order is deliberate and each step depends on the previous one being ready:

1. **Logger** — first, because every subsequent step logs through it.
2. **HLC** (`pkg/clock`) — hybrid logical clock, used to timestamp MVCC versions.
3. **Metrics / tracer / health checker** — so startup itself is observable.
4. **MVCC GC tracker** — created before storage opens, so no version can be garbage-collected before the tracker exists to protect it.
5. **Storage engine** (`storage.Open`) — opens/replays the WAL, initializes the memtable.
6. **gRPC server** — registers the KV, Raft, and Membership service handlers.
7. **Membership** (SWIM) — starts gossiping.
8. **Raft** — starts the consensus state machine for this node's shard(s).
9. **Shard map** — loads which key ranges this node owns.
10. **Consistency layer** — leader lease, ReadIndex tracker.
11. **Rate limiting + circuit breaker.**
12. **Chaos** — only if `EnableChaos` is set; logs a loud warning, never on by default.
13. **Metrics HTTP server.**
14. **Background loops** (compaction, GC, heartbeats) — started last, in their own goroutine.

If you're evaluating this code: the interesting engineering decision here isn't any single step, it's that storage (5) comes before Raft (8). A node must be able to durably persist and replay its own log before it can participate in consensus — get this ordering wrong and a node can acknowledge a Raft entry it hasn't actually made crash-safe yet.

## 3. Write path: `PUT key=value`

1. Client sends `PUT` to any node, over gRPC or HTTP.
2. `pkg/partition.Router` looks up which shard owns `key` (range partitioning — see §5) and, if this node isn't that shard's Raft leader, forwards the request.
3. The shard's Raft leader appends the command to its **in-memory** Raft log and replicates `AppendEntries` to followers in that shard's group.
4. Once a majority of the shard's replicas have persisted the entry (§4 below — this is where the WAL comes in), the leader advances `commitIndex` and applies the command to its local storage engine.
5. The storage engine writes: **WAL append + fsync → memtable insert**. The client receives success only after this local apply completes on the leader.
6. Followers receive the updated `leaderCommit` on the next heartbeat/AppendEntries and apply the same command to their own storage engines in the same order — this is what makes replica state converge.

Reads default to going through the leader via `ReadIndex` (`pkg/consistency/readindex.go`) to stay linearizable without needing a full Raft round-trip per read; `pkg/consistency/lease.go` implements the leader-lease optimization for the case where clock skew bounds are trusted.

## 4. Storage engine (`pkg/storage`)

Standard LSM design, each stage exists to solve a specific failure mode:

- **WAL** (`pkg/storage/wal`) — every write is appended and fsynced here *before* touching the memtable. This is the crash-safety boundary: if the process dies between WAL fsync and memtable insert, replay on restart reconstructs the memtable from the WAL. If it dies before fsync, the write was never acknowledged, so there's nothing to lose.
- **MemTable** (`pkg/storage/memtable`) — an in-memory sorted structure (skip list) for O(log n) inserts. This is what makes writes fast; nothing here has hit disk in its final form yet.
- **SSTable** (`pkg/storage/sstable`) — when the memtable crosses its size threshold, it's flushed as an immutable, sorted, on-disk file. Sequential write, which is why LSM trees favor write throughput over B-trees.
- **Bloom filter** (`pkg/storage/bloom`) — avoids disk reads for keys that definitely aren't in a given SSTable, which matters because a read may otherwise have to check every SSTable level.
- **MVCC GC** (`pkg/storage/mvcc_gc.go`) — reclaims old key versions once no active transaction can still observe them (tracked via `ActiveTransactionTracker`, started before any writes can occur — see startup step 4).

Read path: memtable → bloom filter per SSTable level → SSTable index (binary search) → block cache → disk. Write amplification and the level-based compaction strategy follow the standard LSM tradeoff (~10x per level); this is not currently tunable per-workload.

## 5. Partitioning (`pkg/partition`)

Hermes uses **range partitioning** as the primary strategy (shard 0 owns `["", "m")`, shard 1 owns `["m", "z")`, etc.) — the same choice CockroachDB, TiKV, and Spanner make, because it keeps range scans efficient (contiguous keys stay on one shard), at the cost of needing a deliberate split-key strategy to avoid hot shards. `pkg/partition/consistent_hash.go` also implements consistent hashing with virtual nodes, used internally for node-to-vnode assignment rather than as the primary keyspace partitioning strategy.

`pkg/partition/rebalancer.go` implements shard splitting: pick a split key, create a new Raft group, snapshot-transfer the affected range, atomically update the shard map, redirect traffic. **This is implemented but not currently triggered by anything** — see Known Limitations in the top-level README. It only runs today via its own demo path.

## 6. Membership and failure detection (`pkg/membership`)

SWIM (Scalable Weakly-consistent Infection-style Membership) handles cluster-wide "who's alive" independent of Raft — Raft only knows about the replicas in its own shard's group; SWIM knows about every node in the cluster. Failure detection uses a **phi-accrual detector** (`phi_accrual.go`, same algorithm Cassandra and Akka use) rather than a fixed timeout: instead of a binary alive/dead flag, it produces a suspicion score that adapts to observed heartbeat variance, which reduces false-positive failure detection on nodes with naturally jittery network latency.

## 7. Consistency guarantees actually provided today

- **Linearizable reads and writes within a shard** — via Raft + ReadIndex/leader-lease. Tested directly in `test/integration/system_test.go: TestLinearizability`.
- **Crash durability** — WAL fsync before ack. Tested in `TestDurability`.
- **Leader failover** — automatic re-election on leader loss, tested in `TestLeaderFailover`.
- **Partition tolerance** — tested in `TestNetworkPartition`.
- **No cross-shard transactional guarantees yet.** A transaction touching keys in shard 0 and shard 1 has no atomicity guarantee today — this is exactly the gap `pkg/txn`'s Percolator implementation exists to close, but it isn't wired into the write path (§9).

## 8. What chaos and verification tooling actually does (`pkg/chaos`)

This package is why the correctness claims above aren't just assertions:

- `fault_injector.go` — deliberately drops/delays messages and kills nodes mid-test.
- `jepsen_checker.go` — checks recorded operation histories for linearizability violations, in the spirit of Kyle Kingsbury's Jepsen methodology.
- `simulator.go` — a deterministic simulator (FoundationDB-style): runs the whole system against a simulated clock and network so failure scenarios are reproducible instead of relying on real-world timing flakiness.
- `snapshot.go` — Chandy-Lamport distributed snapshots, for capturing globally consistent state during simulated runs.

This is only ever active if `EnableChaos` is explicitly set, and the server logs a loud warning when it is — it's a verification tool, not a runtime feature.

## 9. Explicitly not wired in yet (and why that's stated, not hidden)

- **`pkg/txn`** (2PC, Percolator, Saga, SSI, timestamp oracle) is fully implemented with its own tests but is not called from the client write path. Percolator is the natural integration point for cross-shard transactions given the range-partitioned design (it's what TiKV does), and is the recommended next step over 2PC or Saga for that reason.
- **`pkg/cdc`** tails the WAL format but nothing currently subscribes to it externally.
- **`pkg/multiregion`** models topology but the router doesn't consult it — everything today is effectively single-region.
- **`pkg/query`** implements operator push-down planning for a query layer that doesn't have a query API in front of it yet — Hermes is a KV store (`get`/`put`/`delete`/`scan`), not a query engine, today.
- **TLS**: `pkg/transport/tls.go` produces valid credentials; `startGRPC()` does not currently pass them to `grpc.NewServer()`. This is a one-line-looking fix that's flagged as priority one in the roadmap precisely because it's the kind of gap that's easy to miss and expensive to ship.

Listing these here rather than only in code comments is intentional — a reader shouldn't have to grep for `TODO` to find out what's real.

## 10. Further reading

The following were written as first-principles design notes during implementation and are accurate but tutorial-voiced rather than reference-voiced — read this document first, then these for the "why" behind each algorithm choice:

- [`pkg/raft/readme.md`](../pkg/raft/readme.md) — Raft: leader election, log replication, the log matching property, why Raft over Paxos.
- [`pkg/storage/storage.md`](../pkg/storage/storage.md) — LSM-tree write/read paths, level structure, compaction.
- [`pkg/partition/readme.md`](../pkg/partition/readme.md) — partitioning strategy comparison, consistent hashing math, shard splitting.
