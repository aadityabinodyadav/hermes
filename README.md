# Hermes

A distributed, replicated key-value store written in Go, implementing Raft consensus, an LSM-tree storage engine, SWIM-based cluster membership, and range partitioning across multiple Raft groups.

Hermes was built to answer one question honestly: *if I implement the core algorithms behind etcd, CockroachDB, and TiKV from scratch — not a library wrapper — do they actually hold up under node failure, network partition, and concurrent writes?* The integration suite in `test/integration/` is the evidence: cluster formation, leader failover, network partition recovery, linearizability under concurrent load, and durability across restarts are all tested against a real multi-node cluster, not mocked.

**Contents**
- [Part 1 — Overview, quickstart, limitations](#part-1--overview)
  - [What's actually here](#whats-actually-here)
  - [Quickstart](#quickstart)
  - [Observability](#observability)
  - [Known limitations](#known-limitations)
  - [Roadmap](#roadmap)
- [Part 2 — Architecture](#part-2--architecture)
  - [System shape](#1-system-shape)
  - [Node startup sequence](#2-node-startup-sequence-pkgservernodego-start)
  - [Write path](#3-write-path-put-keyvalue)
  - [Storage engine](#4-storage-engine-pkgstorage)
  - [Partitioning](#5-partitioning-pkgpartition)
  - [Membership and failure detection](#6-membership-and-failure-detection-pkgmembership)
  - [Consistency guarantees actually provided today](#7-consistency-guarantees-actually-provided-today)
  - [Chaos and verification tooling](#8-what-chaos-and-verification-tooling-actually-does-pkgchaos)
  - [Explicitly not wired in yet](#9-explicitly-not-wired-in-yet-and-why-thats-stated-not-hidden)
  - [Further reading](#10-further-reading)

---

# Part 1 — Overview

## What's actually here

**Wired into the running server** (`pkg/server/node.go` starts all of these on every node):

| Subsystem | Package | Implements |
|---|---|---|
| Consensus | `pkg/raft` | Leader election, log replication, safety (Raft) |
| Membership | `pkg/membership` | SWIM gossip, phi-accrual failure detection |
| Storage | `pkg/storage` (+`wal`, `memtable`, `sstable`, `bloom`) | WAL → memtable → SSTable LSM pipeline, MVCC GC |
| Partitioning | `pkg/partition` | Range partitioning, consistent hashing, shard routing, rebalancer |
| Consistency | `pkg/consistency` | Leader leases, ReadIndex linearizable reads, distributed locks with fencing |
| Transport | `pkg/transport` | gRPC client/server, connection pooling, mTLS config (see limitations) |
| Reliability | `pkg/ratelimit` | Token-bucket rate limiting, circuit breaker |
| Observability | `pkg/observability` | Structured logging, Prometheus metrics, OpenTelemetry-style tracing, health checks |

**Implemented as standalone protocol modules, not yet wired into the live request path** — these run via their own CLI demo commands (see below) and have their own tests, but a client `PUT` today does not route through them:

| Subsystem | Package | Implements |
|---|---|---|
| Transactions | `pkg/txn` | 2PC, Percolator, Saga, Serializable Snapshot Isolation, timestamp oracle |
| CRDTs / snapshots | `pkg/consistency/crdt.go`, `pkg/chaos/snapshot.go` | Conflict-free replicated types, Chandy-Lamport global snapshots |
| Change data capture | `pkg/cdc` | WAL-tailing changefeed |
| Multi-region | `pkg/multiregion` | Geo-distributed topology |
| Query planning | `pkg/query` | Distributed query plan operator push-down |
| Chaos / verification | `pkg/chaos` | Fault injection, Jepsen-style linearizability checker, FoundationDB-style deterministic simulator |

This split is deliberate and stated up front rather than left for a reader to discover — see [Known Limitations](#known-limitations).

---

## Quickstart

### Run a 3-node cluster locally

```bash
# Terminal 1 — seed node
HERMES_NODE_ID=hermes-0 HERMES_LISTEN_ADDR=127.0.0.1:7001 \
HERMES_HTTP_ADDR=127.0.0.1:7000 HERMES_METRICS_ADDR=127.0.0.1:9000 \
HERMES_DATA_DIR=/tmp/hermes-0 HERMES_SEED_NODES="" \
go run ./cmd/hermes-server server

# Terminal 2
HERMES_NODE_ID=hermes-1 HERMES_LISTEN_ADDR=127.0.0.1:7011 \
HERMES_HTTP_ADDR=127.0.0.1:7010 HERMES_METRICS_ADDR=127.0.0.1:9010 \
HERMES_DATA_DIR=/tmp/hermes-1 HERMES_SEED_NODES=127.0.0.1:7001 \
go run ./cmd/hermes-server server

# Terminal 3
HERMES_NODE_ID=hermes-2 HERMES_LISTEN_ADDR=127.0.0.1:7021 \
HERMES_HTTP_ADDR=127.0.0.1:7020 HERMES_METRICS_ADDR=127.0.0.1:9020 \
HERMES_DATA_DIR=/tmp/hermes-2 HERMES_SEED_NODES=127.0.0.1:7001 \
go run ./cmd/hermes-server server
```

Or run `start_cluster.ps1` / `start_cluster.bat` to automate the above. A Kubernetes manifest set (StatefulSet, Services, Prometheus, Grafana) is in `deploy/kubernetes/`.

### Talk to it

```bash
go run ./cmd/hermes-cli put user:alice 1000
go run ./cmd/hermes-cli get user:alice
go run ./cmd/hermes-cli cluster status
```

or over HTTP:

```bash
curl -X POST http://127.0.0.1:7000/put -d '{"key":"user:alice","value":"1000"}'
curl "http://127.0.0.1:7000/get?key=user:alice"
```

### Run the test suite

```bash
go test ./...                                    # unit tests
go test ./test/integration/... -run TestCluster   # multi-node integration tests
go test -race ./...                              # with the race detector — always develop with this on
```

### Explore individual subsystems in isolation

Every core algorithm also runs as a standalone, narrated demo — useful for verifying one subsystem in isolation without standing up a cluster:

```bash
go run ./cmd/hermes-server raft          # leader election + log replication walkthrough
go run ./cmd/hermes-server storage       # WAL → memtable → SSTable → compaction
go run ./cmd/hermes-server partition     # consistent hashing + range partitioning
go run ./cmd/hermes-server txn           # 2PC / Percolator / Saga / SSI
go run ./cmd/hermes-server chaos         # fault injection + Jepsen-style checking
go run ./cmd/hermes-server all-demos     # run all of the above in sequence
```

---

## Observability

Prometheus metrics are exposed on `HERMES_METRICS_ADDR` (`curl http://127.0.0.1:9000/metrics`). A Grafana dashboard covering leader changes, replication lag, and LSM compaction stats is in `deploy/kubernetes/grafana-dashboard.json`.

---

## Known limitations

Stated directly rather than left for a code reviewer to find:

- **mTLS is configured but not enforced.** `pkg/transport/tls.go` loads and validates certificates, but `startGRPC()` currently discards the resulting credentials instead of passing them into `grpc.NewServer()`. The gRPC server runs in plaintext today. This is the single highest-priority item before any "production-ready" claim.
- **The transaction layer is not in the write path.** 2PC, Percolator, and SSI are implemented and unit-tested, but a client `PUT`/`GET` today goes through Raft + the LSM engine directly — it does not go through `pkg/txn`. Multi-key transactional writes are not currently possible via the client API.
- **Shard splitting is implemented but not triggered automatically.** `pkg/partition/rebalancer.go` contains the split/merge/move logic; nothing currently monitors shard load and calls it.
- **Single-region only in practice.** `pkg/multiregion` models geo-distributed topology but isn't consulted by the router.

## Roadmap

1. Wire TLS credentials into the gRPC server (correctness/security, should be first).
2. Route multi-key writes through `pkg/txn` (Percolator, since it's the modern/Spanner-style approach already implemented) instead of leaving it standalone.
3. Trigger the rebalancer from real shard-load metrics instead of running it only via its demo command.
4. Wire `pkg/cdc` into the WAL so changefeeds are consumable externally.

---
---

# Part 2 — Architecture

This part describes what actually runs when a Hermes node starts, how a request moves through the system, and where the honest edges of the implementation are. It's written for a reader evaluating engineering judgment, not for a first introduction to distributed systems — for that, see [Further reading](#10-further-reading) at the end.

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

`pkg/partition/rebalancer.go` implements shard splitting: pick a split key, create a new Raft group, snapshot-transfer the affected range, atomically update the shard map, redirect traffic. **This is implemented but not currently triggered by anything** — see [Known Limitations](#known-limitations). It only runs today via its own demo path.

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

- [`pkg/raft/readme.md`](pkg/raft/readme.md) — Raft: leader election, log replication, the log matching property, why Raft over Paxos.
- [`pkg/storage/storage.md`](pkg/storage/storage.md) — LSM-tree write/read paths, level structure, compaction.
- [`pkg/partition/readme.md`](pkg/partition/readme.md) — partitioning strategy comparison, consistent hashing math, shard splitting.

## License

Add a license before treating this as a public reference implementation — none is currently declared.
