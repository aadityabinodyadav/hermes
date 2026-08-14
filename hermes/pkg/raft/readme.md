# Raft consensus — design notes

> First-principles notes written while implementing `pkg/raft`. For how this fits into the running cluster (multi-Raft, one group per shard), see [`../../../docs/ARCHITECTURE.md`](../../../docs/ARCHITECTURE.md).

## Why consensus exists

Three nodes each store `balance=100`. Two clients write at the same time:

- Client A → Node 1: `balance=200`
- Client B → Node 2: `balance=50`

Without consensus: Node 1 thinks $200, Node 2 thinks $50, Node 3 doesn't know either happened. The replicas have silently diverged.

With consensus: all nodes agree on **one** value — either $200 wins or $50 wins, but every node ends up seeing the same answer.

## Raft in one paragraph

One node is the **leader**. All writes go through it. The leader appends the write to its log and sends it to followers. When a **majority** of nodes have the entry in their log, it's **committed** — it will never be lost, even if nodes crash afterward. The leader tells everyone to apply the committed entry, and every state machine applies the same entries in the same order, so every node ends up with identical state.

## The three subproblems Raft solves

1. **Leader election** — who's in charge right now?
2. **Log replication** — get all nodes to agree on the same sequence of operations.
3. **Safety** — never lose committed data, even during failures.

## Terms — the Raft clock

Time is divided into **terms** (monotonically increasing integers). Each term has at most one leader. Terms act as a logical clock: stale messages are rejected by comparing terms.

```
Term 1: Node1 is leader
────────────────────────────────────────
Node1 crashes
────────────────────────────────────────
Term 2: Election → Node2 wins
────────────────────────────────────────
Term 3: Election → Node3 wins (Node2 was slow)
```

If Node1 comes back believing it's still leader, it sends a message with `term=1`. Everyone else sees `term=1 < their term=3`, rejects it, and Node1 realizes it's stale and steps down.

## Leader election, step by step

Normal state: the leader sends heartbeats every 150ms, which reset each follower's election timer.

The leader crashes. No more heartbeats arrive.

```
Follower 1: election timeout fires (150–300ms, randomized)
  → becomes CANDIDATE
  → increments term: term = 2
  → votes for itself
  → sends RequestVote to Follower 2, Follower 3

Followers receiving RequestVote check:
  1. Is candidate's term >= mine?                  YES
  2. Have I already voted this term?               NO
  3. Is candidate's log at least as up-to-date?     YES
  → grant vote

Candidate receives a majority (2/3 including itself)
  → becomes LEADER for term 2
  → immediately sends heartbeats (AppendEntries with no entries)
```

The randomized timeout is what prevents every follower from becoming a candidate simultaneously and splitting the vote indefinitely.

## Log replication, step by step

Client sends `PUT balance 100` to the leader.

**Leader:**
1. Appends to its own log: `[{index:1, term:1, cmd:"PUT balance 100"}]`
2. Sends `AppendEntries` to all followers in parallel.

**AppendEntries message:**
```
term:          1
leaderId:      node-1
prevLogIndex:  0    ← entry before what I'm sending
prevLogTerm:   0    ← term of that previous entry
entries:       [{index:1, term:1, cmd:"PUT balance 100"}]
leaderCommit:  0    ← what's committed so far
```

**Follower receiving AppendEntries:**
1. Checks `prevLogIndex`/`prevLogTerm` match its own log — if not, it rejects and the leader backs up and retries with an earlier index.
2. Appends the entries to its own log.
3. Replies `success=true`.

**Leader receiving a majority of `success` responses:**
1. `commitIndex = 1` (majority now have it).
2. Applies the entry to its state machine: `storage.Put("balance", "100")`.
3. Replies to the client: `SUCCESS`.
4. The next heartbeat carries `leaderCommit=1`; followers advance their own `commitIndex` and apply the entry too.

## The log matching property (the core of safety)

> If two logs have an entry with the same index **and** the same term, the logs are identical up to that index.

This holds because:
1. A leader creates at most one entry per index per term.
2. Entries never change once written.
3. The `AppendEntries` consistency check (matching `prevLogIndex`/`prevLogTerm`) enforces this on every replication call.

The consequence: once an entry is committed (a majority has it), it is guaranteed to appear in every future leader's log. **Committed entries are never lost** — this is the property the entire system's durability claim rests on.

## The state machine

```
FOLLOWER ──timeout──▶ CANDIDATE ──majority votes──▶ LEADER
    ▲                     │                            │
    │                     │ higher term seen           │ higher term seen
    └─────────────────────┘◀───────────────────────────┘
```

- **Follower** — receives heartbeats from the leader, appends entries on `AppendEntries`, votes for at most one candidate per term. If no heartbeat arrives within [150ms, 300ms], becomes a candidate.
- **Candidate** — increments its own term, votes for itself, sends `RequestVote` to all peers. Becomes leader on a majority of votes; reverts to follower if it sees a valid `AppendEntries` from a legitimate leader; starts a new election (new term) on its own timeout.
- **Leader** — sends heartbeats every 50ms, accepts client writes, replicates log entries, advances `commitIndex` once a majority acknowledge.

## Why Raft instead of Paxos

Paxos is mathematically elegant but notoriously hard to implement correctly — it's famous for leaving the "how do you actually build this" details as an exercise for the reader.

Raft was explicitly designed for understandability:
- A strong leader simplifies log management (no need to resolve conflicting proposals from multiple nodes).
- Randomized timeouts make leader election simple to reason about.
- Joint consensus makes membership changes safe.
- It's a complete algorithm — Paxos famously isn't, in its original description.

Real systems built on Raft: etcd, CockroachDB, TiKV, Consul, InfluxDB, YugabyteDB, HashiCorp Vault.
