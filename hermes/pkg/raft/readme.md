WHY CONSENSUS EXISTS:

  You have 3 nodes. Each stores "balance=100".
  Two clients simultaneously write:
    Client A → Node 1: "balance=200"
    Client B → Node 2: "balance=50"
  
  Without consensus: Node 1 thinks $200, Node 2 thinks $50.
                     Node 3 doesn't know.
                     YOUR DATABASE IS BROKEN.
  
  With consensus: ALL nodes agree on ONE value.
                  Either $200 wins or $50 wins.
                  ALL nodes see the SAME answer.
                  YOUR DATABASE IS CORRECT.

─────────────────────────────────────────────────────────────────

RAFT IN ONE PARAGRAPH:

  One node is the LEADER. All writes go through it.
  Leader appends the write to its log, sends it to FOLLOWERS.
  When MAJORITY of nodes have the entry in their log,
  it's COMMITTED — it will NEVER be lost, even if nodes crash.
  Leader tells everyone to apply the committed entry.
  All state machines apply the SAME entries in the SAME order.
  Result: every node has IDENTICAL state.

─────────────────────────────────────────────────────────────────

THE THREE SUBPROBLEMS RAFT SOLVES:

  1. LEADER ELECTION
     "Who's in charge right now?"
     
  2. LOG REPLICATION  
     "Get all nodes to agree on the same sequence of operations"
     
  3. SAFETY
     "Never lose committed data, even during failures"

─────────────────────────────────────────────────────────────────

TERMS — THE RAFT CLOCK:

  Time is divided into TERMS (monotonically increasing integers)
  Each term has AT MOST one leader
  Terms act as a logical clock — stale messages are rejected
  
  Term 1: Node1 is leader
  ────────────────────────────────────────────────────────────
  Node1 crashes
  ────────────────────────────────────────────────────────────
  Term 2: Election → Node2 wins
  ────────────────────────────────────────────────────────────
  Term 3: Election → Node3 wins (Node2 was slow)
  
  If Node1 comes back and still thinks it's leader:
  It sends message with term=1
  Others see term=1 < their term=3
  They reject it → Node1 realizes it's stale → steps down

─────────────────────────────────────────────────────────────────

LEADER ELECTION — STEP BY STEP:

  Normal state: Leader sends heartbeats every 150ms
  
  ┌────────────────────────────────────────────────────────────┐
  │  Leader ──heartbeat──▶ Follower 1  (resets election timer) │
  │  Leader ──heartbeat──▶ Follower 2  (resets election timer) │
  └────────────────────────────────────────────────────────────┘
  
  Leader crashes. No more heartbeats.
  
  Follower 1: Election timeout fires (150-300ms random)
  ┌────────────────────────────────────────────────────────────┐
  │  Follower 1 becomes CANDIDATE                              │
  │  Increments term: term = 2                                 │
  │  Votes for itself                                          │
  │  Sends RequestVote to Follower 2, Follower 3               │
  └────────────────────────────────────────────────────────────┘
  
  Followers receive RequestVote:
  ┌────────────────────────────────────────────────────────────┐
  │  Check 1: Is candidate's term >= mine? YES                 │
  │  Check 2: Have I voted this term? NO                       │
  │  Check 3: Is candidate's log at least as up-to-date? YES   │
  │  → Grant vote                                              │
  └────────────────────────────────────────────────────────────┘
  
  Candidate receives majority (2/3 including itself):
  → Becomes LEADER for term 2
  → Immediately sends heartbeats (AppendEntries with no entries)

─────────────────────────────────────────────────────────────────

LOG REPLICATION — STEP BY STEP:

  Client sends PUT key=balance value=100 to leader

  Leader:
  ┌────────────────────────────────────────────────────────────┐
  │  1. Append to OWN log:                                     │
  │     [{index:1, term:1, cmd:"PUT balance 100"}]             │
  │  2. Send AppendEntries to ALL followers (parallel)         │
  └────────────────────────────────────────────────────────────┘
  
  AppendEntries message:
  ┌────────────────────────────────────────────────────────────┐
  │  term:          1                                          │
  │  leaderId:      node-1                                     │
  │  prevLogIndex:  0    ← entry before what I'm sending      │
  │  prevLogTerm:   0    ← term of that previous entry        │
  │  entries:       [{index:1, term:1, cmd:"PUT balance 100"}] │
  │  leaderCommit:  0    ← what's committed so far            │
  └────────────────────────────────────────────────────────────┘
  
  Follower receives AppendEntries:
  ┌────────────────────────────────────────────────────────────┐
  │  Check prevLogIndex/prevLogTerm match my log? YES          │
  │  Append entries to my log                                  │
  │  Reply: success=true                                       │
  └────────────────────────────────────────────────────────────┘
  
  Leader receives majority success responses:
  ┌────────────────────────────────────────────────────────────┐
  │  commitIndex = 1  (majority have it)                       │
  │  Apply to state machine: storage.Put("balance", "100")     │
  │  Reply to client: SUCCESS                                  │
  │  Next heartbeat carries leaderCommit=1                     │
  │  Followers advance their commitIndex                       │
  │  Followers apply to their state machines                   │
  └────────────────────────────────────────────────────────────┘

─────────────────────────────────────────────────────────────────

THE LOG MATCHING PROPERTY (SAFETY CORE):

  If two logs have an entry with same index AND same term:
    → They are IDENTICAL up to that index
  
  Why? Because:
  1. Leader creates at most ONE entry per index per term
  2. Entries never change once written
  3. AppendEntries consistency check enforces this
  
  This means: if entry is COMMITTED (majority have it),
  it will appear in ALL future leaders' logs.
  COMMITTED ENTRIES ARE NEVER LOST.

─────────────────────────────────────────────────────────────────

THE STATE MACHINE:

  FOLLOWER ──timeout──▶ CANDIDATE ──majority votes──▶ LEADER
      ▲                     │                            │
      │                     │ higher term seen           │ higher term seen
      └─────────────────────┘◀───────────────────────────┘
  
  FOLLOWER:
    - Receives heartbeats from leader
    - Appends entries to log on AppendEntries
    - Votes for candidates (at most one per term)
    - If no heartbeat in [150ms, 300ms]: become candidate
  
  CANDIDATE:
    - Increments own term
    - Votes for self
    - Sends RequestVote to all peers
    - If majority votes received: become leader
    - If AppendEntries received (new leader exists): revert to follower
    - If election timeout: start new election (new term)
  
  LEADER:
    - Sends heartbeats every 50ms
    - Accepts client writes
    - Replicates log entries to followers
    - Advances commitIndex when majority acknowledge

─────────────────────────────────────────────────────────────────

WHAT MAKES RAFT DIFFERENT FROM PAXOS:

  Paxos: Mathematically elegant, notoriously hard to implement
         "There are two things in computer science I know are hard:
          distributed consensus and naming things. And I'm not sure
          about naming things." — everyone who implemented Paxos
  
  Raft: Designed for UNDERSTANDABILITY
        Strong leader (simplifies log management)
        Randomized timeouts (simple leader election)
        Joint consensus for membership changes
        Complete algorithm (Paxos leaves many gaps)
        
  Real systems using Raft: etcd, CockroachDB, TiKV, Consul,
                            InfluxDB, YugabyteDB, Vault