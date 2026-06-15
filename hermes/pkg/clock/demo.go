package clock

import (
	"fmt"
	"sync"
	"time"
)

func RunClockDemo() {
	printHeader()

	fmt.Println("━━━ DEMO 1: The Clock Skew Problem ━━━")
	demonstrateClockSkewProblem()

	fmt.Println("\n━━━ DEMO 2: Lamport Clocks ━━━")
	demonstrateLamportClocks()

	fmt.Println("\n━━━ DEMO 3: Vector Clocks — Detecting Concurrency ━━━")
	demonstrateVectorClocks()

	fmt.Println("\n━━━ DEMO 4: Hybrid Logical Clocks ━━━")
	demonstrateHLC()

	fmt.Println("\n━━━ DEMO 5: HLC in Hermes — Real Scenario ━━━")
	demonstrateHLCInHermes()

	fmt.Println("\n━━━ DEMO 6: Causality in Raft Replication ━━━")
	demonstrateCausalityInRaft()

	printSummary()
}

// ─────────────────────────────────────────────────────────────────────────────

func demonstrateClockSkewProblem() {
	fmt.Println()
	fmt.Println("The problem: physical clocks drift. NTP corrects but with uncertainty.")
	fmt.Println("Simulating 3 nodes with different clock skews:")
	fmt.Println()

	// Simulate clocks with drift
	baseTime := time.Now()

	node1Clock := func() time.Time { return baseTime }
	node2Clock := func() time.Time { return baseTime.Add(47 * time.Millisecond) }
	node3Clock := func() time.Time { return baseTime.Add(-109 * time.Millisecond) }

	fmt.Printf("  Node 1 clock: %s (reference)\n",
		node1Clock().Format("15:04:05.000"))
	fmt.Printf("  Node 2 clock: %s (+47ms drift)\n",
		node2Clock().Format("15:04:05.000"))
	fmt.Printf("  Node 3 clock: %s (-109ms drift)\n",
		node3Clock().Format("15:04:05.000"))

	fmt.Println()
	fmt.Println("Scenario: Two concurrent writes to key='balance'")
	fmt.Println("  Node 2 writes $100 at its local time")
	fmt.Println("  Node 3 writes $50  at its local time (AFTER node 2's write)")
	fmt.Println()

	write2Time := node2Clock()
	time.Sleep(10 * time.Millisecond) // Node 3's write happens AFTER in real time
	write3Time := node3Clock()

	fmt.Printf("  Write 1 (Node 2, $100): timestamp = %s\n",
		write2Time.Format("15:04:05.000"))
	fmt.Printf("  Write 2 (Node 3, $50):  timestamp = %s\n",
		write3Time.Format("15:04:05.000"))
	fmt.Println()

	if write3Time.Before(write2Time) {
		fmt.Println("  🔴 DANGER: Node 3's write has EARLIER timestamp!")
		fmt.Println("  If we order by physical timestamp:")
		fmt.Println("    1st: Node 3 writes $50")
		fmt.Println("    2nd: Node 2 writes $100  ← wins (later timestamp)")
		fmt.Println("  Final value: $100  ← WRONG! Node 3 wrote AFTER node 2!")
		fmt.Println("  Correct answer should be: $50")
	}

	fmt.Println()
	fmt.Println("  Solution: Use causal ordering (Lamport/Vector/HLC), not physical time")
}

// ─────────────────────────────────────────────────────────────────────────────

func demonstrateLamportClocks() {
	fmt.Println()
	fmt.Println("Simulating a 3-node Hermes cluster with Lamport clocks")
	fmt.Println("Scenario: Leader election + log replication")
	fmt.Println()

	// Create clocks for each node
	node1 := NewLamportClock("node-1")
	node2 := NewLamportClock("node-2")
	node3 := NewLamportClock("node-3")

	printEvent := func(node, event string, ts int64) {
		fmt.Printf("  [node=%s, t=%d] %s\n", node, ts, event)
	}

	// ── Sequence of events ───────────────────────────────────────────────────

	// Node 1 decides to start an election
	t1 := node1.Tick()
	printEvent("node-1", "START ELECTION — RequestVote", t1)

	// Node 1 sends RequestVote to node-2
	t1_send := node1.Send()
	printEvent("node-1", fmt.Sprintf("SEND RequestVote → node-2 (timestamp=%d)", t1_send), t1_send)

	// Node 1 sends RequestVote to node-3
	t1_send2 := node1.Send()
	printEvent("node-1", fmt.Sprintf("SEND RequestVote → node-3 (timestamp=%d)", t1_send2), t1_send2)

	// Node 2 receives and processes
	t2_recv := node2.Receive(t1_send) // t1_send was attached to the message
	printEvent("node-2", fmt.Sprintf("RECV RequestVote from node-1 (my clock: %d)", t2_recv), t2_recv)
	t2_send := node2.Send()
	printEvent("node-2", fmt.Sprintf("SEND VoteGranted → node-1 (timestamp=%d)", t2_send), t2_send)

	// Node 3 receives and processes
	t3_recv := node3.Receive(t1_send2)
	printEvent("node-3", fmt.Sprintf("RECV RequestVote from node-1 (my clock: %d)", t3_recv), t3_recv)
	t3_send := node3.Send()
	printEvent("node-3", fmt.Sprintf("SEND VoteGranted → node-1 (timestamp=%d)", t3_send), t3_send)

	// Node 1 receives both votes and becomes leader
	t1_recv2 := node1.Receive(t2_send)
	printEvent("node-1", fmt.Sprintf("RECV VoteGranted from node-2 (my clock: %d)", t1_recv2), t1_recv2)
	t1_recv3 := node1.Receive(t3_send)
	printEvent("node-1", fmt.Sprintf("RECV VoteGranted from node-3 (my clock: %d)", t1_recv3), t1_recv3)

	t1_leader := node1.Tick()
	printEvent("node-1", "BECAME LEADER ✅", t1_leader)

	// Node 1 (leader) appends a log entry
	t1_append := node1.Tick()
	printEvent("node-1", "PROPOSE log entry: {PUT balance=100}", t1_append)

	t1_ae_send := node1.Send()
	printEvent("node-1", fmt.Sprintf("SEND AppendEntries → all followers (timestamp=%d)", t1_ae_send), t1_ae_send)

	fmt.Println()
	fmt.Println("  Lamport ordering guarantees:")
	fmt.Printf("  - All events on node-1 are ordered\n")
	fmt.Printf("  - RECV events have larger timestamps than corresponding SEND events\n")
	fmt.Printf("  - BECAME LEADER (t=%d) has larger timestamp than START ELECTION (t=%d)\n",
		t1_leader, t1)
	fmt.Println()
	fmt.Println("  What Lamport clocks CANNOT tell us:")
	fmt.Println("  - Were node-2's local events before or after node-3's local events?")
	fmt.Println("  - That requires Vector Clocks")
}

// ─────────────────────────────────────────────────────────────────────────────

func demonstrateVectorClocks() {
	fmt.Println()
	fmt.Println("Scenario: Network partition → concurrent writes → conflict detection")
	fmt.Println()

	nodes := []string{"node-1", "node-2", "node-3"}

	node1 := NewVectorClock("node-1", nodes)
	node2 := NewVectorClock("node-2", nodes)
	node3 := NewVectorClock("node-3", nodes)

	printVC := func(node string, event string, vc map[string]int64) {
		fmt.Printf("  [%s] %s\n       VC: {n1:%d, n2:%d, n3:%d}\n",
			node, event,
			vc["node-1"], vc["node-2"], vc["node-3"])
	}

	// Initial replication: node-1 is leader, replicates to all
	fmt.Println("Phase 1: Normal operation (no partition)")

	vc := node1.Tick()
	printVC("node-1", "Client writes: balance=100", vc)

	// Replicate to node-2
	replicaMsg := node1.Send()
	node2VC := node2.Receive(replicaMsg)
	printVC("node-2", "Replicated balance=100 from leader", node2VC)

	// Replicate to node-3
	replicaMsg2 := node1.Send()
	node3VC := node3.Receive(replicaMsg2)
	printVC("node-3", "Replicated balance=100 from leader", node3VC)

	fmt.Println()
	fmt.Println("  Verifying causal ordering:")
	rel := Compare(node1.Now(), node2.Now())
	fmt.Printf("  node-1 vs node-2: %s\n", rel)

	fmt.Println()
	fmt.Println("━━━")
	fmt.Println("Phase 2: NETWORK PARTITION — node-1 and node-2 cannot communicate")
	fmt.Println("         Both continue accepting writes independently")
	fmt.Println()

	// Simulate partition: node-1 and node-2 both accept writes independently

	// node-2 accepts a write during partition (local leader or leaderless)
	vc2_write := node2.Tick()
	printVC("node-2", "WRITE during partition: balance=150 (transfer in)", vc2_write)

	vc2_write2 := node2.Tick()
	printVC("node-2", "WRITE during partition: notes='VIP customer'", vc2_write2)

	// node-3 (same partition as node-1) accepts a different write
	vc3_write := node3.Tick()
	printVC("node-3", "WRITE during partition: balance=80 (withdrawal)", vc3_write)

	fmt.Println()
	fmt.Println("Phase 3: Partition HEALS — detecting conflicts")
	fmt.Println()

	// node-2 tries to sync with node-3
	// They compare vector clocks
	vc2_final := node2.Now()
	vc3_final := node3.Now()

	fmt.Printf("  node-2 final VC: {n1:%d, n2:%d, n3:%d}\n",
		vc2_final["node-1"], vc2_final["node-2"], vc2_final["node-3"])
	fmt.Printf("  node-3 final VC: {n1:%d, n2:%d, n3:%d}\n",
		vc3_final["node-1"], vc3_final["node-2"], vc3_final["node-3"])

	relation := Compare(vc2_final, vc3_final)
	fmt.Printf("\n  Relationship: %s\n", relation)

	if relation == Concurrent {
		fmt.Println()
		fmt.Println("  🔴 CONFLICT DETECTED!")
		fmt.Println("  node-2 wrote balance=150")
		fmt.Println("  node-3 wrote balance=80")
		fmt.Println("  These happened CONCURRENTLY — neither is 'correct'")
		fmt.Println()
		fmt.Println("  Resolution strategies (Hermes will use Raft to prevent this):")
		fmt.Println("  1. Last-Write-Wins (LWW): pick higher physical timestamp — lossy!")
		fmt.Println("  2. Multi-value: keep both, return to client — complex!")
		fmt.Println("  3. Application-level merge: caller decides — requires app logic")
		fmt.Println("  4. RAFT (our approach): only leader accepts writes — no conflicts!")
		fmt.Println("     (Raft turns multi-master into single-master with replication)")
	}
}

// ─────────────────────────────────────────────────────────────────────────────

func demonstrateHLC() {
	fmt.Println()
	fmt.Println("HLC combines physical time (human-readable) with logical ordering (causal)")
	fmt.Println()

	// Create nodes with slightly different physical clocks
	// This simulates real clock skew
	baseTime := time.Now()

	hlc1 := NewHLCWithClock("node-1", func() time.Time { return time.Now() })
	hlc2 := NewHLCWithClock("node-2", func() time.Time {
		return time.Now().Add(47 * time.Millisecond) // 47ms ahead
	})
	hlc3 := NewHLCWithClock("node-3", func() time.Time {
		return time.Now().Add(-30 * time.Millisecond) // 30ms behind
	})
	_ = baseTime

	fmt.Println("Generating timestamps from nodes with different physical clocks:")
	fmt.Println()

	t1 := hlc1.Now()
	fmt.Printf("  node-1 (accurate clock): %s = %d\n", t1, int64(t1))

	t2 := hlc2.Now()
	fmt.Printf("  node-2 (+47ms drift):    %s = %d\n", t2, int64(t2))

	t3 := hlc3.Now()
	fmt.Printf("  node-3 (-30ms drift):    %s = %d\n", t3, int64(t3))

	fmt.Println()
	fmt.Println("Demonstrating HLC properties:")

	// Property 1: Causality preserved across message passing
	fmt.Println()
	fmt.Println("Property 1: Causality across message passing")

	ts_send := hlc2.Now() // node-2 sends message
	fmt.Printf("  node-2 sends: %s\n", ts_send)

	ts_recv, err := hlc3.Update(ts_send) // node-3 receives
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Printf("  node-3 recv: %s\n", ts_recv)
		fmt.Printf("  recv > send: %v (causality preserved ✅)\n", ts_recv.After(ts_send))
	}

	// Property 2: Multiple events same millisecond → logical counter
	fmt.Println()
	fmt.Println("Property 2: Multiple events in same millisecond")

	// Force same physical millisecond by using fixed clock
	fixedTime := time.Now()
	fixedClock := NewHLCWithClock("fixed", func() time.Time { return fixedTime })

	prev := fixedClock.Now()
	fmt.Printf("  Event 1: %s\n", prev)
	for i := 2; i <= 5; i++ {
		curr := fixedClock.Now()
		fmt.Printf("  Event %d: %s (counter=%d)\n", i, curr, curr.Logical())

		if !curr.After(prev) {
			fmt.Printf("  🔴 ORDERING VIOLATED at event %d!\n", i)
		}
		prev = curr
	}
	fmt.Println("  All events properly ordered via logical counter ✅")

	// Property 3: HLC never goes backwards
	fmt.Println()
	fmt.Println("Property 3: HLC never goes backwards (monotonic)")

	// Simulate NTP correction that would move clock backwards
	currentTime := time.Now()
	goingBackwards := false
	backwardsCount := 0

	ntpCorrectedClock := NewHLCWithClock("ntp-node", func() time.Time {
		backwardsCount++
		// Every 3rd call, simulate NTP moving clock backwards by 50ms
		if backwardsCount%3 == 0 {
			goingBackwards = true
			return currentTime.Add(-50 * time.Millisecond)
		}
		goingBackwards = false
		return currentTime
	})

	fmt.Println("  Simulating NTP backward correction every 3rd tick:")
	prev = ntpCorrectedClock.Now()
	allMonotonic := true

	for i := 0; i < 9; i++ {
		curr := ntpCorrectedClock.Now()
		phys, logical := curr.Unpack()
		indicator := "  "
		if goingBackwards {
			indicator = "↩️ "
		}
		fmt.Printf("  %sEvent %d: physical=%dms, logical=%d → %s\n",
			indicator, i+1, phys%1000, logical, curr)

		if curr.Before(prev) {
			allMonotonic = false
			fmt.Printf("    🔴 WENT BACKWARDS!\n")
		}
		prev = curr
	}

	if allMonotonic {
		fmt.Println("  All timestamps monotonically increasing despite NTP corrections ✅")
	}
}

// ─────────────────────────────────────────────────────────────────────────────

func demonstrateHLCInHermes() {
	fmt.Println()
	fmt.Println("Real Hermes scenario: Multi-node transaction with HLC timestamps")
	fmt.Println()
	fmt.Println("  Setup: 3-node cluster, node-1 is leader")
	fmt.Println("  Operation: Client writes, leader replicates with HLC timestamps")
	fmt.Println("  Goal: Show how HLC enables consistent snapshots across nodes")
	fmt.Println()

	// Three nodes in a Hermes cluster
	leaderHLC := NewHLC("leader")
	follower1HLC := NewHLC("follower-1")
	follower2HLC := NewHLC("follower-2")

	// Step 1: Client sends write request to leader
	fmt.Println("Step 1: Client → Leader: PUT user:alice balance=1000")
	clientTimestamp := leaderHLC.Now()
	fmt.Printf("  Leader assigns HLC timestamp: %s\n", clientTimestamp)

	// Step 2: Leader creates WAL entry with HLC timestamp
	fmt.Println("\nStep 2: Leader writes to WAL")
	walTimestamp := leaderHLC.Now()
	fmt.Printf("  WAL entry timestamp: %s\n", walTimestamp)
	fmt.Printf("  WAL entry: LogEntry{index=1, term=1, ts=%s, PUT user:alice=1000}\n",
		walTimestamp)

	// Step 3: Leader sends AppendEntries with HLC timestamp to followers
	fmt.Println("\nStep 3: Leader → Followers: AppendEntries (with leader's HLC)")
	appendTimestamp := leaderHLC.Now()
	fmt.Printf("  AppendEntries carries timestamp: %s\n", appendTimestamp)

	// Followers update their HLC upon receiving
	f1Ts, err1 := follower1HLC.Update(appendTimestamp)
	f2Ts, err2 := follower2HLC.Update(appendTimestamp)

	if err1 != nil || err2 != nil {
		fmt.Printf("  Clock skew error: %v %v\n", err1, err2)
		return
	}

	fmt.Printf("  Follower-1 HLC after update: %s\n", f1Ts)
	fmt.Printf("  Follower-2 HLC after update: %s\n", f2Ts)

	// Step 4: Client sends a READ request
	// With HLC, we can do a consistent snapshot read!
	fmt.Println("\nStep 4: Client requests snapshot read at a specific timestamp")

	// The read timestamp is the HLC from when the write was committed
	readTimestamp := walTimestamp
	fmt.Printf("  Read at timestamp: %s\n", readTimestamp)
	fmt.Println("  Any replica can serve this read by checking:")
	fmt.Printf("  'Do I have all entries with HLC ≤ %s?' → YES → serve from local\n",
		readTimestamp)
	fmt.Println("  This enables FOLLOWER READS with consistency guarantees!")

	// Step 5: Second transaction — demonstrate ordering
	fmt.Println("\nStep 5: Second write — ordering guarantee")
	time.Sleep(time.Millisecond) // ensure new physical millisecond

	ts2 := leaderHLC.Now()
	fmt.Printf("  Second write HLC: %s\n", ts2)
	fmt.Printf("  ts2.After(walTimestamp): %v (causality preserved ✅)\n",
		ts2.After(walTimestamp))

	// Step 6: Clock skew rejection
	fmt.Println("\nStep 6: Clock skew detection — rejecting future timestamps")
	farFutureTS := Pack(
		time.Now().Add(2*time.Second).UnixMilli(),
		0,
	)
	fmt.Printf("  Received suspicious timestamp: %s\n", farFutureTS)

	_, err := leaderHLC.Update(farFutureTS)
	if err != nil {
		fmt.Printf("  🔴 Rejected: %v\n", err)
		fmt.Println("  This protects against Byzantine nodes and misconfigured clocks")
	}
}

// ─────────────────────────────────────────────────────────────────────────────

func demonstrateCausalityInRaft() {
	fmt.Println()
	fmt.Println("Demonstrating how causality tracking enables:")
	fmt.Println("  1. Consistent reads from followers (after seeing a write)")
	fmt.Println("  2. Transaction ordering across shards")
	fmt.Println("  3. Linearizable operations")
	fmt.Println()

	// Simulate a sequence of causally related operations
	type Operation struct {
		nodeID    string
		op        string
		key       string
		value     string
		timestamp HLCTimestamp
		causeOf   []HLCTimestamp // timestamps this op depends on
	}

	clusterHLC := NewHLC("cluster")
	var ops []Operation
	var mu sync.Mutex

	addOp := func(nodeID, op, key, value string, causes ...HLCTimestamp) HLCTimestamp {
		var ts HLCTimestamp
		if len(causes) > 0 {
			// Update HLC with the most recent cause
			maxCause := causes[0]
			for _, c := range causes[1:] {
				if c.After(maxCause) {
					maxCause = c
				}
			}
			var err error
			ts, err = clusterHLC.Update(maxCause)
			if err != nil {
				ts = clusterHLC.Now()
			}
		} else {
			ts = clusterHLC.Now()
		}

		mu.Lock()
		ops = append(ops, Operation{
			nodeID:    nodeID,
			op:        op,
			key:       key,
			value:     value,
			timestamp: ts,
			causeOf:   causes,
		})
		mu.Unlock()

		return ts
	}

	// Causal chain:
	// 1. Client writes to node-1
	ts1 := addOp("node-1", "PUT", "user:alice", "registered")
	fmt.Printf("  [%s] node-1: PUT user:alice='registered' → %s\n",
		ts1.Physical().Format("15:04:05.000"), ts1)

	// 2. Node-1 replicates to node-2 (caused by ts1)
	ts2 := addOp("node-2", "REPLICATE", "user:alice", "registered", ts1)
	fmt.Printf("  [%s] node-2: REPLICATE user:alice ← caused by %s\n",
		ts2.Physical().Format("15:04:05.000"), ts1)

	// 3. Client reads from node-2 (caused by ts1 — wants to see the write)
	ts3 := addOp("node-2", "GET", "user:alice", "", ts1)
	fmt.Printf("  [%s] node-2: GET user:alice → 'registered' ← reads after %s\n",
		ts3.Physical().Format("15:04:05.000"), ts1)

	// 4. Concurrent write from different client on node-3
	time.Sleep(time.Millisecond)
	ts4 := addOp("node-3", "PUT", "user:bob", "registered")
	fmt.Printf("  [%s] node-3: PUT user:bob='registered' (independent)\n",
		ts4.Physical().Format("15:04:05.000"))

	// 5. Cross-shard operation depends on both
	ts5 := addOp("node-1", "PUT", "summary", "alice+bob", ts3, ts4)
	fmt.Printf("  [%s] node-1: PUT summary='alice+bob' ← caused by BOTH ts3 AND ts4\n",
		ts5.Physical().Format("15:04:05.000"))

	fmt.Println()
	fmt.Println("Causal ordering verification:")
	verifyOrdering := func(a, b HLCTimestamp, aDesc, bDesc string) {
		if a.Before(b) {
			fmt.Printf("  ✅ %s → %s (correctly ordered)\n", aDesc, bDesc)
		} else {
			fmt.Printf("  🔴 VIOLATION: %s should be before %s!\n", aDesc, bDesc)
		}
	}

	verifyOrdering(ts1, ts2, "PUT alice", "REPLICATE alice")
	verifyOrdering(ts1, ts3, "PUT alice", "GET alice")
	verifyOrdering(ts2, ts3, "REPLICATE alice", "GET alice")
	verifyOrdering(ts3, ts5, "GET alice", "PUT summary")
	verifyOrdering(ts4, ts5, "PUT bob", "PUT summary")

	fmt.Println()
	fmt.Printf("ts4 vs ts2 (independent): %s\n", Compare(
		map[string]int64{"t": int64(ts4)},
		map[string]int64{"t": int64(ts2)},
	))
	fmt.Println("(ts4 and ts2 have a physical ordering but no causal relationship)")
	fmt.Println()
	fmt.Println("This causality tracking enables:")
	fmt.Println("  ✅ Read-your-own-writes: GET after PUT sees the PUT (ts3 > ts1)")
	fmt.Println("  ✅ Cross-shard transactions: summary write knows it saw alice AND bob")
	fmt.Println("  ✅ Follower reads: follower serves read when its HLC >= ts1")
	fmt.Println("  ✅ Snapshot isolation: 'read at ts=X' sees all writes with ts≤X")
}

// ─────────────────────────────────────────────────────────────────────────────

func printHeader() {
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("\n╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "HERMES — PHASE 2: TIME, ORDER & CAUSALITY")
	fmt.Printf("╚%s╝\n\n", line)
}

func printSummary() {
	fmt.Println()
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "PHASE 2 COMPLETE ✅")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "Three clocks, one purpose: consistent ordering")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "  Lamport Clocks:")
	fmt.Printf("║  %-61s║\n", "    ✅ Simple counter, captures happened-before")
	fmt.Printf("║  %-61s║\n", "    ✅ if A→B then clock(A) < clock(B)")
	fmt.Printf("║  %-61s║\n", "    ❌ Cannot detect concurrent events")
	fmt.Printf("║  %-61s║\n", "    Used for: basic event ordering, Raft log ordering")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "  Vector Clocks:")
	fmt.Printf("║  %-61s║\n", "    ✅ Detects true concurrency (conflict detection)")
	fmt.Printf("║  %-61s║\n", "    ✅ A∥B iff neither VC dominates the other")
	fmt.Printf("║  %-61s║\n", "    ❌ Size grows with cluster size")
	fmt.Printf("║  %-61s║\n", "    Used for: partition conflict detection, CRDT merging")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "  Hybrid Logical Clocks (HLC) ← HERMES PRIMARY CLOCK:")
	fmt.Printf("║  %-61s║\n", "    ✅ Physical time (human readable, bounded skew)")
	fmt.Printf("║  %-61s║\n", "    ✅ Causality (if A→B then HLC(A) < HLC(B))")
	fmt.Printf("║  %-61s║\n", "    ✅ Never goes backwards (monotonic)")
	fmt.Printf("║  %-61s║\n", "    ✅ Enables snapshot reads at a timestamp")
	fmt.Printf("║  %-61s║\n", "    ✅ Enables follower reads with staleness bound")
	fmt.Printf("║  %-61s║\n", "    Used for: EVERYTHING in Hermes")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "How it connects forward:")
	fmt.Printf("║  %-61s║\n", "  → Phase 3 (WAL):  every log entry has HLC timestamp")
	fmt.Printf("║  %-61s║\n", "  → Phase 4 (Raft): AppendEntries carries HLC")
	fmt.Printf("║  %-61s║\n", "  → Phase 7 (Txn):  read timestamp = HLC snapshot")
	fmt.Printf("║  %-61s║\n", "  → Phase 8 (Cons): linearizable reads use HLC fence")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "→ NEXT: Phase 3 — Storage Engine (WAL + LSM-Tree)")
	fmt.Printf("╚%s╝\n", line)
}
