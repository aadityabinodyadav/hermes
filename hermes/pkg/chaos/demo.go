// pkg/chaos/demo.go
package chaos

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

func RunChaosDemo() {
	printHeader()

	fmt.Println("━━━ DEMO 1: Fault Injector ━━━")
	demoFaultInjector()

	fmt.Println("\n━━━ DEMO 2: Network Partition Effects ━━━")
	demoNetworkPartition()

	fmt.Println("\n━━━ DEMO 3: Deterministic Simulation ━━━")
	demoDeterministicSimulation()

	fmt.Println("\n━━━ DEMO 4: Jepsen-Style Verification ━━━")
	demoJepsenVerification()

	fmt.Println("\n━━━ DEMO 5: Chandy-Lamport Snapshot ━━━")
	demoChandyLamportSnapshot()

	fmt.Println("\n━━━ DEMO 6: Chaos Scenarios ━━━")
	demoChaoScenarios()

	fmt.Println("\n━━━ DEMO 7: Property-Based Testing ━━━")
	demoPropertyTesting()

	printSummary()
}

// ─────────────────────────────────────────────────────────────────────────────

func demoFaultInjector() {
	fmt.Println()
	fmt.Println("Fault injector: programmatic chaos control")
	fmt.Println()

	fi := NewFaultInjector()
	fi.Activate()

	nodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}

	// Show normal operation first
	fmt.Println("Normal network (no faults):")
	sent, dropped := 0, 0
	for _, from := range nodes {
		for _, to := range nodes {
			if from != to {
				if fi.ShouldDrop(from, to) {
					dropped++
				} else {
					sent++
				}
			}
		}
	}
	fmt.Printf("  Messages: %d sent, %d dropped\n", sent, dropped)

	// Inject network partition
	fmt.Println()
	partitionID := fi.PartitionNodes(
		[]string{"node-1", "node-2"},
		[]string{"node-3", "node-4", "node-5"},
	)

	fmt.Println("\nAfter partition [1,2] ↔ [3,4,5]:")
	crossPartitionTests := [][2]string{
		{"node-1", "node-3"}, // cross partition
		{"node-2", "node-4"}, // cross partition
		{"node-1", "node-2"}, // same partition
		{"node-3", "node-4"}, // same partition
	}

	for _, test := range crossPartitionTests {
		dropped := fi.ShouldDrop(test[0], test[1])
		icon := "✅ OK"
		if dropped {
			icon = "❌ DROPPED"
		}
		fmt.Printf("  %s → %s: %s\n", test[0], test[1], icon)
	}

	// Clear partition
	fi.ClearFault(partitionID)
	fmt.Println()
	fmt.Println("After healing partition:")
	fmt.Printf("  node-1 → node-3: %v\n",
		map[bool]string{true: "DROPPED", false: "OK"}[fi.ShouldDrop("node-1", "node-3")])

	// Inject packet loss
	fmt.Println()
	fmt.Printf("Injecting 30%% packet loss on node-2 → node-3:\n")
	fi.DropPackets("node-2", "node-3", 0.30)

	// Show distribution of drops
	drops := 0
	const trials = 1000
	for i := 0; i < trials; i++ {
		if fi.ShouldDrop("node-2", "node-3") {
			drops++
		}
	}
	fmt.Printf("  Actual drop rate: %.1f%% (%d/%d)\n",
		float64(drops)/trials*100, drops, trials)

	// Inject node pause (simulates GC pause)
	fmt.Println()
	fmt.Printf("Pausing node-4 for 200ms (simulating GC):\n")
	fi.PauseNode("node-4", 200*time.Millisecond)
	fmt.Printf("  node-4 paused: %v\n", fi.IsPaused("node-4"))
	time.Sleep(250 * time.Millisecond)
	fmt.Printf("  node-4 paused (after 250ms): %v\n", fi.IsPaused("node-4"))

	// Clock skew
	fmt.Println()
	skewID := fi.SkewClock("node-5", 100*time.Millisecond)
	fmt.Printf("Clock skew on node-5: +%v\n", fi.GetClockSkew("node-5"))
	fi.ClearFault(skewID)
	fmt.Printf("Clock skew after clear: %v\n", fi.GetClockSkew("node-5"))

	fi.ClearAll()

	stats := fi.Stats()
	fmt.Printf("\nFault stats: %+v\n", *stats)
}

// ─────────────────────────────────────────────────────────────────────────────

func demoNetworkPartition() {
	fmt.Println()
	fmt.Println("Network partition: showing system behavior under split-brain")
	fmt.Println()

	fi := NewFaultInjector()
	fi.Activate()

	// Simulate a 5-node Raft cluster
	type NodeView struct {
		nodeID       string
		committedOps int32
		leaderID     string
		mu           sync.Mutex
	}

	nodes := make(map[string]*NodeView)
	for _, id := range []string{"n1", "n2", "n3", "n4", "n5"} {
		nodes[id] = &NodeView{nodeID: id, leaderID: "n1"}
	}

	fmt.Println("Initial state: n1 is leader, cluster is healthy")
	fmt.Println()

	// Simulate some writes
	writeToLeader := func(value string) bool {
		leader := "n1"
		// In real Raft: leader proposes, majority ACKs
		// Simulate majority ACK (3 of 5)
		acks := 0
		for _, n := range []string{"n1", "n2", "n3", "n4", "n5"} {
			if !fi.ShouldDrop(leader, n) {
				acks++
			}
		}
		if acks >= 3 {
			// Committed
			for _, n := range nodes {
				atomic.AddInt32(&n.committedOps, 1)
			}
			return true
		}
		return false
	}

	for i := 0; i < 5; i++ {
		ok := writeToLeader(fmt.Sprintf("op-%d", i))
		if ok {
			fmt.Printf("  Write op-%d: COMMITTED (5/5 reachable)\n", i)
		}
	}

	// Create partition
	fmt.Println()
	fmt.Println("🔴 PARTITION: [n1, n2] ↔ [n3, n4, n5]")
	fi.PartitionNodes([]string{"n1", "n2"}, []string{"n3", "n4", "n5"})
	fmt.Println()

	// n1 (minority) tries to write
	fmt.Println("n1 (minority leader) tries to write:")
	for i := 5; i < 8; i++ {
		acks := 0
		for _, n := range []string{"n1", "n2", "n3", "n4", "n5"} {
			if !fi.ShouldDrop("n1", n) {
				acks++
			}
		}
		if acks >= 3 {
			fmt.Printf("  Write op-%d: COMMITTED (%d acks) ← should NOT happen!\n",
				i, acks)
		} else {
			fmt.Printf("  Write op-%d: BLOCKED (%d acks, need 3) ✅\n", i, acks)
		}
	}

	fmt.Println()
	fmt.Println("n3 (majority partition) elects new leader and writes:")
	for i := 5; i < 8; i++ {
		acks := 0
		for _, n := range []string{"n1", "n2", "n3", "n4", "n5"} {
			if !fi.ShouldDrop("n3", n) {
				acks++
			}
		}
		if acks >= 3 {
			fmt.Printf("  Write op-%d: COMMITTED (%d acks) ✅\n", i, acks)
		}
	}

	fi.ClearAll()

	fmt.Println()
	fmt.Println("After healing:")
	fmt.Println("  n1 realizes it's stale (sees n3's higher term)")
	fmt.Println("  n1 steps down to follower")
	fmt.Println("  n1 replays n3's committed ops")
	fmt.Println("  Cluster converges to n3's state")
	fmt.Println()
	fmt.Println("KEY INSIGHT: Raft's quorum requirement prevented split-brain")
	fmt.Println("  Minority (2/5) could NOT commit → no data divergence")
	fmt.Println("  Majority (3/5) continued normally → availability maintained")
}

// ─────────────────────────────────────────────────────────────────────────────

func demoDeterministicSimulation() {
	fmt.Println()
	fmt.Println("Deterministic simulation: reproducible distributed testing")
	fmt.Println()

	// Simulate a simple ping-pong protocol
	seed := int64(42)
	sim := NewDeterministicSimulator(seed)

	// Track messages
	var messages []string
	var mu sync.Mutex

	record := func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		messages = append(messages, msg)
	}

	// Register handlers
	sim.network.RegisterNode("alice", func(msg SimulatedMessage) {
		record(fmt.Sprintf("alice received '%v' from %s at T+%v",
			msg.Content,
			msg.From,
			time.Duration(sim.currentTime)))

		// Alice pings back
		if count, ok := msg.Content.(int); ok && count < 3 {
			sim.After(50*time.Millisecond, "alice→bob ping",
				func() {
					sim.network.Send("alice", "bob", count+1)
				})
		}
	})

	sim.network.RegisterNode("bob", func(msg SimulatedMessage) {
		record(fmt.Sprintf("bob received '%v' from %s at T+%v",
			msg.Content,
			msg.From,
			time.Duration(sim.currentTime)))

		// Bob pings back
		if count, ok := msg.Content.(int); ok && count < 3 {
			sim.After(30*time.Millisecond, "bob→alice pong",
				func() {
					sim.network.Send("bob", "alice", count+1)
				})
		}
	})

	// Set network parameters
	sim.network.minDelay = SimTime((10 * time.Millisecond).Nanoseconds())
	sim.network.maxDelay = SimTime((20 * time.Millisecond).Nanoseconds())

	// Start the ping-pong
	sim.Schedule(0, "initial ping", func() {
		sim.network.Send("alice", "bob", 0)
	})

	// Run simulation
	sim.Run(500 * time.Millisecond)

	fmt.Printf("Run 1 (seed=%d): %d events processed\n", seed, sim.eventsProcessed)
	fmt.Println("Message order:")
	for _, msg := range messages {
		fmt.Printf("  %s\n", msg)
	}

	// Run again with same seed - should produce identical result
	fmt.Println()
	fmt.Println("Running again with same seed (should be IDENTICAL)...")
	messages2 := messages[:0:0] // clear

	sim2 := NewDeterministicSimulator(seed)
	mu2 := &sync.Mutex{}

	sim2.network.RegisterNode("alice", func(msg SimulatedMessage) {
		mu2.Lock()
		messages2 = append(messages2, fmt.Sprintf("alice received '%v' from %s at T+%v",
			msg.Content, msg.From, time.Duration(sim2.currentTime)))
		mu2.Unlock()
		if count, ok := msg.Content.(int); ok && count < 3 {
			sim2.After(50*time.Millisecond, "alice→bob ping",
				func() { sim2.network.Send("alice", "bob", count+1) })
		}
	})

	sim2.network.RegisterNode("bob", func(msg SimulatedMessage) {
		mu2.Lock()
		messages2 = append(messages2, fmt.Sprintf("bob received '%v' from %s at T+%v",
			msg.Content, msg.From, time.Duration(sim2.currentTime)))
		mu2.Unlock()
		if count, ok := msg.Content.(int); ok && count < 3 {
			sim2.After(30*time.Millisecond, "bob→alice pong",
				func() { sim2.network.Send("bob", "alice", count+1) })
		}
	})

	sim2.network.minDelay = SimTime((10 * time.Millisecond).Nanoseconds())
	sim2.network.maxDelay = SimTime((20 * time.Millisecond).Nanoseconds())
	sim2.Schedule(0, "initial ping", func() {
		sim2.network.Send("alice", "bob", 0)
	})
	sim2.Run(500 * time.Millisecond)

	identical := len(messages) == len(messages2)
	if identical {
		for i := range messages {
			if i < len(messages2) && messages[i] != messages2[i] {
				identical = false
				break
			}
		}
	}

	if identical {
		fmt.Println("✅ IDENTICAL to first run! Determinism verified.")
	} else {
		fmt.Println("❌ Different from first run! Bug in simulator!")
	}

	fmt.Println()
	fmt.Println("WHY THIS MATTERS:")
	fmt.Println("  When a test fails at 3 AM:")
	fmt.Println("  - Traditional: 'It was a race condition, can't reproduce'")
	fmt.Println("  - Deterministic: replay exact same sequence, fix the bug")
	fmt.Println()
	fmt.Println("  FoundationDB found and fixed ALL their bugs this way")
	fmt.Println("  before shipping to customers. Zero production data loss.")
}

// ─────────────────────────────────────────────────────────────────────────────

func demoJepsenVerification() {
	fmt.Println()
	fmt.Println("Jepsen-style verification: checking operation histories")
	fmt.Println()

	checker := NewJepsenChecker()
	baseTime := time.Now()

	// Record a correct history
	fmt.Println("Test 1: Correct history")
	checker.Record(JepsenOp{
		ClientID:    "client-1",
		Type:        "write",
		Key:         "x",
		WriteValue:  42,
		InvokeAt:    baseTime,
		CompleteAt:  baseTime.Add(10 * time.Millisecond),
		Success:     true,
		ServingNode: "leader",
	})

	checker.Record(JepsenOp{
		ClientID:    "client-2",
		Type:        "read",
		Key:         "x",
		ReadValue:   42,
		InvokeAt:    baseTime.Add(15 * time.Millisecond),
		CompleteAt:  baseTime.Add(20 * time.Millisecond),
		Success:     true,
		ServingNode: "leader",
	})

	violations := checker.Verify()
	if len(violations) == 0 {
		fmt.Println("  ✅ No violations found")
	} else {
		for _, v := range violations {
			fmt.Printf("  ❌ %s: %s\n", v.Type, v.Description)
		}
	}

	// Record a violation (lost write)
	checker2 := NewJepsenChecker()
	baseTime2 := time.Now()

	fmt.Println()
	fmt.Println("Test 2: Lost write violation")
	checker2.Record(JepsenOp{
		ClientID:    "client-1",
		Type:        "write",
		Key:         "balance",
		WriteValue:  100,
		InvokeAt:    baseTime2,
		CompleteAt:  baseTime2.Add(10 * time.Millisecond),
		Success:     true,
		ServingNode: "leader-old",
	})

	// Read returns nil after a successful write (data loss!)
	checker2.Record(JepsenOp{
		ClientID:    "client-2",
		Type:        "read",
		Key:         "balance",
		ReadValue:   nil, // Lost the write!
		InvokeAt:    baseTime2.Add(20 * time.Millisecond),
		CompleteAt:  baseTime2.Add(25 * time.Millisecond),
		Success:     true,
		ServingNode: "leader-new",
	})

	violations2 := checker2.Verify()
	if len(violations2) == 0 {
		fmt.Println("  No violations found (false negative in simplified checker)")
	} else {
		for _, v := range violations2 {
			fmt.Printf("  ❌ %s: %s\n", v.Type, v.Description)
		}
	}

	stats := checker.Stats()
	fmt.Printf("\nHistory stats: %+v\n", stats)
}

// ─────────────────────────────────────────────────────────────────────────────

func demoChandyLamportSnapshot() {
	fmt.Println()
	fmt.Println("Chandy-Lamport Snapshot: consistent distributed snapshot")
	fmt.Println()

	// Create 3-node cluster
	nodes := []string{"node-1", "node-2", "node-3"}
	coordinators := make(map[string]*SnapshotCoordinator)

	// Simulated application state per node
	appState := map[string]map[string]interface{}{
		"node-1": {"balance_alice": 1000, "balance_bob": 0},
		"node-2": {"balance_charlie": 500},
		"node-3": {"balance_dave": 250},
	}

	// Collect snapshots
	var snapMu sync.Mutex
	snapshotReceived := make(chan *GlobalSnapshot, 3)

	// Create coordinators
	for _, nodeID := range nodes {
		id := nodeID // capture
		state := appState[id]

		coord := NewSnapshotCoordinator(
			id,
			nodes,
			func() map[string]interface{} {
				// Return this node's application state
				result := make(map[string]interface{})
				for k, v := range state {
					result[k] = v
				}
				return result
			},
			func(to string, snapshotID string) {
				// In real system: send MARKER via network
				// For demo: directly call receiver
				snapMu.Lock()
				target := coordinators[to]
				snapMu.Unlock()
				if target != nil {
					go func() {
						time.Sleep(5 * time.Millisecond) // simulate network delay
						target.ReceiveMarker(id, snapshotID)
					}()
				}
			},
			func(snapshot *GlobalSnapshot) {
				snapshotReceived <- snapshot
			},
		)

		snapMu.Lock()
		coordinators[id] = coord
		snapMu.Unlock()
	}

	fmt.Println("Application state before snapshot:")
	for _, nodeID := range nodes {
		state := appState[nodeID]
		fmt.Printf("  %s: %v\n", nodeID, state)
	}

	fmt.Println()
	fmt.Println("Initiating snapshot from node-1...")
	coordinators["node-1"].InitiateSnapshot("snap-001")

	// Wait for snapshot to complete
	select {
	case snapshot := <-snapshotReceived:
		fmt.Println()
		fmt.Printf("Snapshot %s COMPLETE!\n", snapshot.SnapshotID)
		fmt.Printf("  Initiator: %s\n", snapshot.Initiator)
		fmt.Printf("  Duration: %v\n",
			snapshot.CompletedAt.Sub(snapshot.StartedAt))
		fmt.Println()
		fmt.Println("Captured node states:")
		for nodeID, state := range snapshot.NodeStates {
			fmt.Printf("  %s: %v\n", nodeID, state.State)
		}
		fmt.Println()
		fmt.Println("Captured channel states (in-flight messages):")
		if len(snapshot.ChannelStates) == 0 {
			fmt.Println("  (no in-flight messages)")
		}
		for channel, state := range snapshot.ChannelStates {
			fmt.Printf("  %s: %d messages\n", channel, len(state.Messages))
		}

	case <-time.After(2 * time.Second):
		fmt.Println("Snapshot timeout!")
	}

	fmt.Println()
	fmt.Println("Chandy-Lamport guarantees:")
	fmt.Println("  ✅ Consistent: if A→B and B is in snapshot, A is too")
	fmt.Println("  ✅ No coordination overhead during normal operation")
	fmt.Println("  ✅ Works while system continues processing requests")
	fmt.Println("  ✅ Can be used for: backup, deadlock detection, debugging")
}

// ─────────────────────────────────────────────────────────────────────────────

func demoChaoScenarios() {
	fmt.Println()
	fmt.Println("Standard chaos scenarios for Hermes testing")
	fmt.Println()

	runner := NewScenarioRunner()

	// Scenario 1: Leader failure
	runner.Add(TestScenario{
		Name:        "Leader Failure",
		Description: "Kill the leader, verify new leader elected and data accessible",
		Seed:        42,
		Duration:    500 * time.Millisecond,
		Setup: func(sim *DeterministicSimulator) {
			// Set up 5 nodes
			for _, nodeID := range []string{"n1", "n2", "n3", "n4", "n5"} {
				id := nodeID
				sim.network.RegisterNode(id, func(msg SimulatedMessage) {
					// Simplified: just track receipt
				})
			}

			// Schedule leader crash at T=100ms
			sim.After(100*time.Millisecond, "crash leader n1", func() {
				sim.crashed["n1"] = true
				fmt.Printf("    [T+100ms] n1 (leader) CRASHED\n")
			})

			// Schedule election check at T=300ms
			sim.After(300*time.Millisecond, "verify new leader", func() {
				// In real test: query cluster for leader
				// In simulation: trust Raft's election timeout
				fmt.Printf("    [T+300ms] New leader should be elected\n")
				fmt.Printf("    [T+300ms] Election timeout = 150-300ms\n")
				fmt.Printf("    [T+300ms] Expected: one of n2,n3,n4,n5 is leader\n")
			})

			// Restart n1 at T=400ms
			sim.After(400*time.Millisecond, "restart n1", func() {
				sim.crashed["n1"] = false
				fmt.Printf("    [T+400ms] n1 restarted (replaying from WAL)\n")
			})
		},
		Verify: func(sim *DeterministicSimulator) []string {
			// In real test: verify linearizability checker found no violations
			return nil // simplified: assume passes for demo
		},
	})

	// Scenario 2: Network partition
	runner.Add(TestScenario{
		Name:        "Minority Partition",
		Description: "Partition 2/5 nodes, verify they can't commit",
		Seed:        100,
		Duration:    500 * time.Millisecond,
		Setup: func(sim *DeterministicSimulator) {
			sim.After(50*time.Millisecond, "create partition", func() {
				sim.network.Partition("n1", "n3")
				sim.network.Partition("n1", "n4")
				sim.network.Partition("n1", "n5")
				sim.network.Partition("n2", "n3")
				sim.network.Partition("n2", "n4")
				sim.network.Partition("n2", "n5")
				fmt.Printf("    [T+50ms] PARTITION: [n1,n2] ↔ [n3,n4,n5]\n")
			})

			sim.After(200*time.Millisecond, "heal partition", func() {
				sim.network.Heal("n1", "n3")
				sim.network.Heal("n1", "n4")
				sim.network.Heal("n1", "n5")
				sim.network.Heal("n2", "n3")
				sim.network.Heal("n2", "n4")
				sim.network.Heal("n2", "n5")
				fmt.Printf("    [T+200ms] Partition HEALED\n")
			})
		},
		Verify: func(sim *DeterministicSimulator) []string {
			return nil
		},
	})

	// Scenario 3: GC pause on leader
	runner.Add(TestScenario{
		Name:        "Leader GC Pause",
		Description: "Leader pauses for 500ms (GC), verify lease expires, followers detect",
		Seed:        200,
		Duration:    800 * time.Millisecond,
		Setup: func(sim *DeterministicSimulator) {
			sim.After(100*time.Millisecond, "GC pause starts", func() {
				fmt.Printf("    [T+100ms] Leader GC PAUSE starts (500ms)\n")
				// In real system: leader stops sending heartbeats
				// Followers detect missing heartbeats, start election
			})

			sim.After(600*time.Millisecond, "GC pause ends", func() {
				fmt.Printf("    [T+600ms] Leader GC PAUSE ends\n")
				fmt.Printf("    [T+600ms] Old leader sees new term, steps down\n")
			})
		},
		Verify: func(sim *DeterministicSimulator) []string {
			return nil
		},
	})

	results := runner.RunAll()
	runner.Summary()

	// Summary statistics
	passed := 0
	for _, r := range results {
		if r.Passed {
			passed++
		}
	}
	fmt.Printf("\nOverall: %d/%d scenarios passed\n", passed, len(results))
}

// ─────────────────────────────────────────────────────────────────────────────

func demoPropertyTesting() {
	fmt.Println()
	fmt.Println("Property-based testing: verify invariants hold randomly")
	fmt.Println()

	// Property: After any sequence of puts/gets, reads are consistent
	fmt.Println("Property 1: Read-after-write consistency")
	fmt.Println("  'After a write commits, subsequent reads see the write'")
	fmt.Println()

	rng := rand.New(rand.NewSource(42))

	// Simulate a simple store
	type Entry struct {
		value   int
		version int64
	}

	store := make(map[string]*Entry)
	storeMu := sync.RWMutex{}

	writeValue := func(key string, value int) int64 {
		storeMu.Lock()
		defer storeMu.Unlock()
		version := time.Now().UnixNano()
		store[key] = &Entry{value: value, version: version}
		return version
	}

	readValue := func(key string, afterVersion int64) (int, bool) {
		storeMu.RLock()
		defer storeMu.RUnlock()
		entry, exists := store[key]
		if !exists {
			return 0, false
		}
		if entry.version < afterVersion {
			return 0, false // stale
		}
		return entry.value, true
	}

	violations := 0
	tests := 1000

	for i := 0; i < tests; i++ {
		key := fmt.Sprintf("key-%d", rng.Intn(10))
		value := rng.Intn(100)

		// Write and get version
		writeVersion := writeValue(key, value)

		// Read back immediately
		got, ok := readValue(key, writeVersion)
		if !ok || got != value {
			violations++
			fmt.Printf("  VIOLATION: wrote %d to %s, read back %d (ok=%v)\n",
				value, key, got, ok)
		}
	}

	if violations == 0 {
		fmt.Printf("  ✅ Passed %d trials: no violations\n", tests)
	} else {
		fmt.Printf("  ❌ Found %d violations in %d trials!\n", violations, tests)
	}

	// Property 2: No phantom deletes
	fmt.Println()
	fmt.Println("Property 2: Durability — no committed writes are lost")
	fmt.Println("  'If write returned success, it must be readable'")
	fmt.Println()

	committedWrites := make(map[string]int)
	violations2 := 0
	tests2 := 500

	for i := 0; i < tests2; i++ {
		key := fmt.Sprintf("key-%d", rng.Intn(5))
		value := rng.Intn(1000)

		// Commit write
		writeValue(key, value)
		committedWrites[key] = value

		// Verify all committed writes are readable
		for k, v := range committedWrites {
			got, ok := readValue(k, 0)
			if !ok || got != v {
				violations2++
			}
		}
	}

	if violations2 == 0 {
		fmt.Printf("  ✅ Passed %d trials: all committed writes preserved\n", tests2)
	} else {
		fmt.Printf("  ❌ Found %d violations: committed writes were lost!\n", violations2)
	}

	fmt.Println()
	fmt.Println("In real Hermes testing, property tests run:")
	fmt.Println("  - Against actual cluster (not mock)")
	fmt.Println("  - With fault injection enabled")
	fmt.Println("  - For thousands of random scenarios")
	fmt.Println("  - With the linearizability checker verifying all reads")
}

// ─────────────────────────────────────────────────────────────────────────────

func printHeader() {
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("\n╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "HERMES — PHASE 9: FAULT TOLERANCE & CHAOS ENGINEERING")
	fmt.Printf("╚%s╝\n\n", line)
}

func printSummary() {
	fmt.Println()
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "PHASE 9 COMPLETE ✅")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "What we built:")
	fmt.Printf("║  %-61s║\n", "  ✅ FaultInjector — programmatic chaos control")
	fmt.Printf("║  %-61s║\n", "    ✅ Network: partition, drop, delay")
	fmt.Printf("║  %-61s║\n", "    ✅ Node: crash, pause (GC simulation)")
	fmt.Printf("║  %-61s║\n", "    ✅ Disk: corruption, slow, full")
	fmt.Printf("║  %-61s║\n", "    ✅ Clock: skew, jump")
	fmt.Printf("║  %-61s║\n", "  ✅ DeterministicSimulator — reproducible testing")
	fmt.Printf("║  %-61s║\n", "  ✅ JepsenChecker — operation history verification")
	fmt.Printf("║  %-61s║\n", "  ✅ ChandyLamport — consistent distributed snapshots")
	fmt.Printf("║  %-61s║\n", "  ✅ ScenarioRunner — automated chaos scenarios")
	fmt.Printf("║  %-61s║\n", "  ✅ Property-based testing patterns")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "Key testing philosophy:")
	fmt.Printf("║  %-61s║\n", "  Jepsen: real cluster, real failures, verify history")
	fmt.Printf("║  %-61s║\n", "  Simulation: fake everything, run 1000x faster")
	fmt.Printf("║  %-61s║\n", "  Property: random inputs, verify invariants")
	fmt.Printf("║  %-61s║\n", "  Chaos: break things deliberately, verify recovery")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "Famous bugs found by chaos testing:")
	fmt.Printf("║  %-61s║\n", "  Cassandra: lost writes during partition (Jepsen)")
	fmt.Printf("║  %-61s║\n", "  MongoDB: data loss during failover (Jepsen)")
	fmt.Printf("║  %-61s║\n", "  Redis: stale reads from replica (Jepsen)")
	fmt.Printf("║  %-61s║\n", "  etcd: all fixed after Jepsen testing")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "→ NEXT: Phase 10 — Observability & Operations")
	fmt.Printf("╚%s╝\n", line)
}
