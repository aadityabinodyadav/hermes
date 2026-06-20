// pkg/partition/demo.go
package partition

import (
	"fmt"
	"math/rand"
	"sort"
)

func RunPartitionDemo() {
	printHeader()

	fmt.Println("━━━ DEMO 1: Consistent Hashing Ring ━━━")
	demoConsistentHash()

	fmt.Println("\n━━━ DEMO 2: The Mod-N Problem (Why Consistent Hashing) ━━━")
	demoModNProblem()

	fmt.Println("\n━━━ DEMO 3: Virtual Nodes & Load Distribution ━━━")
	demoVirtualNodes()

	fmt.Println("\n━━━ DEMO 4: Range Partitioning & ShardMap ━━━")
	demoShardMap()

	fmt.Println("\n━━━ DEMO 5: Router — Key to Node Resolution ━━━")
	demoRouter()

	fmt.Println("\n━━━ DEMO 6: Shard Split ━━━")
	demoShardSplit()

	fmt.Println("\n━━━ DEMO 7: Hot Spot Detection ━━━")
	demoHotSpot()

	fmt.Println("\n━━━ DEMO 8: Multi-Raft Architecture ━━━")
	demoMultiRaft()

	printSummary()
}

// ─────────────────────────────────────────────────────────────────────────────

func demoConsistentHash() {
	fmt.Println()
	fmt.Println("Building a consistent hash ring with 3 nodes:")
	fmt.Println()

	ring := NewConsistentHashRing(10) // 10 vnodes per node (small for demo)

	nodes := []string{"node-1", "node-2", "node-3"}
	for _, node := range nodes {
		ring.AddNode(node)
	}

	fmt.Println(ring.RingVisualization())

	// Route some keys
	fmt.Println("Key → Node routing:")
	keys := []string{
		"user:alice", "user:bob", "order:1001",
		"product:phone", "session:xyz", "cache:hot",
	}

	for _, key := range keys {
		node, _ := ring.GetNode(key)
		nodes3, _ := ring.GetNodes(key, 3) // 3-way replication
		fmt.Printf("  %-20s → primary=%-8s replicas=%v\n",
			key, node, nodes3)
	}

	fmt.Println()
	fmt.Println("Distribution across nodes:")
	dist := ring.Distribution()
	for node, pct := range dist {
		bar := ""
		for i := 0; i < int(pct/2); i++ {
			bar += "█"
		}
		fmt.Printf("  %-8s: %5.1f%% %s\n", node, pct, bar)
	}
}

// ─────────────────────────────────────────────────────────────────────────────

func demoModNProblem() {
	fmt.Println()
	fmt.Println("THE MOD-N PROBLEM: Why naive hashing breaks when nodes change")
	fmt.Println()

	// Simulate hashing 1000 keys across 3 nodes
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%04d", i)
	}

	modHash := func(key string, n int) int {
		h := 0
		for _, c := range key {
			h = h*31 + int(c)
		}
		if h < 0 {
			h = -h
		}
		return h % n
	}

	// Assignment with 3 nodes
	assignment3 := make(map[string]int)
	for _, key := range keys {
		assignment3[key] = modHash(key, 3)
	}

	// Add a 4th node
	assignment4 := make(map[string]int)
	for _, key := range keys {
		assignment4[key] = modHash(key, 4)
	}

	// Count keys that moved
	moved := 0
	for _, key := range keys {
		if assignment3[key] != assignment4[key] {
			moved++
		}
	}

	fmt.Printf("Hash mod 3 → mod 4 (adding 1 node to 3-node cluster):\n")
	fmt.Printf("  Total keys:     %d\n", len(keys))
	fmt.Printf("  Keys that MOVE: %d (%.1f%%)\n",
		moved, float64(moved)/float64(len(keys))*100)
	fmt.Printf("  Expected:       %.1f%% (formula: (N-1)/N × 100%%)\n",
		float64(3)/float64(4)*100)
	fmt.Println()
	fmt.Println("  This is CATASTROPHIC for a live system:")
	fmt.Println("  Adding 1 node means 75% of data must transfer!")
	fmt.Println("  During transfer: high disk I/O, network saturation")
	fmt.Println()

	// Now show consistent hashing
	ring3 := NewConsistentHashRing(150)
	for _, n := range []string{"node-1", "node-2", "node-3"} {
		ring3.AddNode(n)
	}

	ring4 := NewConsistentHashRing(150)
	for _, n := range []string{"node-1", "node-2", "node-3", "node-4"} {
		ring4.AddNode(n)
	}

	movedCH := 0
	for _, key := range keys {
		n3, _ := ring3.GetNode(key)
		n4, _ := ring4.GetNode(key)
		if n3 != n4 {
			movedCH++
		}
	}

	fmt.Printf("Consistent hashing (3 nodes → 4 nodes, 150 vnodes each):\n")
	fmt.Printf("  Total keys:     %d\n", len(keys))
	fmt.Printf("  Keys that MOVE: %d (%.1f%%)\n",
		movedCH, float64(movedCH)/float64(len(keys))*100)
	fmt.Printf("  Expected:       ~25%% (formula: 1/N × 100%%)\n")
	fmt.Println()
	fmt.Printf("  Improvement: %.1fx fewer keys move!\n",
		float64(moved)/float64(movedCH+1))
}

// ─────────────────────────────────────────────────────────────────────────────

func demoVirtualNodes() {
	fmt.Println()
	fmt.Println("Virtual nodes solve uneven distribution on the ring")
	fmt.Println()

	vnodeCounts := []int{1, 10, 50, 150}

	for _, vnodes := range vnodeCounts {
		ring := NewConsistentHashRing(vnodes)
		for _, n := range []string{"node-1", "node-2", "node-3", "node-4"} {
			ring.AddNode(n)
		}

		dist := ring.Distribution()

		// Calculate std dev from ideal (25%)
		ideal := 25.0
		maxDev := 0.0
		for _, pct := range dist {
			dev := pct - ideal
			if dev < 0 {
				dev = -dev
			}
			if dev > maxDev {
				maxDev = dev
			}
		}

		fmt.Printf("  %3d vnodes/node: max deviation from ideal = ±%.1f%%\n",
			vnodes, maxDev)
	}

	fmt.Println()
	fmt.Println("  With 150 vnodes: ±2-5% from ideal distribution")
	fmt.Println("  Memory cost: 150 × 4 nodes × 16 bytes = ~10KB (negligible)")
	fmt.Println()

	// Show with 150 vnodes
	ring := NewConsistentHashRing(150)
	for _, n := range []string{"node-1", "node-2", "node-3", "node-4"} {
		ring.AddNode(n)
	}

	dist := ring.Distribution()
	sorted := make([]string, 0, len(dist))
	for k := range dist {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	fmt.Printf("  Final distribution (150 vnodes per node):\n")
	for _, node := range sorted {
		pct := dist[node]
		bar := ""
		for i := 0; i < int(pct); i++ {
			bar += "█"
		}
		fmt.Printf("    %-8s: %5.1f%% %s\n", node, pct, bar)
	}
}

// ─────────────────────────────────────────────────────────────────────────────

func demoShardMap() {
	fmt.Println()
	fmt.Println("Range partitioning: keys are kept sorted, splits are easy")
	fmt.Println()

	nodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	sm := NewShardMap()
	sm.Initialize(4, nodes)

	fmt.Println()
	fmt.Println("Routing some keys:")
	testKeys := []string{
		"user:alice", "user:zeus", "order:0001",
		"payment:xyz", "session:abc", "product:tv",
	}

	for _, key := range testKeys {
		shard, err := sm.Lookup(key)
		if err != nil {
			fmt.Printf("  %-20s → ERROR: %v\n", key, err)
			continue
		}
		fmt.Printf("  %-20s → shard=%d group=%d leader=%s\n",
			key, shard.ShardID, shard.RaftGroup, shard.Leader)
	}

	fmt.Println()

	// Simulate leader updates from Raft
	fmt.Println("Simulating leader elections (Raft updates shard map):")
	sm.UpdateLeader(0, "node-1")
	sm.UpdateLeader(1, "node-2")
	sm.UpdateLeader(2, "node-3")
	sm.UpdateLeader(3, "node-4")

	fmt.Println("After leader updates:")
	for _, shard := range sm.All() {
		fmt.Printf("  %s\n", shard)
	}
}

// ─────────────────────────────────────────────────────────────────────────────

func demoRouter() {
	fmt.Println()
	fmt.Println("Router: intelligent key → node resolution with leader caching")
	fmt.Println()

	nodes := []string{"node-1", "node-2", "node-3"}
	sm := NewShardMap()
	sm.Initialize(3, nodes)
	sm.UpdateLeader(0, "node-1")
	sm.UpdateLeader(1, "node-2")
	sm.UpdateLeader(2, "node-3")

	router := NewRouter(sm, DefaultRouterConfig())

	// Route some requests
	keys := []string{
		"user:alice", "user:bob", "payment:001",
		"order:999", "cache:hot", "session:xyz",
	}

	fmt.Println("First routing (cold cache):")
	for _, key := range keys {
		req, err := router.Route(key)
		if err != nil {
			fmt.Printf("  %-20s → ERROR: %v\n", key, err)
			continue
		}
		fmt.Printf("  %-20s → %s (shard=%d)\n",
			key, req.TargetNode, req.Shard.ShardID)
	}

	fmt.Println()
	fmt.Println("Second routing (warm cache - same results, faster):")
	for _, key := range keys[:3] {
		req, _ := router.Route(key)
		fmt.Printf("  %-20s → %s (cached)\n", key, req.TargetNode)
	}

	fmt.Println()
	fmt.Println("Simulating NOT_LEADER response (leader changed):")
	req, _ := router.Route("user:alice")
	fmt.Printf("  Routed to: %s\n", req.TargetNode)
	fmt.Printf("  Got NOT_LEADER, new leader hint: node-3\n")

	newReq, err := router.HandleNotLeader(req, "node-3")
	if err == nil {
		fmt.Printf("  Retrying with: %s\n", newReq.TargetNode)
	}

	fmt.Println()
	fmt.Printf("Router stats: %s\n", router.Stats())
}

// ─────────────────────────────────────────────────────────────────────────────

func demoShardSplit() {
	fmt.Println()
	fmt.Println("Shard split: one hot shard → two cooler shards")
	fmt.Println()

	nodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	sm := NewShardMap()
	sm.Initialize(2, nodes)

	fmt.Println("Before split:")
	for _, s := range sm.All() {
		fmt.Printf("  %s\n", s)
	}

	// Shard 0 becomes too large, split at "G"
	newReplicas := []ReplicaDescriptor{
		{NodeID: "node-3", Address: "node-3:7000", Role: ReplicaVoter},
		{NodeID: "node-4", Address: "node-4:7000", Role: ReplicaVoter},
		{NodeID: "node-5", Address: "node-5:7000", Role: ReplicaVoter},
	}

	fmt.Println()
	fmt.Println("Splitting shard 0 at key 'G'...")
	original, newShard, err := sm.Split(0, "G", 10, newReplicas)
	if err != nil {
		fmt.Printf("Split error: %v\n", err)
		return
	}

	_ = newShard
	fmt.Println()
	fmt.Printf("After split (shard map v%d):\n", sm.Version())
	for _, s := range sm.All() {
		indicator := "  "
		if s.ShardID == original.ShardID {
			indicator = "📦"
		} else if s.ShardID == newShard.ShardID {
			indicator = "🆕"
		}
		fmt.Printf("%s %s\n", indicator, s)
	}

	// Verify routing still works
	fmt.Println()
	fmt.Println("Routing after split:")
	testKeys := []string{"Alice", "Bob", "George", "Zara"}
	for _, key := range testKeys {
		shard, _ := sm.Lookup(key)
		if shard != nil {
			fmt.Printf("  %-10s → shard=%d\n", key, shard.ShardID)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────

func demoHotSpot() {
	fmt.Println()
	fmt.Println("Hot spot problem: non-uniform key access patterns")
	fmt.Println()
	fmt.Println("Scenario: social media app, celebrities get 1000x more traffic")
	fmt.Println()

	nodes := []string{"node-1", "node-2", "node-3"}
	sm := NewShardMap()
	sm.Initialize(3, nodes)

	// Simulate key access patterns
	rng := rand.New(rand.NewSource(42))

	type KeyAccess struct {
		key    string
		shard  ShardID
		access int
	}

	var accesses []KeyAccess
	shardAccess := make(map[ShardID]int)

	// 90% of traffic goes to celebrity accounts
	celebrities := []string{"user:celeb1", "user:celeb2", "user:celeb3"}
	normalUsers := make([]string, 100)
	for i := range normalUsers {
		normalUsers[i] = fmt.Sprintf("user:normal%03d", i)
	}

	for i := 0; i < 10000; i++ {
		var key string
		if rng.Float64() < 0.9 {
			key = celebrities[rng.Intn(len(celebrities))]
		} else {
			key = normalUsers[rng.Intn(len(normalUsers))]
		}

		shard, _ := sm.Lookup(key)
		if shard != nil {
			shardAccess[shard.ShardID]++
			accesses = append(accesses, KeyAccess{
				key:   key,
				shard: shard.ShardID,
			})
		}
	}

	fmt.Println("Access distribution by shard:")
	total := 0
	for _, count := range shardAccess {
		total += count
	}

	for shardID := ShardID(0); shardID < 3; shardID++ {
		count := shardAccess[shardID]
		pct := float64(count) / float64(total) * 100
		bar := ""
		for i := 0; i < int(pct/2); i++ {
			bar += "█"
		}
		fmt.Printf("  Shard %d: %5d requests (%5.1f%%) %s\n",
			shardID, count, pct, bar)
	}

	fmt.Println()
	fmt.Println("Hot shard detection by rebalancer:")

	rebalancer := NewRebalancer(sm, DefaultRebalancerConfig())

	// Report stats for each shard
	for shardID := ShardID(0); shardID < 3; shardID++ {
		rps := float64(shardAccess[shardID]) / 60.0 // per second
		rebalancer.UpdateStats(ShardStats{
			ShardID:   shardID,
			SizeBytes: int64(shardAccess[shardID]) * 1024,
			RPS:       rps,
		})
	}

	rebalancer.evaluate()
	actions := rebalancer.PendingActions()

	if len(actions) == 0 {
		fmt.Println("  No actions needed (RPS below threshold for demo)")
	} else {
		for _, action := range actions {
			fmt.Printf("  → %s shard %d: %s\n",
				action.Type, action.ShardID, action.Reason)
		}
	}

	fmt.Println()
	fmt.Println("Solutions for hot spots:")
	fmt.Println("  1. Split the hot shard into smaller pieces")
	fmt.Println("  2. Use key prefix salting for celebrities")
	fmt.Println("     'user:celeb1' → 'user:celeb1#shard0', '#shard1', '#shard2'")
	fmt.Println("     Read from all, merge results")
	fmt.Println("  3. Cache layer in front (Redis/Memcached)")
	fmt.Println("  4. Leaderless reads for celebrity followers (eventual consistency)")
}

// ─────────────────────────────────────────────────────────────────────────────

func demoMultiRaft() {
	fmt.Println()
	fmt.Println("Multi-Raft: one Raft group per shard")
	fmt.Println()
	fmt.Println("Architecture:")
	fmt.Println()
	fmt.Println("  ┌──────────────────────────────────────────────────────────┐")
	fmt.Println("  │                    HERMES CLUSTER                        │")
	fmt.Println("  │                                                           │")
	fmt.Println("  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │")
	fmt.Println("  │  │ Raft Grp 0  │  │ Raft Grp 1  │  │ Raft Grp 2  │      │")
	fmt.Println("  │  │ Shard [0,G) │  │ Shard [G,M) │  │ Shard [M,∞) │      │")
	fmt.Println("  │  │             │  │             │  │             │      │")
	fmt.Println("  │  │ n1(LEADER)  │  │ n2(LEADER)  │  │ n3(LEADER)  │      │")
	fmt.Println("  │  │ n2(follower)│  │ n3(follower)│  │ n4(follower)│      │")
	fmt.Println("  │  │ n3(follower)│  │ n4(follower)│  │ n5(follower)│      │")
	fmt.Println("  │  └─────────────┘  └─────────────┘  └─────────────┘      │")
	fmt.Println("  │        │                │                │               │")
	fmt.Println("  │        │  Every node participates in    │               │")
	fmt.Println("  │        └──── MULTIPLE Raft groups ──────┘               │")
	fmt.Println("  │                                                           │")
	fmt.Println("  └──────────────────────────────────────────────────────────┘")
	fmt.Println()

	nodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	numShards := 3
	replicationFactor := 3

	fmt.Printf("Cluster: %d nodes, %d shards, %d-way replication\n\n",
		len(nodes), numShards, replicationFactor)

	// Show each node's Raft group memberships
	nodeMembership := make(map[string][]int) // nodeID → shard IDs

	splitPoints := computeSplitPoints(numShards)
	for i := 0; i < numShards; i++ {
		replicas := assignReplicas(nodes, i, replicationFactor)
		for _, r := range replicas {
			nodeMembership[r.NodeID] = append(nodeMembership[r.NodeID], i)
		}

		startKey := ""
		if i > 0 {
			startKey = splitPoints[i-1]
		}
		endKey := ""
		if i < len(splitPoints) {
			endKey = splitPoints[i]
		}
		if endKey == "" {
			endKey = "∞"
		}

		replicaIDs := make([]string, len(replicas))
		for j, r := range replicas {
			replicaIDs[j] = r.NodeID
		}
		fmt.Printf("  Raft Group %d [%q, %q): %v\n",
			i, startKey, endKey, replicaIDs)
	}

	fmt.Println()
	fmt.Println("Raft group memberships per node:")
	sortedNodes := make([]string, len(nodes))
	copy(sortedNodes, nodes)
	sort.Strings(sortedNodes)

	for _, nodeID := range sortedNodes {
		groups := nodeMembership[nodeID]
		fmt.Printf("  %-8s: Raft groups %v\n", nodeID, groups)
	}

	fmt.Println()
	fmt.Println("Why Multi-Raft scales:")
	fmt.Printf("  Single Raft: 1 leader handles ALL writes\n")
	fmt.Printf("  Multi-Raft:  %d leaders handle writes SIMULTANEOUSLY\n", numShards)
	fmt.Printf("  Throughput:  ~%dx higher than single Raft group\n", numShards)
	fmt.Println()
	fmt.Println("  Each node runs multiple Raft state machines simultaneously.")
	fmt.Println("  Each Raft group has its own:")
	fmt.Println("    - Election timeout goroutine")
	fmt.Println("    - Heartbeat goroutine")
	fmt.Println("    - Log (stored in same WAL, different prefix)")
	fmt.Println("    - State machine (same storage engine, different key range)")
	fmt.Println()
	fmt.Println("  Shared infrastructure per node:")
	fmt.Println("    - One gRPC server (routes messages to correct Raft group)")
	fmt.Println("    - One WAL (all groups write here, tagged by group ID)")
	fmt.Println("    - One storage engine (all groups use same SSTable layer)")
	fmt.Println("    - One gossip instance (cluster-level membership)")
}

// ─────────────────────────────────────────────────────────────────────────────

func printHeader() {
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("\n╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "HERMES — PHASE 5: PARTITIONING & SHARDING")
	fmt.Printf("╚%s╝\n\n", line)
}

func printSummary() {
	fmt.Println()
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "PHASE 5 COMPLETE ✅")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "What we built:")
	fmt.Printf("║  %-61s║\n", "  ✅ ShardMap — range-based routing table")
	fmt.Printf("║  %-61s║\n", "  ✅ ShardDescriptor — shard metadata")
	fmt.Printf("║  %-61s║\n", "  ✅ ConsistentHashRing — virtual nodes, minimal movement")
	fmt.Printf("║  %-61s║\n", "  ✅ Router — intelligent key→node with leader caching")
	fmt.Printf("║  %-61s║\n", "  ✅ Rebalancer — hot spot detection, split/merge triggers")
	fmt.Printf("║  %-61s║\n", "  ✅ Multi-Raft architecture design")
	fmt.Printf("║  %-61s║\n", "  ✅ Shard split protocol")
	fmt.Printf("║  %-61s║\n", "  ✅ Shard merge protocol")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "Key insights:")
	fmt.Printf("║  %-61s║\n", "  Mod-N hashing: 75% data moves on node add")
	fmt.Printf("║  %-61s║\n", "  Consistent hash: only 25% moves on node add")
	fmt.Printf("║  %-61s║\n", "  Virtual nodes (150): ±5% distribution imbalance")
	fmt.Printf("║  %-61s║\n", "  Range partitioning: enables efficient range scans")
	fmt.Printf("║  %-61s║\n", "  Multi-Raft: throughput scales with shard count")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "Real systems using these techniques:")
	fmt.Printf("║  %-61s║\n", "  CockroachDB: range partitioning + multi-Raft")
	fmt.Printf("║  %-61s║\n", "  TiKV: range partitioning + multi-Raft (PD)")
	fmt.Printf("║  %-61s║\n", "  Cassandra: consistent hashing + virtual nodes")
	fmt.Printf("║  %-61s║\n", "  DynamoDB: consistent hashing + range splits")
	fmt.Printf("║  %-61s║\n", "  Spanner: range partitioning + Paxos groups")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "How Phase 5 connects forward:")
	fmt.Printf("║  %-61s║\n", "  → Phase 6 (Membership): gossip propagates ShardMap")
	fmt.Printf("║  %-61s║\n", "  → Phase 7 (Transactions): cross-shard txns use Router")
	fmt.Printf("║  %-61s║\n", "  → Phase 8 (Consistency): follower reads need shard info")
	fmt.Printf("║  %-61s║\n", "  → Phase 10 (Ops): Rebalancer metrics → Grafana")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "→ NEXT: Phase 6 — Cluster Membership & Failure Detection")
	fmt.Printf("╚%s╝\n", line)
}
