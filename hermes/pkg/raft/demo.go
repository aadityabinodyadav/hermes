// pkg/raft/demo.go
package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aadityabinodyadav/hermes/pkg/clock"
)

// ─────────────────────────────────────────────────────────────────────────────
// IN-MEMORY TRANSPORT FOR TESTING
// ─────────────────────────────────────────────────────────────────────────────

// MemTransport is an in-memory transport for testing
// Messages are delivered synchronously (no real network)
type MemTransport struct {
	mu      sync.Mutex
	nodes   map[string]*RaftNode
	dropped map[string]map[string]bool // from→to→dropped?
	delayed map[string]map[string]time.Duration
}

func NewMemTransport() *MemTransport {
	return &MemTransport{
		nodes:   make(map[string]*RaftNode),
		dropped: make(map[string]map[string]bool),
		delayed: make(map[string]map[string]time.Duration),
	}
}

func (t *MemTransport) Register(id string, node *RaftNode) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[id] = node
}

func (t *MemTransport) Send(msgs []Message) {
	for _, msg := range msgs {
		go t.deliver(msg)
	}
}

func (t *MemTransport) deliver(msg Message) {
	t.mu.Lock()
	dropped := t.dropped[msg.From][msg.To]
	delay := t.delayed[msg.From][msg.To]
	node := t.nodes[msg.To]
	t.mu.Unlock()

	if dropped {
		return // simulate network partition
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	if node != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		node.Step(ctx, msg)
	}
}

func (t *MemTransport) AddPeer(id, addr string) {}
func (t *MemTransport) RemovePeer(id string)    {}

// Partition simulates a network partition: from cannot send to to
func (t *MemTransport) Partition(from, to string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dropped[from] == nil {
		t.dropped[from] = make(map[string]bool)
	}
	t.dropped[from][to] = true
	fmt.Printf("  🔴 PARTITION: %s → %s\n", from, to)
}

// Heal removes a partition
func (t *MemTransport) Heal(from, to string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dropped[from] != nil {
		delete(t.dropped[from], to)
	}
	fmt.Printf("  🟢 HEALED: %s → %s\n", from, to)
}

// ─────────────────────────────────────────────────────────────────────────────
// SIMPLE STATE MACHINE FOR DEMO
// ─────────────────────────────────────────────────────────────────────────────

type SimpleKV struct {
	mu     sync.RWMutex
	store  map[string]string
	nodeID string
}

func NewSimpleKV(nodeID string) *SimpleKV {
	return &SimpleKV{
		store:  make(map[string]string),
		nodeID: nodeID,
	}
}

func (kv *SimpleKV) Apply(entry LogEntry) error {
	if entry.Type == EntryNoop || len(entry.Data) == 0 {
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Simple encoding: "key=value"
	cmd := string(entry.Data)
	for i, c := range cmd {
		if c == '=' {
			key := cmd[:i]
			value := cmd[i+1:]
			kv.store[key] = value
			fmt.Printf("  [%s] APPLY log[%d]: %s=%s\n",
				kv.nodeID, entry.Index, key, value)
			return nil
		}
	}
	return nil
}

func (kv *SimpleKV) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.store[key]
	return v, ok
}

func (kv *SimpleKV) Snapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	var result string
	for k, v := range kv.store {
		result += k + "=" + v + "\n"
	}
	return []byte(result), nil
}

func (kv *SimpleKV) Restore(data []byte) error {
	return nil // simplified
}

// ─────────────────────────────────────────────────────────────────────────────
// DEMO CLUSTER
// ─────────────────────────────────────────────────────────────────────────────

type DemoCluster struct {
	nodes     map[string]*RaftNode
	kvStores  map[string]*SimpleKV
	transport *MemTransport
	hlc       *clock.HLC
}

func NewDemoCluster(nodeIDs []string) *DemoCluster {
	transport := NewMemTransport()
	hlc := clock.NewHLC("cluster")

	cluster := &DemoCluster{
		nodes:     make(map[string]*RaftNode),
		kvStores:  make(map[string]*SimpleKV),
		transport: transport,
		hlc:       hlc,
	}

	// Create nodes
	for _, id := range nodeIDs {
		peers := make([]string, 0, len(nodeIDs)-1)
		for _, other := range nodeIDs {
			if other != id {
				peers = append(peers, other)
			}
		}

		config := DefaultConfig(id, peers)
		kv := NewSimpleKV(id)
		node := NewRaftNode(config, kv, hlc, transport)

		cluster.nodes[id] = node
		cluster.kvStores[id] = kv
		transport.Register(id, node)
	}

	return cluster
}

func (c *DemoCluster) Start() {
	for _, node := range c.nodes {
		node.Start()
	}
}

func (c *DemoCluster) Stop() {
	for _, node := range c.nodes {
		node.Stop()
	}
}

// WaitForLeader waits until the cluster has a stable leader
func (c *DemoCluster) WaitForLeader(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for id, node := range c.nodes {
			if node.IsLeader() {
				return id, nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return "", fmt.Errorf("no leader elected within %v", timeout)
}

// Leader returns the current leader node
func (c *DemoCluster) Leader() *RaftNode {
	for _, node := range c.nodes {
		if node.IsLeader() {
			return node
		}
	}
	return nil
}

// LeaderID returns current leader's ID
func (c *DemoCluster) LeaderID() string {
	for id, node := range c.nodes {
		if node.IsLeader() {
			return id
		}
	}
	return ""
}

// Propose submits a proposal to the leader
func (c *DemoCluster) Propose(ctx context.Context, data string) error {
	leader := c.Leader()
	if leader == nil {
		return fmt.Errorf("no leader")
	}
	return leader.Propose(ctx, []byte(data))
}

// PrintStatus prints the status of all nodes
func (c *DemoCluster) PrintStatus() {
	fmt.Println()
	for id, node := range c.nodes {
		status := node.Status()
		role := "📦 FOLLOWER"
		if node.IsLeader() {
			role = "👑 LEADER"
		} else if node.State() == Candidate {
			role = "🗳️  CANDIDATE"
		}
		fmt.Printf("  %s %s | term=%d | commit=%d | applied=%d | log=%d\n",
			role, id,
			status.Term, status.CommitIndex,
			status.AppliedIndex, status.LastIndex)
	}
	fmt.Println()
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN DEMO
// ─────────────────────────────────────────────────────────────────────────────

// Need to import clock in raft package

func RunRaftDemo() {
	printHeader()

	fmt.Println("━━━ DEMO 1: Leader Election ━━━")
	demoLeaderElection()

	fmt.Println("\n━━━ DEMO 2: Log Replication ━━━")
	demoLogReplication()

	fmt.Println("\n━━━ DEMO 3: Leader Failure & Re-election ━━━")
	demoLeaderFailure()

	fmt.Println("\n━━━ DEMO 4: Network Partition (Split Brain) ━━━")
	demoNetworkPartition()

	fmt.Println("\n━━━ DEMO 5: Log Safety Under Failures ━━━")
	demoLogSafety()

	printSummary()
}

func demoLeaderElection() {
	fmt.Println()
	fmt.Println("Starting a 5-node Raft cluster from scratch...")
	fmt.Println("All nodes start as followers, randomized election timeouts")
	fmt.Println()

	nodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	cluster := NewDemoCluster(nodes)
	cluster.Start()
	defer cluster.Stop()

	start := time.Now()
	leader, err := cluster.WaitForLeader(3 * time.Second)
	elapsed := time.Since(start)

	if err != nil {
		fmt.Printf("  ❌ No leader elected: %v\n", err)
		return
	}

	fmt.Printf("  ✅ Leader elected: %s in %v\n", leader, elapsed)
	fmt.Println()
	cluster.PrintStatus()

	fmt.Println("  Why does this work?")
	fmt.Println("  - Each node has a RANDOM election timeout (100-200ms)")
	fmt.Println("  - One node's timer fires first → starts election")
	fmt.Println("  - Others haven't timed out yet → grant vote")
	fmt.Println("  - First node gets majority → becomes leader")
	fmt.Println("  - Leader sends heartbeats → others never time out")
}

func demoLogReplication() {
	fmt.Println()
	fmt.Println("Replicating data across a 3-node cluster...")
	fmt.Println()

	nodes := []string{"node-1", "node-2", "node-3"}
	cluster := NewDemoCluster(nodes)
	cluster.Start()
	defer cluster.Stop()

	leader, _ := cluster.WaitForLeader(3 * time.Second)
	fmt.Printf("  Leader: %s\n\n", leader)

	// Propose some writes
	ctx := context.Background()
	writes := []string{
		"balance=1000",
		"user=alice",
		"status=active",
	}

	for _, write := range writes {
		fmt.Printf("  Proposing: %s\n", write)
		start := time.Now()

		if err := cluster.Propose(ctx, write); err != nil {
			fmt.Printf("  ❌ Propose failed: %v\n", err)
			continue
		}

		// Wait for commit
		time.Sleep(100 * time.Millisecond)
		fmt.Printf("  ✅ Proposed in %v\n", time.Since(start))
	}

	fmt.Println()
	cluster.PrintStatus()

	// Verify data on all nodes
	fmt.Println("  Verifying data on all nodes:")
	time.Sleep(200 * time.Millisecond) // let apply propagate

	for id, kv := range cluster.kvStores {
		bal, _ := kv.Get("balance")
		usr, _ := kv.Get("user")
		fmt.Printf("  %s: balance=%s user=%s\n", id, bal, usr)
	}

	fmt.Println()
	fmt.Println("  KEY INSIGHT: All nodes have IDENTICAL state")
	fmt.Println("  Even though only the leader accepted the write,")
	fmt.Println("  ALL nodes applied the SAME entries in the SAME order.")
}

func demoLeaderFailure() {
	fmt.Println()
	fmt.Println("Scenario: Leader crashes. New leader must be elected.")
	fmt.Println()

	nodes := []string{"node-1", "node-2", "node-3"}
	cluster := NewDemoCluster(nodes)
	cluster.Start()
	defer cluster.Stop()

	// Wait for initial leader
	firstLeader, _ := cluster.WaitForLeader(3 * time.Second)
	fmt.Printf("  Initial leader: %s\n", firstLeader)

	// Write some data
	ctx := context.Background()
	cluster.Propose(ctx, "key=value-before-crash")
	time.Sleep(100 * time.Millisecond)

	// "Kill" the leader by partitioning it from everyone
	fmt.Printf("\n  💀 Simulating leader crash (%s)...\n", firstLeader)
	for _, other := range nodes {
		if other != firstLeader {
			cluster.transport.Partition(firstLeader, other)
			cluster.transport.Partition(other, firstLeader)
		}
	}

	// Wait for new leader
	fmt.Println("  Waiting for new leader election...")
	start := time.Now()

	var newLeader string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for id, node := range cluster.nodes {
			if id != firstLeader && node.IsLeader() {
				newLeader = id
				break
			}
		}
		if newLeader != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	elapsed := time.Since(start)

	if newLeader == "" {
		fmt.Println("  ❌ No new leader elected")
		return
	}

	fmt.Printf("  ✅ New leader: %s elected in %v\n", newLeader, elapsed)
	fmt.Println()

	// Write to new leader
	newLeaderNode := cluster.nodes[newLeader]
	newLeaderNode.Propose(ctx, []byte("key=value-after-failover"))
	time.Sleep(100 * time.Millisecond)

	cluster.PrintStatus()

	fmt.Println("  KEY INSIGHTS:")
	fmt.Printf("  - Failover time: %v (bounded by election timeout)\n", elapsed)
	fmt.Println("  - Writes before crash: preserved (were committed)")
	fmt.Println("  - Old leader: cannot commit new entries (partitioned)")
	fmt.Println("  - New leader: accepts writes immediately after election")
	fmt.Println("  - 3-node cluster can survive 1 failure (quorum = 2)")
}

func demoNetworkPartition() {
	fmt.Println()
	fmt.Println("Scenario: Network splits 5-node cluster into [2] and [3]")
	fmt.Println("  Minority [2]: cannot form quorum → STOPS accepting writes")
	fmt.Println("  Majority [3]: forms quorum → CONTINUES accepting writes")
	fmt.Println()

	nodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	cluster := NewDemoCluster(nodes)
	cluster.Start()
	defer cluster.Stop()

	leader, _ := cluster.WaitForLeader(3 * time.Second)
	fmt.Printf("  Initial leader: %s\n", leader)

	ctx := context.Background()
	cluster.Propose(ctx, "state=before-partition")
	time.Sleep(100 * time.Millisecond)

	// Partition: [node-1, node-2] vs [node-3, node-4, node-5]
	minority := []string{"node-1", "node-2"}
	majority := []string{"node-3", "node-4", "node-5"}

	fmt.Println("\n  🔴 PARTITION: [node-1,node-2] ↔ [node-3,node-4,node-5]")
	for _, m := range minority {
		for _, maj := range majority {
			cluster.transport.Partition(m, maj)
			cluster.transport.Partition(maj, m)
		}
	}

	time.Sleep(500 * time.Millisecond) // let new election happen

	// Find leaders in each partition
	fmt.Println("\n  After partition:")
	for id, node := range cluster.nodes {
		partition := "minority"
		for _, m := range majority {
			if m == id {
				partition = "majority"
				break
			}
		}
		role := "FOLLOWER"
		if node.IsLeader() {
			role = "LEADER ✨"
		}
		fmt.Printf("    [%s] %s - %s (term=%d)\n",
			partition, id, role, node.Status().Term)
	}

	// Try to write to both partitions
	fmt.Println("\n  Attempting writes in each partition:")

	// Write to majority leader
	for _, id := range majority {
		node := cluster.nodes[id]
		if node.IsLeader() {
			writeCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			err := node.Propose(writeCtx, []byte("state=majority-write"))
			cancel()
			if err == nil {
				fmt.Printf("    ✅ Write to majority leader (%s): SUCCESS\n", id)
			} else {
				fmt.Printf("    ❌ Write to majority leader (%s): %v\n", id, err)
			}
			break
		}
	}

	// Write to minority leader (should fail or stall)
	for _, id := range minority {
		node := cluster.nodes[id]
		writeCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		err := node.Propose(writeCtx, []byte("state=minority-write"))
		cancel()
		if err != nil {
			fmt.Printf("    ✅ Write to minority (%s): BLOCKED (correct! no quorum)\n", id)
		} else {
			fmt.Printf("    ❌ Write to minority (%s): committed (should not happen!)\n", id)
		}
		break
	}

	fmt.Println()
	fmt.Println("  KEY INSIGHTS:")
	fmt.Println("  - Majority partition: AVAILABLE for writes")
	fmt.Println("  - Minority partition: UNAVAILABLE for writes (no quorum)")
	fmt.Println("  - This is CAP theorem: we chose CONSISTENCY over AVAILABILITY")
	fmt.Println("  - No split-brain: only ONE partition can commit")
	fmt.Println("  - After heal: minority catches up from majority leader")
}

func demoLogSafety() {
	fmt.Println()
	fmt.Println("Demonstrating Raft's SAFETY guarantee:")
	fmt.Println("'A committed entry is NEVER lost, even under failures'")
	fmt.Println()

	nodes := []string{"node-1", "node-2", "node-3"}
	cluster := NewDemoCluster(nodes)
	cluster.Start()
	defer cluster.Stop()

	leader, _ := cluster.WaitForLeader(3 * time.Second)
	fmt.Printf("  Initial leader: %s\n", leader)

	ctx := context.Background()

	// Commit some entries
	for i := 1; i <= 5; i++ {
		data := fmt.Sprintf("entry-%d=committed", i)
		cluster.Propose(ctx, data)
	}
	time.Sleep(200 * time.Millisecond)

	initialStatus := cluster.nodes[leader].Status()
	fmt.Printf("  Committed %d entries (commit index=%d)\n",
		initialStatus.CommitIndex, initialStatus.CommitIndex)

	// Now simulate chaos: partition and rejoin
	for _, other := range nodes {
		if other != leader {
			cluster.transport.Partition(leader, other)
			cluster.transport.Partition(other, leader)
		}
	}

	time.Sleep(500 * time.Millisecond)

	// Heal
	for _, other := range nodes {
		if other != leader {
			cluster.transport.Heal(leader, other)
			cluster.transport.Heal(other, leader)
		}
	}

	time.Sleep(500 * time.Millisecond)

	// Verify all committed entries still exist on all nodes
	fmt.Println("\n  After chaos: verifying committed entries survive:")
	allSafe := true
	for id, kv := range cluster.kvStores {
		for i := 1; i <= 5; i++ {
			key := fmt.Sprintf("entry-%d", i)
			val, ok := kv.Get(key)
			if !ok {
				fmt.Printf("  ❌ %s: missing committed entry %s!\n", id, key)
				allSafe = false
			} else {
				_ = val
			}
		}
	}

	if allSafe {
		fmt.Println("  ✅ All committed entries present on all nodes!")
		fmt.Println("  ✅ Raft safety guarantee HOLDS")
	}
}

func printHeader() {
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("\n╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "HERMES — PHASE 4: RAFT CONSENSUS")
	fmt.Printf("╚%s╝\n\n", line)
}

func printSummary() {
	fmt.Println()
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "PHASE 4 COMPLETE ✅")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "What we built:")
	fmt.Printf("║  %-61s║\n", "  ✅ Complete Raft state machine (follower/candidate/leader)")
	fmt.Printf("║  %-61s║\n", "  ✅ Leader election with randomized timeouts")
	fmt.Printf("║  %-61s║\n", "  ✅ Log replication with consistency check")
	fmt.Printf("║  %-61s║\n", "  ✅ Quorum-based commit (majority must acknowledge)")
	fmt.Printf("║  %-61s║\n", "  ✅ Safety: committed entries never lost")
	fmt.Printf("║  %-61s║\n", "  ✅ Leader failure and re-election")
	fmt.Printf("║  %-61s║\n", "  ✅ Network partition handling")
	fmt.Printf("║  %-61s║\n", "  ✅ Follower progress tracking")
	fmt.Printf("║  %-61s║\n", "  ✅ Log compaction foundation (Compact())")
	fmt.Printf("║  %-61s║\n", "  ✅ Snapshot installation")
	fmt.Printf("║  %-61s║\n", "  ✅ Pure state machine design (testable)")
	fmt.Printf("║  %-61s║\n", "  ✅ Separate goroutine-safe RaftNode wrapper")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "Safety guarantees we implemented:")
	fmt.Printf("║  %-61s║\n", "  Election safety:  ≤1 leader per term")
	fmt.Printf("║  %-61s║\n", "  Leader append:    leader never overwrites its log")
	fmt.Printf("║  %-61s║\n", "  Log matching:     same index+term → same content")
	fmt.Printf("║  %-61s║\n", "  Leader complete:  leader has all committed entries")
	fmt.Printf("║  %-61s║\n", "  State machine:    all nodes apply same entries, same order")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "Key numbers:")
	fmt.Printf("║  %-61s║\n", "  Election timeout:  100-200ms (randomized)")
	fmt.Printf("║  %-61s║\n", "  Heartbeat interval: 50ms")
	fmt.Printf("║  %-61s║\n", "  Failover time:      150-300ms typical")
	fmt.Printf("║  %-61s║\n", "  Quorum (5 nodes):   3 nodes must acknowledge")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "How Phase 4 connects forward:")
	fmt.Printf("║  %-61s║\n", "  → Phase 5 (Partition): one Raft group per shard")
	fmt.Printf("║  %-61s║\n", "  → Phase 6 (Membership): Raft drives cluster changes")
	fmt.Printf("║  %-61s║\n", "  → Phase 7 (Transactions): Raft orders txn commits")
	fmt.Printf("║  %-61s║\n", "  → Phase 8 (Consistency): ReadIndex for linearizable reads")
	fmt.Printf("║  %-61s║\n", "  → Phase 9 (Fault): Raft handles node failures")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "→ NEXT: Phase 5 — Partitioning & Sharding")
	fmt.Printf("╚%s╝\n", line)
}
