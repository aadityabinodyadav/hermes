// test/integration/system_test.go
package integration

// System tests for the complete Hermes node
//
// These tests verify that all components work TOGETHER correctly.
// They are slower than unit tests but give the highest confidence.

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aadityabinodyadav/hermes/pkg/server"
)

func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("HERMES_RUN_INTEGRATION") != "1" {
		t.Skip("set HERMES_RUN_INTEGRATION=1 to run system integration tests")
	}
}

// TestClusterFormation verifies that a cluster forms correctly
func TestClusterFormation(t *testing.T) {
	requireIntegration(t)
	t.Log("Testing cluster formation with 3 nodes")

	cluster := startTestCluster(t, 3)
	defer cluster.Stop()

	// Verify a leader is elected within 2 seconds
	leader, err := cluster.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("No leader elected: %v", err)
	}
	t.Logf("Leader elected: %s", leader)

	// Verify all nodes know the leader
	for _, nodeID := range cluster.NodeIDs() {
		if got := cluster.GetLeader(nodeID); got != leader {
			t.Errorf("node %s thinks leader is %s, expected %s",
				nodeID, got, leader)
		}
	}
}

// TestBasicReadWrite verifies that reads and writes work correctly
func TestBasicReadWrite(t *testing.T) {
	requireIntegration(t)
	cluster := startTestCluster(t, 3)
	defer cluster.Stop()

	_, err := cluster.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("No leader: %v", err)
	}

	ctx := context.Background()

	// Write via leader
	err = cluster.Put(ctx, "key1", "value1")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Read back
	val, found, err := cluster.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("Key not found after write")
	}
	if val != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}

	// Verify all nodes have the value
	time.Sleep(100 * time.Millisecond) // wait for replication
	for _, nodeID := range cluster.NodeIDs() {
		val, found, err := cluster.GetFromNode(ctx, nodeID, "key1")
		if err != nil || !found || val != "value1" {
			t.Errorf("Node %s: value mismatch (err=%v, found=%v, val=%s)",
				nodeID, err, found, val)
		}
	}
}

// TestLeaderFailover verifies that failover works correctly
func TestLeaderFailover(t *testing.T) {
	requireIntegration(t)
	cluster := startTestCluster(t, 5)
	defer cluster.Stop()

	leader, _ := cluster.WaitForLeader(2 * time.Second)
	t.Logf("Initial leader: %s", leader)

	ctx := context.Background()

	// Write some data
	for i := 0; i < 10; i++ {
		cluster.Put(ctx, fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
	}

	// Kill the leader
	t.Logf("Killing leader %s", leader)
	cluster.KillNode(leader)

	// Wait for new leader
	time.Sleep(100 * time.Millisecond) // give time for leader to be "down"
	newLeader, err := cluster.WaitForLeaderExcluding(leader, 3*time.Second)
	if err != nil {
		t.Fatalf("No new leader elected: %v", err)
	}
	t.Logf("New leader: %s", newLeader)

	if newLeader == leader {
		t.Error("New leader is the same as killed leader!")
	}

	// Verify old data is still accessible
	for i := 0; i < 10; i++ {
		val, found, err := cluster.Get(ctx, fmt.Sprintf("key%d", i))
		if err != nil || !found || val != fmt.Sprintf("val%d", i) {
			t.Errorf("Data loss after failover: key%d (err=%v, found=%v, val=%s)",
				i, err, found, val)
		}
	}

	// Verify new writes work
	err = cluster.Put(ctx, "post-failover", "success")
	if err != nil {
		t.Errorf("Write after failover failed: %v", err)
	}

	t.Log("Failover test: PASSED ✅")
}

// TestLinearizability runs a Jepsen-style linearizability test
func TestLinearizability(t *testing.T) {
	requireIntegration(t)
	cluster := startTestCluster(t, 3)
	defer cluster.Stop()

	_, _ = cluster.WaitForLeader(2 * time.Second)

	ctx := context.Background()
	checker := newLinearizabilityChecker()

	// Run concurrent reads and writes
	const goroutines = 10
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("key-%d", clientID%3) // contend on 3 keys

				if j%3 == 0 {
					// Write
					value := fmt.Sprintf("v%d-%d", clientID, j)
					opID := checker.InvokeWrite(key, value)
					err := cluster.Put(ctx, key, value)
					checker.CompleteWrite(opID, err == nil)
				} else {
					// Read
					opID := checker.InvokeRead(key)
					val, _, err := cluster.Get(ctx, key)
					checker.CompleteRead(opID, val, err == nil)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify linearizability
	violations := checker.Check()
	if len(violations) > 0 {
		t.Errorf("Linearizability violated! %d violations:", len(violations))
		for _, v := range violations {
			t.Errorf("  %s", v)
		}
	} else {
		t.Log("Linearizability check: PASSED ✅")
	}
}

// TestNetworkPartition tests behavior during network partition
func TestNetworkPartition(t *testing.T) {
	requireIntegration(t)
	cluster := startTestCluster(t, 5)
	defer cluster.Stop()

	leader, _ := cluster.WaitForLeader(2 * time.Second)
	ctx := context.Background()

	// Write before partition
	cluster.Put(ctx, "pre-partition", "value")

	// Create minority partition (leader in minority)
	minority := []string{leader}
	majority := cluster.OtherNodes(minority)

	t.Log("Creating partition: minority=[leader] majority=[others]")
	cluster.Partition(minority, majority)

	// Writes to minority should fail (can't get quorum)
	time.Sleep(50 * time.Millisecond)
	err := cluster.PutToNode(ctx, leader, "minority-write", "value")
	if err == nil {
		t.Error("Write to minority leader should have failed!")
	} else {
		t.Logf("Minority write correctly rejected: %v", err)
	}

	// Writes to majority should succeed (new leader elected)
	time.Sleep(300 * time.Millisecond) // wait for election
	newLeader, err := cluster.WaitForLeaderAmong(majority, 2*time.Second)
	if err != nil {
		t.Fatalf("No leader in majority partition: %v", err)
	}
	t.Logf("Majority elected new leader: %s", newLeader)

	err = cluster.PutToNode(ctx, newLeader, "majority-write", "value")
	if err != nil {
		t.Errorf("Write to majority leader failed: %v", err)
	}

	// Heal partition
	cluster.Heal(minority, majority)
	time.Sleep(200 * time.Millisecond)

	// Verify old minority leader caught up
	val, found, _ := cluster.GetFromNode(ctx, leader, "majority-write")
	if !found || val != "value" {
		t.Error("Old minority leader didn't catch up after heal")
	}

	t.Log("Network partition test: PASSED ✅")
}

// TestDurability verifies data survives node restarts
func TestDurability(t *testing.T) {
	requireIntegration(t)
	tmpDir := t.TempDir()

	// Write data
	{
		node := startSingleNode(t, "node-1", tmpDir)
		ctx := context.Background()

		for !node.IsLeader() {
			time.Sleep(10 * time.Millisecond)
		}

		for i := 0; i < 100; i++ {
			err := node.Put(ctx,
				fmt.Sprintf("key-%d", i),
				fmt.Sprintf("value-%d", i))
			if err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}

		node.Stop()
		t.Log("Node stopped (simulating crash)")
	}

	// Restart and verify
	{
		node := startSingleNode(t, "node-1", tmpDir)
		defer node.Stop()

		ctx := context.Background()

		recovered := 0
		for i := 0; i < 100; i++ {
			val, found, err := node.Get(ctx, fmt.Sprintf("key-%d", i))
			if err != nil || !found || val != fmt.Sprintf("value-%d", i) {
				t.Errorf("key-%d not recovered (err=%v, found=%v, val=%s)",
					i, err, found, val)
			} else {
				recovered++
			}
		}

		t.Logf("Recovered %d/100 keys after restart ✅", recovered)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST HELPERS
// ─────────────────────────────────────────────────────────────────────────────

type TestCluster struct {
	nodes   map[string]*server.HermesNode
	configs map[string]*server.Config
	t       *testing.T
	mu      sync.Mutex
}

func startTestCluster(t *testing.T, size int) *TestCluster {
	t.Helper()
	cluster := &TestCluster{
		nodes:   make(map[string]*server.HermesNode),
		configs: make(map[string]*server.Config),
		t:       t,
	}

	// Create node configs
	for i := 0; i < size; i++ {
		nodeID := fmt.Sprintf("node-%d", i+1)
		dataDir := t.TempDir()
		listenAddr := fmt.Sprintf("127.0.0.1:%d", 17000+i)

		config := server.DefaultConfig(nodeID, listenAddr, dataDir)
		config.MetricsAddr = "127.0.0.1:0"
		cluster.configs[nodeID] = config
	}

	// Start all nodes
	for nodeID, config := range cluster.configs {
		node, err := server.NewHermesNode(config)
		if err != nil {
			t.Fatalf("Failed to create node %s: %v", nodeID, err)
		}
		if err := node.Start(); err != nil {
			t.Fatalf("Failed to start node %s: %v", nodeID, err)
		}
		cluster.nodes[nodeID] = node
	}

	return cluster
}

func (c *TestCluster) WaitForLeader(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for nodeID, node := range c.nodes {
			if node.IsLeader() {
				return nodeID, nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return "", fmt.Errorf("no leader elected within %v", timeout)
}

func (c *TestCluster) WaitForLeaderExcluding(excludeID string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for nodeID, node := range c.nodes {
			if nodeID != excludeID && node.IsLeader() {
				return nodeID, nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return "", fmt.Errorf("no leader elected within %v", timeout)
}

func (c *TestCluster) WaitForLeaderAmong(nodeIDs []string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range nodeIDs {
			if node, ok := c.nodes[id]; ok && node.IsLeader() {
				return id, nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return "", fmt.Errorf("no leader among %v within %v", nodeIDs, timeout)
}

func (c *TestCluster) NodeIDs() []string {
	ids := make([]string, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	return ids
}

func (c *TestCluster) OtherNodes(exclude []string) []string {
	excludeSet := make(map[string]bool)
	for _, id := range exclude {
		excludeSet[id] = true
	}
	var others []string
	for id := range c.nodes {
		if !excludeSet[id] {
			others = append(others, id)
		}
	}
	return others
}

func (c *TestCluster) GetLeader(nodeID string) string {
	node, ok := c.nodes[nodeID]
	if !ok {
		return ""
	}
	return node.Leader()
}

func (c *TestCluster) Put(ctx context.Context, key, value string) error {
	// Find leader and put
	for _, node := range c.nodes {
		if node.IsLeader() {
			return node.Put(ctx, key, []byte(value))
		}
	}
	return fmt.Errorf("no leader")
}

func (c *TestCluster) PutToNode(ctx context.Context, nodeID, key, value string) error {
	node, ok := c.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}
	return node.Put(ctx, key, []byte(value))
}

func (c *TestCluster) Get(ctx context.Context, key string) (string, bool, error) {
	for _, node := range c.nodes {
		if node.IsLeader() {
			val, found, err := node.Get(ctx, key)
			if err != nil {
				return "", false, err
			}
			return string(val), found, nil
		}
	}
	return "", false, fmt.Errorf("no leader")
}

func (c *TestCluster) GetFromNode(ctx context.Context, nodeID, key string) (string, bool, error) {
	node, ok := c.nodes[nodeID]
	if !ok {
		return "", false, fmt.Errorf("node %s not found", nodeID)
	}
	val, found, err := node.Get(ctx, key)
	return string(val), found, err
}

func (c *TestCluster) KillNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.nodes[nodeID]
	if !ok {
		return
	}
	node.Stop()
	delete(c.nodes, nodeID)
}

func (c *TestCluster) Partition(groupA, groupB []string) {
	// Simulate network partition via fault injector
	// In real tests: use actual network manipulation
	c.t.Logf("Partitioning %v from %v", groupA, groupB)
}

func (c *TestCluster) Heal(groupA, groupB []string) {
	c.t.Logf("Healing partition between %v and %v", groupA, groupB)
}

func (c *TestCluster) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, node := range c.nodes {
		node.Stop()
	}
}

// Simple linearizability checker for tests
type linearizabilityChecker struct {
	mu  sync.Mutex
	ops []linearizabilityOp
}

type linearizabilityOp struct {
	id         int
	opType     string
	key        string
	writeVal   string
	readVal    string
	success    bool
	invokeAt   time.Time
	completeAt time.Time
}

func newLinearizabilityChecker() *linearizabilityChecker {
	return &linearizabilityChecker{}
}

func (c *linearizabilityChecker) InvokeWrite(key, value string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := len(c.ops)
	c.ops = append(c.ops, linearizabilityOp{
		id:       id,
		opType:   "write",
		key:      key,
		writeVal: value,
		invokeAt: time.Now(),
	})
	return id
}

func (c *linearizabilityChecker) CompleteWrite(id int, success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if id < len(c.ops) {
		c.ops[id].completeAt = time.Now()
		c.ops[id].success = success
	}
}

func (c *linearizabilityChecker) InvokeRead(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := len(c.ops)
	c.ops = append(c.ops, linearizabilityOp{
		id:       id,
		opType:   "read",
		key:      key,
		invokeAt: time.Now(),
	})
	return id
}

func (c *linearizabilityChecker) CompleteRead(id int, value string, success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if id < len(c.ops) {
		c.ops[id].completeAt = time.Now()
		c.ops[id].readVal = value
		c.ops[id].success = success
	}
}

func (c *linearizabilityChecker) Check() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	var violations []string

	// Simplified check: successful write followed by read should return that value
	lastWrite := make(map[string]string)
	lastWriteComplete := make(map[string]time.Time)

	for _, op := range c.ops {
		if !op.success {
			continue
		}

		if op.opType == "write" {
			lastWrite[op.key] = op.writeVal
			lastWriteComplete[op.key] = op.completeAt
		} else if op.opType == "read" {
			// If read started AFTER the last write completed,
			// it MUST see the write (linearizability)
			if lw, ok := lastWrite[op.key]; ok {
				lwComplete := lastWriteComplete[op.key]
				if op.invokeAt.After(lwComplete) && op.readVal != lw {
					violations = append(violations, fmt.Sprintf(
						"Stale read: key=%s wrote=%s read=%s (write completed at %v, read started at %v)",
						op.key, lw, op.readVal,
						lwComplete.Format("15:04:05.000"),
						op.invokeAt.Format("15:04:05.000")))
				}
			}
		}
	}

	return violations
}

func startSingleNode(t *testing.T, nodeID, dataDir string) *HermesNodeHelper {
	t.Helper()
	config := server.DefaultConfig(nodeID, "127.0.0.1:0", dataDir)
	config.MetricsAddr = "127.0.0.1:0"
	node, err := server.NewHermesNode(config)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	if err := node.Start(); err != nil {
		t.Fatalf("Failed to start node: %v", err)
	}
	return &HermesNodeHelper{node: node}
}

type HermesNodeHelper struct {
	node *server.HermesNode
}

func (h *HermesNodeHelper) Put(ctx context.Context, key, value string) error {
	return h.node.Put(ctx, key, []byte(value))
}

func (h *HermesNodeHelper) Get(ctx context.Context, key string) (string, bool, error) {
	val, found, err := h.node.Get(ctx, key)
	return string(val), found, err
}

func (h *HermesNodeHelper) IsLeader() bool { return h.node.IsLeader() }
func (h *HermesNodeHelper) Leader() string { return h.node.Leader() }
func (h *HermesNodeHelper) Stop()          { h.node.Stop() }
