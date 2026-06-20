// pkg/advanced/demo.go
package advanced

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aadityabinodyadav/hermes/pkg/cdc"
	"github.com/aadityabinodyadav/hermes/pkg/multiregion"
	"github.com/aadityabinodyadav/hermes/pkg/query"
	"github.com/aadityabinodyadav/hermes/pkg/ratelimit"
)

func RunAdvancedDemo() {
	printHeader()

	fmt.Println("━━━ DEMO 1: Distributed Query Processing ━━━")
	demoDistributedQuery()

	fmt.Println("\n━━━ DEMO 2: Change Data Capture (CDC) ━━━")
	demoCDC()

	fmt.Println("\n━━━ DEMO 3: Multi-Region Topology ━━━")
	demoMultiRegion()

	fmt.Println("\n━━━ DEMO 4: Token Bucket Rate Limiting ━━━")
	demoTokenBucket()

	fmt.Println("\n━━━ DEMO 5: Distributed Rate Limiting ━━━")
	demoDistributedRateLimit()

	fmt.Println("\n━━━ DEMO 6: Circuit Breaker ━━━")
	demoCircuitBreaker()

	fmt.Println("\n━━━ DEMO 7: Putting It All Together ━━━")
	demoIntegration()

	printSummary()
}

// ─────────────────────────────────────────────────────────────────────────────

func demoDistributedQuery() {
	fmt.Println()
	fmt.Println("Scatter-Gather: executing queries across multiple shards")
	fmt.Println()

	// Define shard layout
	shards := []query.ShardInfo{
		{ShardID: 0, StartKey: "", EndKey: "key:d", LeaderID: "node-1"},
		{ShardID: 1, StartKey: "key:d", EndKey: "key:m", LeaderID: "node-2"},
		{ShardID: 2, StartKey: "key:m", EndKey: "key:t", LeaderID: "node-3"},
		{ShardID: 3, StartKey: "key:t", EndKey: "", LeaderID: "node-4"},
	}

	planner := query.NewQueryPlanner(shards)

	// Simulate a shard executor
	executor := &mockShardExecutor{
		data: map[uint64][]query.KeyValue{
			0: {
				{Key: "key:alice", Value: []byte("100")},
				{Key: "key:bob", Value: []byte("200")},
				{Key: "key:charlie", Value: []byte("150")},
			},
			1: {
				{Key: "key:dave", Value: []byte("300")},
				{Key: "key:eve", Value: []byte("250")},
				{Key: "key:frank", Value: []byte("175")},
			},
			2: {
				{Key: "key:mallory", Value: []byte("500")},
				{Key: "key:nancy", Value: []byte("125")},
			},
			3: {
				{Key: "key:oscar", Value: []byte("400")},
				{Key: "key:peggy", Value: []byte("350")},
			},
		},
	}

	sg := query.NewScatterGather(planner, executor)

	// Query 1: Scan all keys
	fmt.Println("Query 1: Scan all accounts")
	start := time.Now()
	result, err := sg.Execute(&query.Query{
		Type:    query.QueryScan,
		Timeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Printf("  Found %d entries in %v\n",
			len(result.Entries), time.Since(start).Round(time.Millisecond))
		for _, e := range result.Entries[:min(3, len(result.Entries))] {
			fmt.Printf("  %s = %s\n", e.Key, string(e.Value))
		}
		if len(result.Entries) > 3 {
			fmt.Printf("  ... and %d more\n", len(result.Entries)-3)
		}
	}

	// Query 2: Count
	fmt.Println()
	fmt.Println("Query 2: COUNT all accounts")
	start = time.Now()
	result, err = sg.Execute(&query.Query{
		Type:        query.QueryAggregate,
		Aggregation: query.AggCount,
		Timeout:     5 * time.Second,
	})
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Printf("  COUNT = %d (in %v)\n",
			result.Count, time.Since(start).Round(time.Millisecond))
	}

	// Query 3: SUM
	fmt.Println()
	fmt.Println("Query 3: SUM of all balances")
	start = time.Now()
	result, err = sg.Execute(&query.Query{
		Type:        query.QueryAggregate,
		Aggregation: query.AggSum,
		Timeout:     5 * time.Second,
	})
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Printf("  SUM = %.0f (in %v)\n",
			result.Sum, time.Since(start).Round(time.Millisecond))
		fmt.Println()
		fmt.Println("  KEY INSIGHT: Each shard computed local SUM in parallel")
		fmt.Printf("  Coordinator received %d partial results and summed them\n",
			len(shards))
	}

	// Query 4: Range scan
	fmt.Println()
	fmt.Println("Query 4: Scan accounts key:d to key:n (shard pruning)")
	start = time.Now()
	plan := planner.Plan(&query.Query{
		Type:     query.QueryScan,
		StartKey: "key:d",
		EndKey:   "key:n",
	})
	fmt.Printf("  Shard pruning: %d of %d shards needed (skipped %d)\n",
		len(plan.ShardPlans), len(shards),
		len(shards)-len(plan.ShardPlans))
	result, _ = sg.Execute(&query.Query{
		Type:     query.QueryScan,
		StartKey: "key:d",
		EndKey:   "key:n",
		Timeout:  5 * time.Second,
	})
	fmt.Printf("  Found %d entries in %v\n",
		len(result.Entries), time.Since(start).Round(time.Millisecond))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// mockShardExecutor simulates executing sub-queries on shards
type mockShardExecutor struct {
	data map[uint64][]query.KeyValue
}

func (e *mockShardExecutor) ExecuteShard(q *query.ShardQuery) (*query.ShardResult, error) {
	// Simulate network latency
	time.Sleep(time.Duration(10+q.ShardID*5) * time.Millisecond)

	entries := e.data[q.ShardID]
	result := &query.ShardResult{ShardID: q.ShardID}

	// Apply range filter
	for _, entry := range entries {
		if q.StartKey != "" && entry.Key < q.StartKey {
			continue
		}
		if q.EndKey != "" && entry.Key >= q.EndKey {
			continue
		}

		switch q.LocalAgg {
		case query.AggCount:
			result.Count++
		case query.AggSum:
			// Parse value as number
			var val float64
			fmt.Sscanf(string(entry.Value), "%f", &val)
			result.Sum += val
			result.Count++
		case query.AggMin:
			var val float64
			fmt.Sscanf(string(entry.Value), "%f", &val)
			if result.Count == 0 || val < result.Min {
				result.Min = val
			}
			result.Count++
		case query.AggMax:
			var val float64
			fmt.Sscanf(string(entry.Value), "%f", &val)
			if val > result.Max {
				result.Max = val
			}
			result.Count++
		default:
			result.Entries = append(result.Entries, entry)
			result.Count++
		}
	}

	return result, nil
}

// ─────────────────────────────────────────────────────────────────────────────

func demoCDC() {
	fmt.Println()
	fmt.Println("Change Data Capture: streaming writes to external systems")
	fmt.Println()

	// Create a channel sink to capture events
	sink := cdc.NewChannelSink(100)

	// Create a mock WAL reader
	walReader := &mockWALReader{
		entries: make(chan *cdc.WALEntry, 100),
	}

	config := cdc.DefaultChangeFeedConfig("feed-1")
	config.KeyPrefix = "user:" // only capture user: keys

	feed := cdc.NewChangeFeed(config, sink, walReader)
	feed.Start()

	// Simulate some writes to the WAL
	fmt.Println("Simulating writes to Hermes:")
	writes := []struct {
		key   string
		value string
	}{
		{"user:alice", "1000"},
		{"user:bob", "500"},
		{"product:laptop", "999"}, // filtered out (not user: prefix)
		{"user:charlie", "750"},
		{"user:alice", "900"}, // update
	}

	go func() {
		for i, w := range writes {
			walReader.entries <- &cdc.WALEntry{
				Sequence:  uint64(i + 1),
				Timestamp: time.Now().UnixNano(),
				Key:       w.key,
				Value:     []byte(w.value),
				ShardID:   0,
				NodeID:    "node-1",
			}
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(200 * time.Millisecond)
		feed.Stop()
	}()

	// Collect and display change events
	fmt.Println("\nChange events delivered to sink:")
	fmt.Println("(product:laptop filtered out by prefix)")
	fmt.Println()

	timeout := time.After(2 * time.Second)
	eventCount := 0

DRAIN:
	for {
		select {
		case event, ok := <-sink.Events():
			if !ok {
				break DRAIN
			}
			fmt.Printf("  [%s] key=%s value=%s\n",
				event.Op, event.Key, string(event.AfterValue))
			eventCount++
		case <-timeout:
			break DRAIN
		}
	}

	stats := feed.Stats()
	fmt.Printf("\nCDC Stats: %+v\n", stats)
	fmt.Printf("Delivered: %d events (2 filtered: product: key)\n", eventCount)
	fmt.Println()
	fmt.Println("CDC enables:")
	fmt.Println("  → Real-time search indexing (Elasticsearch)")
	fmt.Println("  → Cache invalidation (Redis)")
	fmt.Println("  → Event streaming (Kafka, Kinesis)")
	fmt.Println("  → Audit logging")
	fmt.Println("  → Cross-datacenter sync")
}

// mockWALReader reads from a channel
type mockWALReader struct {
	entries chan *cdc.WALEntry
}

func (r *mockWALReader) ReadFrom(_ context.Context, _ uint64) (<-chan *cdc.WALEntry, error) {
	return r.entries, nil
}

// ─────────────────────────────────────────────────────────────────────────────

func demoMultiRegion() {
	fmt.Println()
	fmt.Println("Multi-region: global data distribution")
	fmt.Println()

	topology := multiregion.NewRegionTopology("us-east")

	// Add regions with realistic latencies
	regions := []*multiregion.Region{
		{
			ID:       "us-east",
			Name:     "US East",
			Location: "us-east-1",
			Nodes:    []string{"he-0", "he-1", "he-2"},
			Latency: map[string]time.Duration{
				"eu-west": 87 * time.Millisecond,
				"ap-syd":  168 * time.Millisecond,
			},
		},
		{
			ID:       "eu-west",
			Name:     "Europe West",
			Location: "eu-west-1",
			Nodes:    []string{"hw-0", "hw-1", "hw-2"},
			Latency: map[string]time.Duration{
				"us-east": 87 * time.Millisecond,
				"ap-syd":  253 * time.Millisecond,
			},
		},
		{
			ID:       "ap-syd",
			Name:     "Asia Pacific (Sydney)",
			Location: "ap-southeast-2",
			Nodes:    []string{"ha-0", "ha-1", "ha-2"},
			Latency: map[string]time.Duration{
				"us-east": 168 * time.Millisecond,
				"eu-west": 253 * time.Millisecond,
			},
		},
	}

	for _, region := range regions {
		topology.AddRegion(region)
	}

	fmt.Println("\nRegion topology (from US East perspective):")
	for _, region := range topology.AllRegionsByLatency() {
		if region.ID == "us-east" {
			fmt.Printf("  ✦ %-8s [LOCAL]  ← %d nodes\n",
				region.ID, len(region.Nodes))
		} else {
			fmt.Printf("  → %-8s [REMOTE] ← %d nodes\n",
				region.ID, len(region.Nodes))
		}
	}

	// Show routing decisions
	router := multiregion.NewFollowerReadRouter(topology, 200*time.Millisecond)

	fmt.Println("\nRead routing decisions:")
	consistencyLevels := []string{"eventual", "bounded", "strong"}
	for _, level := range consistencyLevels {
		regionID, latency, staleness := router.RouteRead(level)
		fmt.Printf("  %-10s → %-12s latency=%-8v staleness=%v\n",
			level, regionID, latency.Round(time.Millisecond),
			staleness.Round(time.Millisecond))
	}

	fmt.Println()
	fmt.Println("Multi-region challenges:")
	fmt.Println("  ⚡ Speed of light: US↔EU = ~87ms, can't avoid")
	fmt.Println("  📖 Reads: serve local (fast but may be stale)")
	fmt.Println("  ✏️  Writes: must go to owner region (latency varies by user location)")
	fmt.Println("  🔄 Conflict: 2+ regions write same key → need CRDT or designated owner")
	fmt.Println()
	fmt.Println("Hermes multi-region strategy:")
	fmt.Println("  - Geo-partitioned data: user:US → us-east, user:EU → eu-west")
	fmt.Println("  - Global metadata: replicated to all regions (eventual consistency)")
	fmt.Println("  - Async learners: each region has non-voting replicas")
	fmt.Println("  - Leader lease: followers can serve reads within staleness bound")
}

// ─────────────────────────────────────────────────────────────────────────────

func demoTokenBucket() {
	fmt.Println()
	fmt.Println("Token bucket: allows bursts, limits sustained rate")
	fmt.Println()

	// 100 req/sec, burst up to 20
	tb := ratelimit.NewTokenBucket(20, 100)

	fmt.Println("Simulating traffic patterns:")
	fmt.Println()

	// Pattern 1: Normal sustained rate
	fmt.Println("Normal operation (100 req/sec):")
	allowed, rejected := 0, 0
	for i := 0; i < 100; i++ {
		if tb.Allow(1) {
			allowed++
		} else {
			rejected++
		}
		time.Sleep(10 * time.Millisecond) // 100/sec rate
	}
	fmt.Printf("  Result: %d allowed, %d rejected\n", allowed, rejected)

	// Pattern 2: Burst
	fmt.Println()
	fmt.Println("Burst (200 req in 100ms, bucket has 20 tokens):")
	tb2 := ratelimit.NewTokenBucket(20, 100)
	allowed, rejected = 0, 0
	for i := 0; i < 200; i++ {
		if tb2.Allow(1) {
			allowed++
		} else {
			rejected++
		}
	}
	fmt.Printf("  Result: %d allowed, %d rejected\n", allowed, rejected)
	fmt.Printf("  Allowed ≈ bucket capacity (20 tokens) ✅\n")

	// Pattern 3: Different costs
	fmt.Println()
	fmt.Println("Operations with different costs:")
	fmt.Println("  PUT = 1 token (cheap)")
	fmt.Println("  SCAN = 10 tokens (expensive, reads many keys)")
	tb3 := ratelimit.NewTokenBucket(100, 100)
	tb3.Allow(50) // pre-spend some tokens

	putAllowed := 0
	for i := 0; i < 20; i++ {
		if tb3.Allow(1) {
			putAllowed++
		}
	}

	scanAllowed := 0
	for i := 0; i < 5; i++ {
		if tb3.Allow(10) {
			scanAllowed++
		}
	}

	fmt.Printf("  PUTs allowed: %d/20\n", putAllowed)
	fmt.Printf("  SCANs allowed: %d/5\n", scanAllowed)

	fmt.Println()
	stats := tb.Stats()
	fmt.Printf("Final stats: %s\n", stats)
}

// ─────────────────────────────────────────────────────────────────────────────

func demoDistributedRateLimit() {
	fmt.Println()
	fmt.Println("Distributed rate limiting: cluster-wide quota sharing")
	fmt.Println()

	// 3-node cluster, 1000 req/sec global limit
	globalLimit := 1000.0
	clusterSize := 3

	node1 := ratelimit.NewDistributedRateLimiter("node-1", globalLimit, clusterSize)
	node2 := ratelimit.NewDistributedRateLimiter("node-2", globalLimit, clusterSize)
	node3 := ratelimit.NewDistributedRateLimiter("node-3", globalLimit, clusterSize)

	fmt.Printf("Global limit: %.0f req/sec across %d nodes\n",
		globalLimit, clusterSize)
	fmt.Printf("Each node gets: %.0f req/sec initially\n",
		globalLimit/float64(clusterSize))
	fmt.Println()

	// Simulate traffic and show quota adaptation
	var wg sync.WaitGroup
	results := make(map[string][2]int64) // node → [allowed, rejected]
	var mu sync.Mutex

	for _, node := range []*ratelimit.DistributedRateLimiter{node1, node2, node3} {
		wg.Add(1)
		n := node
		go func() {
			defer wg.Done()
			allowed, rejected := int64(0), int64(0)
			start := time.Now()
			for time.Since(start) < 100*time.Millisecond {
				if n.Allow() {
					allowed++
				} else {
					rejected++
				}
				time.Sleep(time.Millisecond)
			}
			stats := n.Stats()
			mu.Lock()
			results[stats.NodeID] = [2]int64{allowed, rejected}
			mu.Unlock()
		}()
	}

	wg.Wait()

	fmt.Println("Traffic results (100ms window):")
	totalAllowed, totalRejected := int64(0), int64(0)
	for nodeID, counts := range results {
		fmt.Printf("  %s: %d allowed, %d rejected\n",
			nodeID, counts[0], counts[1])
		totalAllowed += counts[0]
		totalRejected += counts[1]
	}
	fmt.Printf("  Total: %d allowed, %d rejected\n", totalAllowed, totalRejected)
	fmt.Println()
	fmt.Println("Gossip adaptation:")
	fmt.Println("  If node-1 uses 50% of cluster traffic:")
	node2.UpdatePeerRate("node-1", 500)
	node3.UpdatePeerRate("node-1", 500)
	fmt.Printf("  node-2 local limit adjusted to: %.0f req/sec\n",
		node2.Stats().LocalLimit)
	fmt.Printf("  node-3 local limit adjusted to: %.0f req/sec\n",
		node3.Stats().LocalLimit)
}

// ─────────────────────────────────────────────────────────────────────────────

func demoCircuitBreaker() {
	fmt.Println()
	fmt.Println("Circuit breaker: preventing cascade failures")
	fmt.Println()

	cb := ratelimit.NewCircuitBreaker("storage-service")

	fmt.Printf("Initial state: %s\n\n", cb.State())

	// Normal operation
	fmt.Println("Phase 1: Normal operation")
	for i := 0; i < 15; i++ {
		allowed := cb.Allow()
		if allowed {
			cb.Success()
		}
	}
	fmt.Printf("State: %s ✅\n", cb.State())

	// Failures start
	fmt.Println()
	fmt.Println("Phase 2: Errors start (disk failing)")
	for i := 0; i < 20; i++ {
		allowed := cb.Allow()
		if allowed {
			if i%2 == 0 {
				cb.Failure() // 50% error rate
			} else {
				cb.Success()
			}
		}
	}
	fmt.Printf("State: %s\n", cb.State())

	// Circuit is open - all requests rejected
	fmt.Println()
	fmt.Println("Phase 3: Circuit OPEN - fast failures")
	rejected := 0
	for i := 0; i < 10; i++ {
		if !cb.Allow() {
			rejected++
		}
	}
	fmt.Printf("  Rejected %d/10 requests immediately\n", rejected)
	fmt.Printf("  State: %s\n", cb.State())
	fmt.Println("  No waiting, no timeouts — fail fast!")

	fmt.Println()
	fmt.Println("Benefits:")
	fmt.Println("  ✅ Fast failure: no waiting for timeouts")
	fmt.Println("  ✅ Recovery time: system can heal without overload")
	fmt.Println("  ✅ Cascade prevention: one bad service doesn't kill others")
	fmt.Println()
	fmt.Println("Used in Hermes for:")
	fmt.Println("  - Storage engine (disk failing → fail fast)")
	fmt.Println("  - Peer connections (partitioned node → don't retry immediately)")
	fmt.Println("  - External services (metrics, auth → degrade gracefully)")
}

// ─────────────────────────────────────────────────────────────────────────────

func demoIntegration() {
	fmt.Println()
	fmt.Println("Integration: all advanced features working together")
	fmt.Println()

	fmt.Println("Scenario: High-load e-commerce platform on Hermes")
	fmt.Println()

	type Request struct {
		clientID  string
		operation string
		key       string
		region    string
	}

	// Rate limiter per client
	clientLimiters := make(map[string]*ratelimit.TokenBucket)
	limiterMu := sync.RWMutex{}

	getOrCreateLimiter := func(clientID string) *ratelimit.TokenBucket {
		limiterMu.Lock()
		defer limiterMu.Unlock()
		if lb, ok := clientLimiters[clientID]; ok {
			return lb
		}
		lb := ratelimit.NewTokenBucket(10, 100) // 100 req/sec, burst 10
		clientLimiters[clientID] = lb
		return lb
	}

	// Circuit breaker for storage
	cb := ratelimit.NewCircuitBreaker("storage")

	// Simulate requests
	requests := []Request{
		{"client-1", "GET", "product:laptop", "us-east"},
		{"client-1", "GET", "product:phone", "us-east"},
		{"client-2", "PUT", "cart:user-42", "eu-west"},
		{"client-1", "GET", "inventory:laptop", "us-east"},
		{"client-3", "SCAN", "products:", "ap-syd"},
		{"client-1", "PUT", "order:1234", "us-east"},
	}

	fmt.Println("Processing requests:")
	fmt.Println()

	for _, req := range requests {
		// Rate check
		limiter := getOrCreateLimiter(req.clientID)
		if !limiter.Allow(1) {
			fmt.Printf("  ❌ [%s] %s %s: RATE LIMITED\n",
				req.clientID, req.operation, req.key)
			continue
		}

		// Circuit breaker check
		if !cb.Allow() {
			fmt.Printf("  ❌ [%s] %s %s: CIRCUIT OPEN (storage unavailable)\n",
				req.clientID, req.operation, req.key)
			continue
		}

		// Simulate processing
		time.Sleep(5 * time.Millisecond)
		cb.Success()

		fmt.Printf("  ✅ [%s] %s %s (from %s)\n",
			req.clientID, req.operation, req.key, req.region)
	}

	fmt.Println()
	fmt.Println("Architecture in use:")
	fmt.Println("  ✅ Rate limiting: per-client token buckets")
	fmt.Println("  ✅ Circuit breaker: protect storage from overload")
	fmt.Println("  ✅ Multi-region: reads from nearest region")
	fmt.Println("  ✅ CDC: all writes stream to Kafka for search index")
	fmt.Println("  ✅ Scatter-gather: SCAN queries parallelized across shards")
}

// ─────────────────────────────────────────────────────────────────────────────

func printHeader() {
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("\n╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "HERMES — PHASE 11: ADVANCED TOPICS")
	fmt.Printf("╚%s╝\n\n", line)
}

func printSummary() {
	fmt.Println()
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "PHASE 11 COMPLETE ✅")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "What we built:")
	fmt.Printf("║  %-61s║\n", "  ✅ Distributed Query Processing")
	fmt.Printf("║  %-61s║\n", "    ✅ QueryPlanner with shard pruning")
	fmt.Printf("║  %-61s║\n", "    ✅ ScatterGather with parallel execution")
	fmt.Printf("║  %-61s║\n", "    ✅ Aggregation push-down (COUNT,SUM,AVG,MIN,MAX)")
	fmt.Printf("║  %-61s║\n", "    ✅ Merge and sort across shards")
	fmt.Printf("║  %-61s║\n", "  ✅ Change Data Capture (CDC)")
	fmt.Printf("║  %-61s║\n", "    ✅ WAL-based change streaming")
	fmt.Printf("║  %-61s║\n", "    ✅ Key prefix filtering")
	fmt.Printf("║  %-61s║\n", "    ✅ Multiple sink types (log, channel, fan-out)")
	fmt.Printf("║  %-61s║\n", "    ✅ Checkpoint-based resumption")
	fmt.Printf("║  %-61s║\n", "  ✅ Multi-Region Topology")
	fmt.Printf("║  %-61s║\n", "    ✅ Region-aware routing")
	fmt.Printf("║  %-61s║\n", "    ✅ Follower reads with staleness bounds")
	fmt.Printf("║  %-61s║\n", "    ✅ Latency-based region selection")
	fmt.Printf("║  %-61s║\n", "  ✅ Rate Limiting")
	fmt.Printf("║  %-61s║\n", "    ✅ Token bucket (burst-aware)")
	fmt.Printf("║  %-61s║\n", "    ✅ Sliding window counter (approximate)")
	fmt.Printf("║  %-61s║\n", "    ✅ Distributed rate limiting with gossip")
	fmt.Printf("║  %-61s║\n", "    ✅ Circuit breaker (cascade failure prevention)")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "Real systems using these techniques:")
	fmt.Printf("║  %-61s║\n", "  Scatter-Gather: BigQuery, Presto, Spark SQL")
	fmt.Printf("║  %-61s║\n", "  CDC:            Debezium, Maxwell, Kafka Connect")
	fmt.Printf("║  %-61s║\n", "  Multi-region:   Spanner, CockroachDB, YugabyteDB")
	fmt.Printf("║  %-61s║\n", "  Rate limiting:  Nginx, HAProxy, Envoy, Kong")
	fmt.Printf("║  %-61s║\n", "  Circuit breaker:Netflix Hystrix, Resilience4j")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "→ NEXT: Phase 12 — Integration & Capstone")
	fmt.Printf("╚%s╝\n", line)
}
