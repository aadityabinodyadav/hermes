package os_fundamentals

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Pattern 1: Worker Pool
// Used in Hermes for: processing incoming gRPC requests, compaction workers
type WorkerPool struct {
	tasks   chan func()
	wg      sync.WaitGroup
	workers int
}

func NewWorkerPool(workers int, queueSize int) *WorkerPool {
	pool := &WorkerPool{
		tasks:   make(chan func(), queueSize),
		workers: workers,
	}
	pool.start()
	return pool
}

func (p *WorkerPool) start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go func(workerID int) {
			defer p.wg.Done()
			for task := range p.tasks {
				task()
			}
		}(i)
	}
}

func (p *WorkerPool) Submit(task func()) {
	p.tasks <- task
}

func (p *WorkerPool) Close() {
	close(p.tasks)
	p.wg.Wait()
}

// Pattern 2: Done Channel / Context Cancellation
// Used in Hermes for: graceful shutdown, request timeouts, leader step-down
func ContextCancellationDemo() {
	fmt.Println("=== CONTEXT CANCELLATION (Critical for Distributed Systems) ===")
	fmt.Println()
	fmt.Println("Every operation in Hermes has a deadline.")
	fmt.Println("If a Raft vote doesn't come back in time, we move on.")
	fmt.Println()

	// Simulate a Raft vote request with timeout
	voteWithTimeout := func(peer string, timeout time.Duration) (bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		resultCh := make(chan bool, 1)

		go func() {
			// Simulate network round trip to peer
			networkDelay := time.Duration(50+len(peer)*10) * time.Millisecond
			select {
			case <-time.After(networkDelay):
				resultCh <- true // vote granted
			case <-ctx.Done():
				// Request cancelled before we could even send
			}
		}()

		select {
		case granted := <-resultCh:
			return granted, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	peers := []string{"node-1", "node-2", "node-3", "node-4"}

	fmt.Println("Requesting votes from peers (150ms timeout each):")
	votes := 1 // self-vote
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			granted, err := voteWithTimeout(p, 150*time.Millisecond)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				fmt.Printf("  %s: timeout/error (%v)\n", p, err)
			} else if granted {
				fmt.Printf("  %s: GRANTED\n", p)
				votes++
			} else {
				fmt.Printf("  %s: DENIED\n", p)
			}
		}(peer)
	}

	wg.Wait()
	quorum := (len(peers)+1)/2 + 1
	fmt.Printf("\nVotes received: %d/%d, Quorum needed: %d\n", votes, len(peers)+1, quorum)
	fmt.Printf("Election result: %v\n", votes >= quorum)
}

// Pattern 3: Fan-Out / Fan-In
// Used in Hermes for: sending AppendEntries to all followers simultaneously
func FanOutFanIn() {
	fmt.Println("\n=== FAN-OUT / FAN-IN (Raft Replication Pattern) ===")
	fmt.Println()
	fmt.Println("Leader sends log entry to ALL followers simultaneously,")
	fmt.Println("then waits for MAJORITY to acknowledge (quorum).")
	fmt.Println()

	type AppendResult struct {
		nodeID  string
		success bool
		latency time.Duration
	}

	// Simulate sending AppendEntries to 4 followers
	sendAppendEntries := func(nodeID string, latency time.Duration) AppendResult {
		time.Sleep(latency)
		return AppendResult{
			nodeID:  nodeID,
			success: latency < 100*time.Millisecond, // "fail" if too slow
			latency: latency,
		}
	}

	followers := map[string]time.Duration{
		"follower-1": 20 * time.Millisecond,
		"follower-2": 35 * time.Millisecond,
		"follower-3": 250 * time.Millisecond, // slow! (maybe packet loss)
		"follower-4": 45 * time.Millisecond,
	}

	resultCh := make(chan AppendResult, len(followers))
	start := time.Now()

	// FAN OUT: send to all followers concurrently
	for nodeID, latency := range followers {
		go func(id string, lat time.Duration) {
			resultCh <- sendAppendEntries(id, lat)
		}(nodeID, latency)
	}

	// FAN IN: wait for QUORUM (not all)
	// This is the key insight: we don't wait for ALL, just MAJORITY
	// Cluster: 5 nodes (1 leader + 4 followers) → quorum = 3 total = 2 followers
	quorumNeeded := 2
	acks := 0
	total := len(followers)
	var committed bool

	for i := 0; i < total; i++ {
		result := <-resultCh
		fmt.Printf("  %s: success=%v, latency=%v\n",
			result.nodeID, result.success, result.latency)

		if result.success {
			acks++
			if !committed && acks >= quorumNeeded {
				committed = true
				fmt.Printf("\n  ✅ COMMITTED after %v (quorum reached with %d/%d acks)\n",
					time.Since(start), acks, total)
				fmt.Printf("  (Still waiting for remaining acks in background...)\n\n")
			}
		}
	}

	fmt.Printf("  Final: %d/%d acknowledged\n", acks, total)
	fmt.Println()
	fmt.Println("KEY INSIGHT: follower-3 was slow (250ms) but we committed at 45ms")
	fmt.Println("because we only needed quorum, not unanimous agreement!")
}

// Pattern 4: Select with Priority
// Used in Hermes for: Raft tick vs incoming messages
// Raft needs to process ticks BEFORE messages to maintain timing accuracy
func SelectPriorityDemo() {
	fmt.Println("\n=== SELECT WITH PRIORITY (Raft Scheduling) ===")
	fmt.Println()
	fmt.Println("In Raft, some operations are more urgent than others.")
	fmt.Println("Leadership heartbeats must NOT be delayed by client requests.")
	fmt.Println()

	tickCh := make(chan struct{}, 100)
	msgCh := make(chan string, 100)

	// Simulate tick and message arrivals
	go func() {
		for i := 0; i < 10; i++ {
			time.Sleep(10 * time.Millisecond)
			tickCh <- struct{}{}
		}
	}()

	go func() {
		messages := []string{"AppendEntries", "Vote", "Put", "Get", "Put", "Get"}
		for _, msg := range messages {
			msgCh <- msg
			time.Sleep(7 * time.Millisecond) // messages come faster than ticks
		}
	}()

	processed := 0
	timer := time.After(200 * time.Millisecond)

	for {
		// PRIORITY: Always drain ticks first!
		// This is how etcd/raft library works internally
		select {
		case <-tickCh:
			fmt.Printf("  [TICK] Processing heartbeat timer\n")
			processed++
			continue // check ticks again before messages
		default:
		}

		// Then handle messages
		select {
		case <-tickCh:
			fmt.Printf("  [TICK] Processing heartbeat timer\n")
			processed++
		case msg := <-msgCh:
			fmt.Printf("  [MSG]  Processing: %s\n", msg)
			processed++
		case <-timer:
			fmt.Printf("\nProcessed %d events total\n", processed)
			return
		}
	}
}

// Pattern 5: Atomic Operations for Lock-Free Metrics
// Used in Hermes for: request counters, byte counters, etc.
type NodeMetrics struct {
	requestsTotal   int64
	bytesReceived   int64
	bytesSent       int64
	activeConns     int64
	raftCommitTotal int64
}

func (m *NodeMetrics) IncrRequests()          { atomic.AddInt64(&m.requestsTotal, 1) }
func (m *NodeMetrics) AddBytesReceived(n int) { atomic.AddInt64(&m.bytesReceived, int64(n)) }
func (m *NodeMetrics) IncrConnections()       { atomic.AddInt64(&m.activeConns, 1) }
func (m *NodeMetrics) DecrConnections()       { atomic.AddInt64(&m.activeConns, -1) }
func (m *NodeMetrics) Snapshot() NodeMetrics {
	return NodeMetrics{
		requestsTotal:   atomic.LoadInt64(&m.requestsTotal),
		bytesReceived:   atomic.LoadInt64(&m.bytesReceived),
		bytesSent:       atomic.LoadInt64(&m.bytesSent),
		activeConns:     atomic.LoadInt64(&m.activeConns),
		raftCommitTotal: atomic.LoadInt64(&m.raftCommitTotal),
	}
}

func AtomicMetricsDemo() {
	fmt.Println("\n=== ATOMIC OPERATIONS (Lock-Free Metrics) ===")
	fmt.Println()
	fmt.Println("Lock-free metrics are critical in hot paths.")
	fmt.Println("Mutex in a metrics counter would serialize all requests.")
	fmt.Println()

	metrics := &NodeMetrics{}
	var wg sync.WaitGroup

	// Simulate 1000 concurrent requests updating metrics
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			metrics.IncrRequests()
			metrics.AddBytesReceived(512)
			metrics.IncrConnections()
			runtime.Gosched()
			metrics.DecrConnections()
		}()
	}

	wg.Wait()

	snapshot := metrics.Snapshot()
	fmt.Printf("Requests:       %d (expected: 1000)\n", snapshot.requestsTotal)
	fmt.Printf("Bytes received: %d (expected: 512000)\n", snapshot.bytesReceived)
	fmt.Printf("Active conns:   %d (expected: 0)\n", snapshot.activeConns)
	fmt.Println()
	fmt.Println("All counts exact - no race conditions, no locks!")
}

// Pattern 6: sync.Pool - Reducing GC Pressure
// Used in Hermes for: Raft message buffers, protobuf encoding buffers
func SyncPoolDemo() {
	fmt.Println("\n=== SYNC.POOL (Reducing GC Pressure in Hot Paths) ===")
	fmt.Println()
	fmt.Println("In Hermes, every Raft message needs a byte buffer for encoding.")
	fmt.Println("Without pooling: allocate + GC = bad. With pool: reuse = good.")
	fmt.Println()

	// Without pool - allocate every time
	const iterations = 100000
	const bufSize = 4096

	start := time.Now()
	for i := 0; i < iterations; i++ {
		buf := make([]byte, bufSize)
		_ = buf // use it
	}
	noPoolTime := time.Since(start)

	// With sync.Pool - reuse buffers
	bufPool := &sync.Pool{
		New: func() interface{} {
			buf := make([]byte, bufSize)
			return &buf
		},
	}

	start = time.Now()
	for i := 0; i < iterations; i++ {
		bufPtr := bufPool.Get().(*[]byte)
		_ = *bufPtr // use it
		bufPool.Put(bufPtr)
	}
	withPoolTime := time.Since(start)

	fmt.Printf("Without pool (%d iterations): %v\n", iterations, noPoolTime)
	fmt.Printf("With pool    (%d iterations): %v\n", iterations, withPoolTime)
	fmt.Printf("Speedup: %.2fx\n", float64(noPoolTime)/float64(withPoolTime))
	fmt.Println()
	fmt.Println("sync.Pool is cleared by GC, so objects don't accumulate forever.")
	fmt.Println("Use it for: encoding buffers, temporary slices, decoder objects.")
}
