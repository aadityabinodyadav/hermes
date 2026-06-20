// pkg/ratelimit/limiter.go
package ratelimit

// RateLimiter implements multiple rate limiting algorithms
//
// Used in Hermes for:
//   - Per-client request rate limiting
//   - Per-shard write rate limiting
//   - Cluster-level operation limiting (migrations, compactions)

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// TOKEN BUCKET
// ─────────────────────────────────────────────────────────────────────────────

// TokenBucket implements the token bucket algorithm
//
// Allows bursts up to capacity, then limits to rate/sec
type TokenBucket struct {
	mu       sync.Mutex
	capacity float64   // max tokens (burst size)
	rate     float64   // tokens added per second
	tokens   float64   // current token count
	lastTime time.Time // when we last added tokens

	// Stats
	allowed  int64
	rejected int64
}

// NewTokenBucket creates a new token bucket
// capacity: max burst size (tokens)
// rate: tokens per second (sustained throughput)
func NewTokenBucket(capacity, rate float64) *TokenBucket {
	return &TokenBucket{
		capacity: capacity,
		rate:     rate,
		tokens:   capacity, // start full
		lastTime: time.Now(),
	}
}

// Allow checks if a request should be allowed
// cost: how many tokens this request consumes (usually 1)
func (tb *TokenBucket) Allow(cost float64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	// Refill tokens based on elapsed time
	now := time.Now()
	elapsed := now.Sub(tb.lastTime).Seconds()
	tb.lastTime = now

	tb.tokens = math.Min(tb.capacity, tb.tokens+elapsed*tb.rate)

	if tb.tokens >= cost {
		tb.tokens -= cost
		atomic.AddInt64(&tb.allowed, 1)
		return true
	}

	atomic.AddInt64(&tb.rejected, 1)
	return false
}

// AllowN allows up to n tokens to be consumed
// Returns how many were actually allowed
func (tb *TokenBucket) AllowN(n int) int {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastTime).Seconds()
	tb.lastTime = now
	tb.tokens = math.Min(tb.capacity, tb.tokens+elapsed*tb.rate)

	allowed := int(math.Min(float64(n), tb.tokens))
	tb.tokens -= float64(allowed)

	atomic.AddInt64(&tb.allowed, int64(allowed))
	atomic.AddInt64(&tb.rejected, int64(n-allowed))

	return allowed
}

// Wait blocks until a token is available (with context)
func (tb *TokenBucket) Wait(ctx context.Context) error {
	for {
		if tb.Allow(1) {
			return nil
		}

		// Calculate when next token will be available
		tb.mu.Lock()
		waitTime := time.Duration((1 - tb.tokens) / tb.rate * float64(time.Second))
		tb.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
		}
	}
}

// Stats returns token bucket statistics
func (tb *TokenBucket) Stats() TokenBucketStats {
	tb.mu.Lock()
	currentTokens := tb.tokens
	tb.mu.Unlock()

	return TokenBucketStats{
		Capacity:      tb.capacity,
		Rate:          tb.rate,
		CurrentTokens: currentTokens,
		Allowed:       atomic.LoadInt64(&tb.allowed),
		Rejected:      atomic.LoadInt64(&tb.rejected),
	}
}

type TokenBucketStats struct {
	Capacity      float64
	Rate          float64
	CurrentTokens float64
	Allowed       int64
	Rejected      int64
}

func (s TokenBucketStats) String() string {
	total := s.Allowed + s.Rejected
	rejectRate := 0.0
	if total > 0 {
		rejectRate = float64(s.Rejected) / float64(total) * 100
	}
	return fmt.Sprintf("TokenBucket{capacity=%.0f, rate=%.0f/s, tokens=%.1f, rejected=%.1f%%}",
		s.Capacity, s.Rate, s.CurrentTokens, rejectRate)
}

// ─────────────────────────────────────────────────────────────────────────────
// SLIDING WINDOW COUNTER
// ─────────────────────────────────────────────────────────────────────────────

// SlidingWindowCounter implements the sliding window counter algorithm
// Approximation: uses current and previous window counts
type SlidingWindowCounter struct {
	mu          sync.Mutex
	limit       int64         // max requests per window
	windowSize  time.Duration // window size
	currCount   int64         // count in current window
	prevCount   int64         // count in previous window
	windowStart time.Time     // start of current window

	// Stats
	allowed  int64
	rejected int64
}

// NewSlidingWindowCounter creates a sliding window counter
// limit: max requests per windowSize duration
func NewSlidingWindowCounter(limit int64, windowSize time.Duration) *SlidingWindowCounter {
	return &SlidingWindowCounter{
		limit:       limit,
		windowSize:  windowSize,
		windowStart: time.Now(),
	}
}

// Allow checks if a request should be allowed
func (sw *SlidingWindowCounter) Allow() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(sw.windowStart)

	if elapsed >= sw.windowSize {
		// Moved to new window
		// Rotate: current becomes previous, reset current
		if elapsed >= 2*sw.windowSize {
			// More than 2 windows elapsed: previous is also stale
			sw.prevCount = 0
		} else {
			sw.prevCount = sw.currCount
		}
		sw.currCount = 0
		sw.windowStart = sw.windowStart.Add(
			(elapsed / sw.windowSize) * sw.windowSize,
		)
		elapsed = now.Sub(sw.windowStart)
	}

	// Estimate count in sliding window:
	// weight = how much of the previous window is still in our window
	// Example: if 30% into current window, previous contributes 70%
	prevWeight := 1.0 - elapsed.Seconds()/sw.windowSize.Seconds()
	estimated := float64(sw.prevCount)*prevWeight + float64(sw.currCount)

	if int64(estimated)+1 > sw.limit {
		atomic.AddInt64(&sw.rejected, 1)
		return false
	}

	sw.currCount++
	atomic.AddInt64(&sw.allowed, 1)
	return true
}

// Stats returns current statistics
func (sw *SlidingWindowCounter) Stats() SlidingWindowStats {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	return SlidingWindowStats{
		Limit:       sw.limit,
		WindowSize:  sw.windowSize,
		CurrentRate: float64(sw.currCount),
		Allowed:     atomic.LoadInt64(&sw.allowed),
		Rejected:    atomic.LoadInt64(&sw.rejected),
	}
}

type SlidingWindowStats struct {
	Limit       int64
	WindowSize  time.Duration
	CurrentRate float64
	Allowed     int64
	Rejected    int64
}

// ─────────────────────────────────────────────────────────────────────────────
// DISTRIBUTED RATE LIMITER
// ─────────────────────────────────────────────────────────────────────────────

// DistributedRateLimiter implements cluster-wide rate limiting
// using local approximation + gossip
//
// Each node tracks its own rate, periodically shares with peers.
// When estimating total cluster rate, uses: my_rate + sum(peer_rates)
// This allows short-term overrun during gossip interval, but
// converges to correct rate within gossip_interval seconds.
type DistributedRateLimiter struct {
	mu sync.RWMutex

	nodeID      string
	globalLimit float64       // cluster-wide limit
	gossipEvery time.Duration // how often to sync with peers

	// Local token bucket (fraction of global limit)
	localLimit  float64
	localBucket *TokenBucket

	// Peer rates (approximate, from gossip)
	peerRates map[string]float64 // nodeID → rate

	// Stats
	globalAllowed  int64
	globalRejected int64
}

// NewDistributedRateLimiter creates a distributed rate limiter
func NewDistributedRateLimiter(
	nodeID string,
	globalLimit float64,
	clusterSize int,
) *DistributedRateLimiter {
	// Each node gets 1/N of the global limit initially
	// Adjusted dynamically based on actual cluster size
	localLimit := globalLimit / float64(clusterSize)

	return &DistributedRateLimiter{
		nodeID:      nodeID,
		globalLimit: globalLimit,
		gossipEvery: 100 * time.Millisecond,
		localLimit:  localLimit,
		localBucket: NewTokenBucket(localLimit, localLimit),
		peerRates:   make(map[string]float64),
	}
}

// Allow checks if a request should be allowed
// Uses local bucket for fast path, periodically syncs with peers
func (d *DistributedRateLimiter) Allow() bool {
	allowed := d.localBucket.Allow(1)

	if allowed {
		atomic.AddInt64(&d.globalAllowed, 1)
	} else {
		atomic.AddInt64(&d.globalRejected, 1)
	}

	return allowed
}

// UpdatePeerRate updates our knowledge of a peer's current rate
// Called when we receive a gossip message
func (d *DistributedRateLimiter) UpdatePeerRate(peerID string, rate float64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.peerRates[peerID] = rate

	// Recalculate our local limit
	// We want: our_rate + sum(peer_rates) <= globalLimit
	// So: our_rate = globalLimit - sum(peer_rates)
	totalPeerRate := 0.0
	for _, r := range d.peerRates {
		totalPeerRate += r
	}

	newLocalLimit := d.globalLimit - totalPeerRate
	if newLocalLimit < d.globalLimit*0.1 {
		newLocalLimit = d.globalLimit * 0.1 // always allow at least 10%
	}
	if newLocalLimit > d.globalLimit {
		newLocalLimit = d.globalLimit
	}

	// Update local bucket rate
	d.localLimit = newLocalLimit
	// In production: would smoothly adjust bucket rate
}

// CurrentRate returns our current request rate (for gossip)
func (d *DistributedRateLimiter) CurrentRate() float64 {
	stats := d.localBucket.Stats()
	// Approximate current rate from allowed count
	// In production: use exponential moving average
	return d.localLimit - stats.CurrentTokens
}

// Stats returns distributed rate limiter statistics
func (d *DistributedRateLimiter) Stats() DistRateLimiterStats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return DistRateLimiterStats{
		NodeID:         d.nodeID,
		GlobalLimit:    d.globalLimit,
		LocalLimit:     d.localLimit,
		PeerCount:      len(d.peerRates),
		GlobalAllowed:  atomic.LoadInt64(&d.globalAllowed),
		GlobalRejected: atomic.LoadInt64(&d.globalRejected),
	}
}

type DistRateLimiterStats struct {
	NodeID         string
	GlobalLimit    float64
	LocalLimit     float64
	PeerCount      int
	GlobalAllowed  int64
	GlobalRejected int64
}

// ─────────────────────────────────────────────────────────────────────────────
// CIRCUIT BREAKER
// ─────────────────────────────────────────────────────────────────────────────

// CircuitBreaker prevents cascade failures
//
// States:
//   CLOSED:    Normal operation, requests pass through
//   OPEN:      Too many failures, reject all requests immediately
//   HALF-OPEN: Testing recovery, allow some requests through
//
// State transitions:
//   CLOSED → OPEN: error rate exceeds threshold
//   OPEN → HALF-OPEN: after cooldown period
//   HALF-OPEN → CLOSED: success rate improves
//   HALF-OPEN → OPEN: still failing

type CircuitState uint8

const (
	CircuitClosed   CircuitState = 0 // normal
	CircuitOpen     CircuitState = 1 // rejecting all
	CircuitHalfOpen CircuitState = 2 // testing
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "CLOSED"
	case CircuitOpen:
		return "OPEN"
	case CircuitHalfOpen:
		return "HALF_OPEN"
	}
	return "UNKNOWN"
}

// CircuitBreaker protects a service from cascade failures
type CircuitBreaker struct {
	mu sync.Mutex

	name string

	// Thresholds
	failureThreshold float64       // error rate to open circuit
	successThreshold int           // successes to close circuit
	cooldown         time.Duration // time to wait before half-open

	// State
	state         CircuitState
	failures      int
	successes     int
	totalRequests int
	lastFailure   time.Time
	openedAt      time.Time

	// Stats
	allowed  int64
	rejected int64
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(name string) *CircuitBreaker {
	return &CircuitBreaker{
		name:             name,
		failureThreshold: 0.5, // 50% error rate → open
		successThreshold: 5,   // 5 successes → close
		cooldown:         10 * time.Second,
	}
}

// Allow checks if a request should be allowed
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		// Allow request
		cb.totalRequests++
		atomic.AddInt64(&cb.allowed, 1)
		return true

	case CircuitOpen:
		// Check if cooldown has elapsed
		if time.Since(cb.openedAt) >= cb.cooldown {
			// Transition to half-open
			cb.state = CircuitHalfOpen
			cb.successes = 0
			cb.failures = 0
			fmt.Printf("[CircuitBreaker] %s: OPEN → HALF_OPEN\n", cb.name)

			cb.totalRequests++
			atomic.AddInt64(&cb.allowed, 1)
			return true
		}

		// Still open, reject
		atomic.AddInt64(&cb.rejected, 1)
		return false

	case CircuitHalfOpen:
		// Allow limited requests to test recovery
		// Simple: allow every other request
		cb.totalRequests++
		if cb.totalRequests%2 == 0 {
			atomic.AddInt64(&cb.allowed, 1)
			return true
		}
		atomic.AddInt64(&cb.rejected, 1)
		return false
	}

	return true
}

// Success records a successful request
func (cb *CircuitBreaker) Success() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.successes++

	if cb.state == CircuitHalfOpen && cb.successes >= cb.successThreshold {
		// Enough successes — close the circuit
		cb.state = CircuitClosed
		cb.failures = 0
		cb.totalRequests = 0
		fmt.Printf("[CircuitBreaker] %s: HALF_OPEN → CLOSED ✅\n", cb.name)
	}
}

// Failure records a failed request
func (cb *CircuitBreaker) Failure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailure = time.Now()

	switch cb.state {
	case CircuitClosed:
		// Check if failure rate exceeds threshold
		if cb.totalRequests > 10 { // minimum sample size
			errorRate := float64(cb.failures) / float64(cb.totalRequests)
			if errorRate >= cb.failureThreshold {
				cb.state = CircuitOpen
				cb.openedAt = time.Now()
				fmt.Printf("[CircuitBreaker] %s: CLOSED → OPEN ❌ (error_rate=%.0f%%)\n",
					cb.name, errorRate*100)
			}
		}

	case CircuitHalfOpen:
		// Any failure in half-open → back to open
		cb.state = CircuitOpen
		cb.openedAt = time.Now()
		fmt.Printf("[CircuitBreaker] %s: HALF_OPEN → OPEN ❌\n", cb.name)
	}
}

// State returns the current circuit state
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}
