// pkg/consistency/lease.go
package consistency

// LeaderLease implements lease-based reads for zero-overhead linearizable reads
//
// IDEA:
//   When the leader wins an election or sends heartbeats with majority ACK,
//   it's guaranteed to remain leader for at least one election timeout period.
//   During this "lease", it can serve reads without contacting quorum.
//
// PARAMETERS:
//   electionTimeout: how long before followers start a new election
//   maxClockDrift:   maximum expected clock drift between nodes
//   leaseDuration:   electionTimeout - 2*maxClockDrift (conservative)
//
// EXAMPLE:
//   electionTimeout = 150ms
//   maxClockDrift   = 5ms
//   leaseDuration   = 150ms - 10ms = 140ms
//
//   At T=0: leader sends heartbeats, gets majority ACK
//   At T=0: lease valid until T=140ms
//   At T=100ms: leader serves read (no quorum needed)
//   At T=140ms: lease expires, must do ReadIndex or renew lease
//
// CORRECTNESS ARGUMENT:
//   A new leader can't be elected until:
//     1. Followers don't hear from leader for electionTimeout
//     2. Plus the random delay before candidate fires
//
//   So if we subtracted 2*maxClockDrift from electionTimeout,
//   no new leader can be elected during our lease period.
//   (Assuming clocks don't drift more than maxClockDrift)

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// LeaseConfig configures the lease parameters
type LeaseConfig struct {
	// ElectionTimeout is the Raft election timeout
	ElectionTimeout time.Duration

	// MaxClockDrift is the maximum expected clock drift
	MaxClockDrift time.Duration
}

// DefaultLeaseConfig returns sensible defaults
func DefaultLeaseConfig() LeaseConfig {
	return LeaseConfig{
		ElectionTimeout: 150 * time.Millisecond,
		MaxClockDrift:   5 * time.Millisecond,
	}
}

// LeaderLease manages the leader lease
type LeaderLease struct {
	mu sync.RWMutex

	config LeaseConfig

	// nodeID for logging
	nodeID string

	// leaseExpiry is when the current lease expires
	leaseExpiry time.Time

	// lastRenewal is when we last renewed the lease
	lastRenewal time.Time

	// leaseDuration is effective_lease = election_timeout - 2*max_clock_drift
	leaseDuration time.Duration

	// Stats
	readsServedUnderLease uint64
	leaseExpirations      uint64
}

// NewLeaderLease creates a new lease manager
func NewLeaderLease(nodeID string, config LeaseConfig) *LeaderLease {
	effectiveLease := config.ElectionTimeout - 2*config.MaxClockDrift

	return &LeaderLease{
		nodeID:        nodeID,
		config:        config,
		leaseDuration: effectiveLease,
	}
}

// Renew extends the lease (called when we get majority heartbeat ACKs)
func (l *LeaderLease) Renew(at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lastRenewal = at
	l.leaseExpiry = at.Add(l.leaseDuration)

	fmt.Printf("[%s] Lease renewed, valid until %s (duration=%v)\n",
		l.nodeID,
		l.leaseExpiry.Format("15:04:05.000"),
		l.leaseDuration)
}

// IsValid returns true if we currently hold a valid lease
// This means we can serve reads without contacting quorum
func (l *LeaderLease) IsValid() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.leaseExpiry.IsZero() {
		return false // never had a lease
	}

	return time.Now().Before(l.leaseExpiry)
}

// Expire explicitly invalidates the lease
// Called when we step down from leadership
func (l *LeaderLease) Expire() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.leaseExpiry = time.Time{} // zero time = expired
	l.leaseExpirations++

	fmt.Printf("[%s] Lease expired\n", l.nodeID)
}

// TimeRemaining returns how long the lease is still valid
func (l *LeaderLease) TimeRemaining() time.Duration {
	l.mu.RLock()
	defer l.mu.RUnlock()

	remaining := time.Until(l.leaseExpiry)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// ServeWithLease attempts to serve a read using the lease
// Returns true if served from lease (fast path)
// Returns false if lease expired (must use ReadIndex slow path)
func (l *LeaderLease) ServeWithLease(ctx context.Context) (bool, error) {
	if l.IsValid() {
		l.mu.Lock()
		l.readsServedUnderLease++
		l.mu.Unlock()
		return true, nil
	}

	// Lease expired — caller must use ReadIndex
	return false, nil
}

// Stats returns lease statistics
func (l *LeaderLease) Stats() LeaseStats {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return LeaseStats{
		Valid:                 !l.leaseExpiry.IsZero() && time.Now().Before(l.leaseExpiry),
		ReadsServedUnderLease: l.readsServedUnderLease,
		LeaseExpirations:      l.leaseExpirations,
		TimeRemaining:         l.TimeRemaining(),
	}
}

type LeaseStats struct {
	Valid                 bool
	ReadsServedUnderLease uint64
	LeaseExpirations      uint64
	TimeRemaining         time.Duration
}
