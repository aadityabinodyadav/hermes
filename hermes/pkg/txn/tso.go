// pkg/txn/tso.go
package txn

// TimestampOracle provides monotonically increasing timestamps
// for transaction ordering
//
// In a distributed system, we need:
//   1. Monotonically increasing timestamps (never go backwards)
//   2. Globally unique (no two transactions get same ts)
//   3. Low latency (every transaction needs a timestamp)
//   4. High availability (TSO can't be a bottleneck)
//
// Options:
//   1. Centralized TSO (like Spanner TrueTime)
//      - Single service hands out timestamps
//      - Simple, but single point of failure
//      - Use Raft to replicate TSO state
//
//   2. HLC-based (what Hermes uses)
//      - Each node has Hybrid Logical Clock
//      - Leader's HLC is the "truth" for commit timestamps
//      - No central bottleneck
//
//   3. Hybrid (like CockroachDB)
//      - Local HLC for start_ts
//      - Central TSO only for commit_ts when needed

import (
	"sync"
	"time"

	"github.com/aadityabinodyadav/hermes/pkg/clock"
)

// TSOConfig configures the timestamp oracle
type TSOConfig struct {
	// SaveInterval is how often to persist TSO state to disk
	// On restart, we recover from this persisted state
	SaveInterval time.Duration

	// MaxBatch is max timestamps to allocate in one batch
	// Reduces contention on the TSO lock
	MaxBatch uint64
}

// DefaultTSOConfig returns sensible defaults
func DefaultTSOConfig() TSOConfig {
	return TSOConfig{
		SaveInterval: 100 * time.Millisecond,
		MaxBatch:     1000,
	}
}

// TimestampOracle provides transaction timestamps
type TimestampOracle struct {
	mu sync.Mutex

	// hlc is the underlying hybrid logical clock
	hlc *clock.HLC

	// lastSaved is the last timestamp we persisted
	lastSaved uint64

	// batch counters for optimization
	batchBase      uint64
	batchRemaining uint64

	// Physical time tracking
	lastPhysical int64 // last physical ms we returned
}

// NewTimestampOracle creates a new TSO
func NewTimestampOracle(hlc *clock.HLC) *TimestampOracle {
	return &TimestampOracle{
		hlc: hlc,
	}
}

// GetTimestamp returns a new monotonically increasing timestamp
// This is the HOT PATH - called for every transaction
//
// The timestamp is an HLC, which combines:
//   - Physical time (for human readability, bounded skew)
//   - Logical counter (for ordering within same physical ms)
func (t *TimestampOracle) GetTimestamp() uint64 {
	// Fast path: use batch allocation
	t.mu.Lock()
	if t.batchRemaining > 0 {
		ts := t.batchBase + (t.batchRemaining - 1)
		t.batchRemaining--
		t.mu.Unlock()
		return ts
	}
	t.mu.Unlock()

	// Slow path: allocate new batch from HLC
	return t.allocateBatch()
}

// allocateBatch gets a new batch of timestamps from the HLC
func (t *TimestampOracle) allocateBatch() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Get current HLC timestamp
	hlcTS := t.hlc.Now()
	ts := uint64(hlcTS)

	// Ensure monotonicity: never return a timestamp <= last one
	if ts <= t.lastSaved {
		ts = t.lastSaved + 1
	}

	// Allocate a batch
	t.batchBase = ts
	t.batchRemaining = 100 // small batch for demo
	t.lastSaved = ts + t.batchRemaining - 1

	// Return first timestamp from batch
	t.batchRemaining--
	return t.batchBase + t.batchRemaining
}

// GetStartTimestamp returns a timestamp for starting a transaction
// This can be more relaxed (doesn't need to be globally ordered)
func (t *TimestampOracle) GetStartTimestamp() uint64 {
	// For start timestamp, we can use local HLC directly
	// No need for coordination - just needs to be unique per node
	return uint64(t.hlc.Now())
}

// GetCommitTimestamp returns a timestamp for committing a transaction
// This MUST be globally ordered and > all start timestamps
func (t *TimestampOracle) GetCommitTimestamp(minTS uint64) uint64 {
	ts := t.GetTimestamp()

	// Ensure commit_ts > minTS (usually the start_ts)
	if ts <= minTS {
		ts = minTS + 1
	}

	return ts
}

// Persist saves the current TSO state to stable storage
// Called periodically to survive restarts
func (t *TimestampOracle) Persist() error {
	t.mu.Lock()
	lastSaved := t.lastSaved
	t.mu.Unlock()

	// In production: write to WAL or dedicated TSO file
	// For now, just track in memory
	_ = lastSaved
	return nil
}

// Recover restores TSO state from stable storage
// Called on startup to ensure monotonicity after restart
func (t *TimestampOracle) Recover(lastPersisted uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if lastPersisted > t.lastSaved {
		t.lastSaved = lastPersisted
	}
}
