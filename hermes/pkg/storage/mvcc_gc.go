// pkg/storage/mvcc_gc.go
package storage

// MVCCGarbageCollector removes old versions of keys
//
// Without GC:
//   key "balance:alice" at T=1: $100
//   key "balance:alice" at T=2: $200
//   key "balance:alice" at T=3: $300
//   ... 1 million updates later ...
//   key "balance:alice" at T=1000000: $500
//   → Disk full. System dead.
//
// With GC:
//   Keep: latest version + anything within retention window
//   Delete: versions older than retention window
//   Except: versions that active snapshots are reading at
//
// This is what CockroachDB calls "MVCC GC" and
// PostgreSQL calls "VACUUM".
//
// Safe GC invariant:
//   Never delete a version that any active transaction
//   might read. Active transactions have a read_timestamp.
//   The "safe point" is min(all active read_timestamps) - 1.
//   Everything before the safe point can be GC'd.

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// GCConfig configures the MVCC garbage collector
type GCConfig struct {
	// RetentionPeriod: keep versions newer than this
	// Default: 1 hour (for time-travel queries)
	RetentionPeriod time.Duration

	// RunInterval: how often to run GC
	RunInterval time.Duration

	// MaxKeysPerRun: don't GC more than this many keys per run
	// Prevents GC from monopolizing disk I/O
	MaxKeysPerRun int

	// MinVersionsToKeep: always keep at least this many versions
	// Even if all versions are old
	MinVersionsToKeep int
}

// DefaultGCConfig returns sensible defaults
func DefaultGCConfig() GCConfig {
	return GCConfig{
		RetentionPeriod:   1 * time.Hour,
		RunInterval:       10 * time.Minute,
		MaxKeysPerRun:     100000,
		MinVersionsToKeep: 1,
	}
}

// ActiveTransactionTracker tracks read timestamps of active transactions
// GC uses this to determine the safe GC point
type ActiveTransactionTracker struct {
	mu             sync.RWMutex
	readTimestamps map[uint64]int64 // txnID → read_timestamp (HLC)
}

// NewActiveTransactionTracker creates a new tracker
func NewActiveTransactionTracker() *ActiveTransactionTracker {
	return &ActiveTransactionTracker{
		readTimestamps: make(map[uint64]int64),
	}
}

// Register tracks an active transaction's read timestamp
func (t *ActiveTransactionTracker) Register(txnID uint64, readTS int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.readTimestamps[txnID] = readTS
}

// Unregister removes a completed transaction
func (t *ActiveTransactionTracker) Unregister(txnID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.readTimestamps, txnID)
}

// SafePoint returns the timestamp before which it's safe to GC
// = min(all active read timestamps) - 1
// = now - retention_period (if no active transactions)
func (t *ActiveTransactionTracker) SafePoint(retention time.Duration) int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Default safe point: now - retention period
	safePoint := time.Now().Add(-retention).UnixNano()

	// Constrain by active transactions
	for _, readTS := range t.readTimestamps {
		if readTS-1 < safePoint {
			safePoint = readTS - 1
		}
	}

	return safePoint
}

// MVCCVersion is one version of a key
type MVCCVersion struct {
	Timestamp int64
	Value     []byte
	Deleted   bool
}

// MVCCGarbageCollector runs periodic GC on the storage engine
type MVCCGarbageCollector struct {
	config  GCConfig
	tracker *ActiveTransactionTracker

	// Stats
	versionsRemoved int64
	keysGCd         int64
	bytesReclaimed  int64
	lastRunAt       time.Time
	lastRunDuration time.Duration

	// Lifecycle
	mu     sync.Mutex
	stopCh chan struct{}
	doneCh chan struct{}
}

// NewMVCCGarbageCollector creates a GC instance
func NewMVCCGarbageCollector(
	config GCConfig,
	tracker *ActiveTransactionTracker,
) *MVCCGarbageCollector {
	return &MVCCGarbageCollector{
		config:  config,
		tracker: tracker,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
}

// Start begins periodic GC in the background
func (gc *MVCCGarbageCollector) Start() {
	go gc.run()
	fmt.Printf("[GC] MVCC garbage collector started (retention=%v, interval=%v)\n",
		gc.config.RetentionPeriod, gc.config.RunInterval)
}

// Stop halts the GC
func (gc *MVCCGarbageCollector) Stop() {
	close(gc.stopCh)
	<-gc.doneCh
}

// run is the main GC loop
func (gc *MVCCGarbageCollector) run() {
	defer close(gc.doneCh)

	ticker := time.NewTicker(gc.config.RunInterval)
	defer ticker.Stop()

	for {
		select {
		case <-gc.stopCh:
			return
		case <-ticker.C:
			gc.runOnce()
		}
	}
}

// runOnce executes one GC pass
func (gc *MVCCGarbageCollector) runOnce() {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	start := time.Now()
	safePoint := gc.tracker.SafePoint(gc.config.RetentionPeriod)

	fmt.Printf("[GC] Running GC pass (safe_point=%v)\n",
		time.Unix(0, safePoint).Format("15:04:05.000"))

	// In a real implementation:
	// 1. Scan all keys in storage engine
	// 2. For each key, find versions older than safePoint
	// 3. Keep: latest version + any version within retention
	// 4. Delete: old versions beyond MinVersionsToKeep
	//
	// Here we show the algorithm clearly:

	keysGCd := 0
	versionsRemoved := 0

	// Simulated GC (real impl iterates actual SSTable keys)
	gcResult := gc.simulateGCPass(safePoint)
	keysGCd = gcResult.keysProcessed
	versionsRemoved = gcResult.versionsRemoved

	gc.lastRunAt = time.Now()
	gc.lastRunDuration = time.Since(start)

	atomic.AddInt64(&gc.keysGCd, int64(keysGCd))
	atomic.AddInt64(&gc.versionsRemoved, int64(versionsRemoved))

	fmt.Printf("[GC] Pass complete: %d keys processed, %d versions removed (%v)\n",
		keysGCd, versionsRemoved, gc.lastRunDuration)
}

type gcResult struct {
	keysProcessed   int
	versionsRemoved int
	bytesReclaimed  int
}

// simulateGCPass shows the GC algorithm clearly
// In production: this iterates actual SSTable entries
func (gc *MVCCGarbageCollector) simulateGCPass(safePoint int64) gcResult {
	// Example: key "balance:alice" has 5 versions
	// safePoint = T=3
	// Versions: T=1, T=2, T=3, T=4, T=5
	// After GC: T=4, T=5 (T=1,2,3 are before safePoint)
	//           BUT keep at least MinVersionsToKeep (1) even if all old
	//
	// Algorithm for one key:
	//   versions = sorted by timestamp descending
	//   keep_count = 0
	//   for version in versions:
	//     if version.timestamp > safePoint:
	//       keep (still potentially readable)
	//       keep_count++
	//     elif keep_count < MinVersionsToKeep:
	//       keep (must keep at least N versions)
	//       keep_count++
	//     else:
	//       DELETE (safe to remove)

	return gcResult{
		keysProcessed:   0, // Would be actual key count
		versionsRemoved: 0, // Would be actual removed count
	}
}

// ForceRun runs GC immediately (for testing and admin operations)
func (gc *MVCCGarbageCollector) ForceRun() {
	go gc.runOnce()
}

// Stats returns GC statistics
func (gc *MVCCGarbageCollector) Stats() GCStats {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	return GCStats{
		VersionsRemoved: atomic.LoadInt64(&gc.versionsRemoved),
		KeysGCd:         atomic.LoadInt64(&gc.keysGCd),
		BytesReclaimed:  atomic.LoadInt64(&gc.bytesReclaimed),
		LastRunAt:       gc.lastRunAt,
		LastRunDuration: gc.lastRunDuration,
	}
}

// GCStats contains GC metrics
type GCStats struct {
	VersionsRemoved int64
	KeysGCd         int64
	BytesReclaimed  int64
	LastRunAt       time.Time
	LastRunDuration time.Duration
}

func (s GCStats) String() string {
	return fmt.Sprintf(
		"GC{versions_removed=%d, keys_gc'd=%d, bytes_reclaimed=%d, last_run=%v (%v ago)}",
		s.VersionsRemoved,
		s.KeysGCd,
		s.BytesReclaimed,
		s.LastRunAt.Format("15:04:05"),
		time.Since(s.LastRunAt).Round(time.Second),
	)
}
