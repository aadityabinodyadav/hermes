// pkg/txn/types.go
package txn

import (
	"time"

	"github.com/aadityabinodyadav/hermes/pkg/clock"
)

// TxnID uniquely identifies a transaction
// Format: "<timestamp>-<nodeID>-<sequence>"
type TxnID string



// IsolationLevel defines the transaction isolation guarantee
type IsolationLevel uint8

const (
	// ReadCommitted: Each statement sees committed data
	// Non-repeatable reads possible
	ReadCommitted IsolationLevel = 0

	// SnapshotIsolation: Transaction sees snapshot at start timestamp
	// Write skew possible (but not in our SSI implementation)
	SnapshotIsolation IsolationLevel = 1

	// Serializable: Full serializability via SSI
	// No anomalies possible, may abort on conflicts
	Serializable IsolationLevel = 2
)

func (l IsolationLevel) String() string {
	switch l {
	case ReadCommitted:
		return "READ_COMMITTED"
	case SnapshotIsolation:
		return "SNAPSHOT_ISOLATION"
	case Serializable:
		return "SERIALIZABLE"
	}
	return "UNKNOWN"
}

// ─────────────────────────────────────────────────────────────────────────────

// Transaction represents an in-progress distributed transaction
type Transaction struct {
	// ID is the unique transaction identifier
	ID TxnID

	// State is the current transaction state
	State TxnState

	// IsolationLevel is the isolation guarantee
	IsolationLevel IsolationLevel

	// ReadTimestamp is the HLC timestamp for snapshot reads
	// All reads in this transaction see data as of this timestamp
	ReadTimestamp clock.HLCTimestamp

	// CommitTimestamp is assigned at commit time
	// For snapshot isolation: commit_ts > read_ts
	// For serializable: commit_ts determined by conflict detection
	CommitTimestamp clock.HLCTimestamp

	// StartTime is when the transaction began (wall clock)
	StartTime time.Time

	// Coordinator is the node coordinating this transaction
	// For single-shard: the shard leader
	// For multi-shard: the shard containing the primary key
	Coordinator string

	// Participants is the set of shards involved in this transaction
	Participants []string

	// PrimaryKey is the primary key for Percolator-style commit
	// If empty, this is a single-shard transaction
	PrimaryKey string

	// WriteSet contains all keys modified by this transaction
	// Used for conflict detection and commit
	WriteSet map[string]*WriteIntent

	// ReadSet contains all keys read by this transaction
	// Used for serializable conflict detection (SSI)
	ReadSet map[string]clock.HLCTimestamp

	// Locks holds locks acquired by this transaction
	// Map: key → lock information
	Locks map[string]*Lock
}

// WriteIntent represents a pending write in a transaction
// The write is not visible to other transactions until commit
type WriteIntent struct {
	Key       string
	Value     []byte
	OldValue  []byte // for undo/rollback
	Timestamp clock.HLCTimestamp
	Deleted   bool // true if this is a delete operation
}

// Lock represents a lock held by a transaction
type Lock struct {
	Key       string
	TxnID     TxnID
	Timestamp clock.HLCTimestamp
	Primary   string    // primary key for this transaction (Percolator)
	ExpiresAt time.Time // lock timeout
}

// ─────────────────────────────────────────────────────────────────────────────

// LockType defines the type of lock
type LockType uint8

const (
	// LockExclusive: Exclusive write lock (for writes)
	LockExclusive LockType = 0

	// LockShared: Shared read lock (for serializable reads)
	LockShared LockType = 1

	// LockIntent: Write intent lock (Percolator prewrite)
	LockIntent LockType = 2
)

// LockManager manages locks for a single shard
// Thread-safe, used by the storage engine
type LockManager struct {
	// locks maps key → lock
	locks map[string]*Lock

	// txnLocks maps txnID → set of keys locked by that txn
	txnLocks map[TxnID]map[string]bool
}

// NewLockManager creates a new lock manager
func NewLockManager() *LockManager {
	return &LockManager{
		locks:    make(map[string]*Lock),
		txnLocks: make(map[TxnID]map[string]bool),
	}
}

// TryLock attempts to acquire a lock on a key
// Returns true if lock acquired, false if conflict
func (lm *LockManager) TryLock(key string, lock *Lock) bool {
	existing, exists := lm.locks[key]

	if !exists {
		// No existing lock — acquire
		lm.locks[key] = lock
		if lm.txnLocks[lock.TxnID] == nil {
			lm.txnLocks[lock.TxnID] = make(map[string]bool)
		}
		lm.txnLocks[lock.TxnID][key] = true
		return true
	}

	// Check if same transaction (reentrant lock)
	if existing.TxnID == lock.TxnID {
		return true
	}

	// Check if existing lock has expired
	if !existing.ExpiresAt.IsZero() && time.Now().After(existing.ExpiresAt) {
		// Expired lock — steal it
		delete(lm.txnLocks[existing.TxnID], key)
		lm.locks[key] = lock
		if lm.txnLocks[lock.TxnID] == nil {
			lm.txnLocks[lock.TxnID] = make(map[string]bool)
		}
		lm.txnLocks[lock.TxnID][key] = true
		return true
	}

	// Conflict!
	return false
}

// Unlock releases all locks held by a transaction
func (lm *LockManager) Unlock(txnID TxnID) {
	keys, exists := lm.txnLocks[txnID]
	if !exists {
		return
	}

	for key := range keys {
		delete(lm.locks, key)
	}
	delete(lm.txnLocks, txnID)
}

// UnlockKey releases a specific lock
func (lm *LockManager) UnlockKey(key string, txnID TxnID) bool {
	lock, exists := lm.locks[key]
	if !exists || lock.TxnID != txnID {
		return false
	}

	delete(lm.locks, key)
	if lm.txnLocks[txnID] != nil {
		delete(lm.txnLocks[txnID], key)
	}
	return true
}

// GetLock returns the lock on a key (if any)
func (lm *LockManager) GetLock(key string) (*Lock, bool) {
	lock, exists := lm.locks[key]
	return lock, exists
}

// HasConflict checks if a transaction would conflict with existing locks
func (lm *LockManager) HasConflict(key string, txnID TxnID) bool {
	lock, exists := lm.locks[key]
	if !exists {
		return false
	}
	return lock.TxnID != txnID
}

// GetExpiredLocks returns locks that have expired
func (lm *LockManager) GetExpiredLocks() []*Lock {
	now := time.Now()
	var expired []*Lock

	for _, lock := range lm.locks {
		if !lock.ExpiresAt.IsZero() && now.After(lock.ExpiresAt) {
			expired = append(expired, lock)
		}
	}

	return expired
}

// Stats returns lock manager statistics
func (lm *LockManager) Stats() LockStats {
	return LockStats{
		ActiveLocks: len(lm.locks),
		ActiveTxns:  len(lm.txnLocks),
	}
}

type LockStats struct {
	ActiveLocks int
	ActiveTxns  int
}
