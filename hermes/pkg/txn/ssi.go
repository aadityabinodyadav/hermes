// pkg/txn/ssi.go
package txn

// SerializableSnapshotIsolation implements SSI conflict detection
//
// Based on: "Serializable Snapshot Isolation in PostgreSQL"
// (Cahill, Röhm, Fekete 2008)
//
// KEY INSIGHT: Track read-write dependencies between transactions.
// If a cycle forms in the dependency graph, abort one transaction.
//
// DEPENDENCY TYPES:
//
//   wr-dependency (write-read): T1 writes X, T2 reads X
//   rw-dependency (read-write): T1 reads X, T2 writes X
//   ww-dependency (write-write): T1 writes X, T2 writes X
//
// SERIALIZATION CONFLICT:
//   A rw-dependency followed by another rw-dependency creates
//   a potential cycle. This is called a "dangerous structure."
//
//   T1 --rw--> T2 --rw--> T3
//   If T3 also has a dependency back to T1: CYCLE → abort one
//
// IMPLEMENTATION:
//   - Track which keys each transaction reads and writes
//   - On commit, check for dangerous structures
//   - If found, abort the transaction with later start time

import (
	"fmt"
	"sync"
	"time"

	"github.com/aadityabinodyadav/hermes/pkg/clock"
)

// SSIDetector detects serialization conflicts
type SSIDetector struct {
	mu sync.RWMutex

	// activeTxns tracks all active transactions
	activeTxns map[TxnID]*Transaction

	// readWriteSets tracks read/write sets per transaction
	// Key: txnID, Value: map of key → (readTs, writeTs)
	readWriteSets map[TxnID]map[string]*RWSet

	// committedTxns for checking against recently committed
	committedTxns map[TxnID]*committedTxnInfo
}

type RWSet struct {
	ReadAt  clock.HLCTimestamp
	Written bool
	WriteAt clock.HLCTimestamp
}

type committedTxnInfo struct {
	TxnID     TxnID
	CommitTs  clock.HLCTimestamp
	WriteKeys map[string]bool
	ReadKeys  map[string]bool
	ExpiresAt time.Time // GC after this
}

// NewSSIDetector creates a new SSI conflict detector
func NewSSIDetector() *SSIDetector {
	d := &SSIDetector{
		activeTxns:    make(map[TxnID]*Transaction),
		readWriteSets: make(map[TxnID]map[string]*RWSet),
		committedTxns: make(map[TxnID]*committedTxnInfo),
	}

	// Start background GC for committed txn info
	go d.gcLoop()

	return d
}

// RegisterTxn registers a transaction for SSI tracking
func (d *SSIDetector) RegisterTxn(txn *Transaction) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.activeTxns[txn.ID] = txn
	d.readWriteSets[txn.ID] = make(map[string]*RWSet)
}

// RecordRead records that a transaction read a key
func (d *SSIDetector) RecordRead(txnID TxnID, key string, ts clock.HLCTimestamp) {
	d.mu.Lock()
	defer d.mu.Unlock()

	rwSet := d.readWriteSets[txnID]
	if rwSet == nil {
		rwSet = make(map[string]*RWSet)
		d.readWriteSets[txnID] = rwSet
	}

	rwSet[key] = &RWSet{
		ReadAt: ts,
	}
}

// RecordWrite records that a transaction wrote a key
func (d *SSIDetector) RecordWrite(txnID TxnID, key string, ts clock.HLCTimestamp) {
	d.mu.Lock()
	defer d.mu.Unlock()

	rwSet := d.readWriteSets[txnID]
	if rwSet == nil {
		rwSet = make(map[string]*RWSet)
		d.readWriteSets[txnID] = rwSet
	}

	if existing := rwSet[key]; existing != nil {
		existing.Written = true
		existing.WriteAt = ts
	} else {
		rwSet[key] = &RWSet{
			Written: true,
			WriteAt: ts,
		}
	}
}

// CheckConflict checks if committing this transaction would violate SSI
// Returns error with conflict details if serialization would be violated
func (d *SSIDetector) CheckConflict(txn *Transaction) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	txnID := txn.ID
	txnRW := d.readWriteSets[txnID]

	if txnRW == nil {
		return nil // no tracking data
	}

	// Check for rw-dependencies with committed transactions
	// T_other reads X, T_current writes X → rw-dependency
	for committedID, committed := range d.committedTxns {
		if committedID == txnID {
			continue
		}

		// Check for dangerous rw-dependency patterns
		for key := range txnRW {
			rw := txnRW[key]
			if !rw.Written {
				continue // we didn't write this key
			}

			// Did the committed transaction read this key?
			if committed.ReadKeys[key] {
				// rw-dependency: committed read, we write
				// Check if we also read something they wrote (creates cycle risk)
				for committedKey := range committed.WriteKeys {
					if txnRW[committedKey] != nil && !txnRW[committedKey].Written {
						// We read what they wrote → potential cycle
						// Check timestamps to determine who should abort
						if txn.ReadTimestamp.After(committed.CommitTs) {
							// We started after they committed → we should abort
							return &SerializationError{
								TxnID:       txnID,
								ConflictTxn: committedID,
								Key:         key,
								Reason:      "rw-dependency cycle detected",
							}
						}
					}
				}
			}
		}
	}

	// Check for conflicts with other active transactions
	for otherID, otherRW := range d.readWriteSets {
		if otherID == txnID {
			continue
		}

		// Check for write-write conflicts
		for key, rw := range txnRW {
			if !rw.Written {
				continue
			}
			if otherRW[key] != nil && otherRW[key].Written {
				// Both transactions wrote the same key
				// Abort the one that started later
				otherTxn := d.activeTxns[otherID]
				if otherTxn != nil && txn.StartTime.After(otherTxn.StartTime) {
					return &SerializationError{
						TxnID:       txnID,
						ConflictTxn: otherID,
						Key:         key,
						Reason:      "write-write conflict",
					}
				}
			}
		}
	}

	return nil
}

// Commit marks a transaction as committed (for future conflict detection)
func (d *SSIDetector) Commit(txnID TxnID, commitTs clock.HLCTimestamp) {
	d.mu.Lock()
	defer d.mu.Unlock()

	rwSet := d.readWriteSets[txnID]
	if rwSet == nil {
		return
	}

	writeKeys := make(map[string]bool)
	readKeys := make(map[string]bool)

	for key, rw := range rwSet {
		if rw.Written {
			writeKeys[key] = true
		} else {
			readKeys[key] = true
		}
	}

	d.committedTxns[txnID] = &committedTxnInfo{
		TxnID:     txnID,
		CommitTs:  commitTs,
		WriteKeys: writeKeys,
		ReadKeys:  readKeys,
		ExpiresAt: time.Now().Add(5 * time.Minute),
	}

	delete(d.activeTxns, txnID)
}

// Abort cleans up tracking for an aborted transaction
func (d *SSIDetector) Abort(txnID TxnID) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.activeTxns, txnID)
	delete(d.readWriteSets, txnID)
}

// gcLoop periodically removes old committed transaction info
func (d *SSIDetector) gcLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		d.mu.Lock()
		now := time.Now()
		for txnID, info := range d.committedTxns {
			if now.After(info.ExpiresAt) {
				delete(d.committedTxns, txnID)
			}
		}
		d.mu.Unlock()
	}
}

// SerializationError is returned when SSI detects a conflict
type SerializationError struct {
	TxnID       TxnID
	ConflictTxn TxnID
	Key         string
	Reason      string
}

func (e *SerializationError) Error() string {
	return fmt.Sprintf("serialization conflict: txn %s conflicts with %s on key %s (%s)",
		e.TxnID, e.ConflictTxn, e.Key, e.Reason)
}

// IsSerializationError returns true if the error is a serialization conflict
func IsSerializationError(err error) bool {
	_, ok := err.(*SerializationError)
	return ok
}
