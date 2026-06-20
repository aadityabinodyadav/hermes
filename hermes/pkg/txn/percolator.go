// pkg/txn/percolator.go
package txn

// PercolatorTransaction implements Google's Percolator protocol
//
// This is the MODERN approach used by Spanner, TiDB, CockroachDB.
// Key insight: No blocking coordinator — use primary key as coordinator.
//
// PROTOCOL:
//
//   1. PREWRITE phase:
//      - Pick a primary key (first key or random)
//      - For each key:
//        a. Check for conflicts (lock exists from another txn)
//        b. Write LOCK record (txn_id, primary_key, timestamp)
//        c. Write actual value (not visible yet)
//      - Primary key is written LAST
//
//   2. COMMIT phase:
//      - Commit PRIMARY key first (write commit timestamp)
//      - Now transaction is visible!
//      - Commit secondary keys asynchronously (background)
//
//   3. CLEANUP phase:
//      - Background process cleans up stale locks
//      - Checks primary key to determine txn outcome
//
// WHY THIS IS BETTER THAN 2PC:
//   - No single point of failure
//   - No blocking on coordinator crash
//   - Participants can recover independently
//   - Higher availability

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aadityabinodyadav/hermes/pkg/clock"
)

// PercolatorTransaction represents a Percolator-style transaction
type PercolatorTransaction struct {
	*Transaction

	// PrimaryKey is the coordinating key for this transaction
	// All other keys reference this for commit status
	PrimaryKey string

	// PrimaryLock is the lock on the primary key
	PrimaryLock *Lock

	// SecondaryKeys are all non-primary keys in this transaction
	SecondaryKeys []string

	// prewriteSuccess tracks which keys were successfully prewritten
	prewriteSuccess map[string]bool

	// commitStarted is true once primary key commit begins
	commitStarted bool

	mu sync.Mutex
}

// PercolatorClient is the client interface for Percolator operations
type PercolatorClient struct {
	hlc *clock.HLC

	// Lock timeout — if a lock is held longer than this,
	// it's considered stale and can be cleaned up
	lockTimeout time.Duration

	// Write batch size for prewrite
	batchSize int
}

// NewPercolatorClient creates a new Percolator client
func NewPercolatorClient(hlc *clock.HLC) *PercolatorClient {
	return &PercolatorClient{
		hlc:         hlc,
		lockTimeout: 5 * time.Second,
		batchSize:   100,
	}
}

// Begin starts a new Percolator transaction
func (c *PercolatorClient) Begin(
	ctx context.Context,
	isolation IsolationLevel,
) (*PercolatorTransaction, error) {
	txnID := c.generateTxnID()

	txn := &PercolatorTransaction{
		Transaction: &Transaction{
			ID:             txnID,
			State:          TxnActive,
			IsolationLevel: isolation,
			ReadTimestamp:  c.hlc.Now(),
			StartTime:      time.Now(),
			WriteSet:       make(map[string]*WriteIntent),
			ReadSet:        make(map[string]clock.HLCTimestamp),
			Locks:          make(map[string]*Lock),
		},
		prewriteSuccess: make(map[string]bool),
	}

	return txn, nil
}

// Set adds a write to the transaction's write set
// Does NOT write to storage yet — that happens in Prewrite
func (t *PercolatorTransaction) Set(key string, value []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.WriteSet[key] = &WriteIntent{
		Key:       key,
		Value:     value,
		Timestamp: t.ReadTimestamp,
		Deleted:   false,
	}

	// Set primary key on first write
	if t.PrimaryKey == "" {
		t.PrimaryKey = key
	}
}

// Delete adds a delete to the transaction's write set
func (t *PercolatorTransaction) Delete(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.WriteSet[key] = &WriteIntent{
		Key:       key,
		Value:     nil,
		Timestamp: t.ReadTimestamp,
		Deleted:   true,
	}

	if t.PrimaryKey == "" {
		t.PrimaryKey = key
	}
}

// Get reads a value within the transaction
// Uses MVCC snapshot isolation
func (t *PercolatorTransaction) Get(
	ctx context.Context,
	key string,
) ([]byte, bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if we've written this key
	if intent, exists := t.WriteSet[key]; exists {
		if intent.Deleted {
			return nil, false, nil
		}
		return intent.Value, true, nil
	}

	// Read from storage at read timestamp
	// In production: storage.GetAtTimestamp(key, t.ReadTimestamp)
	// For demo, we simulate

	t.ReadSet[key] = t.ReadTimestamp

	// Simulated read
	return nil, false, nil
}

// Prewrite executes the prewrite phase
// This is where locks are acquired and values are written (but not visible)
func (c *PercolatorClient) Prewrite(
	ctx context.Context,
	txn *PercolatorTransaction,
) error {
	txn.mu.Lock()
	if txn.PrimaryKey == "" {
		txn.mu.Unlock()
		return fmt.Errorf("txn: no keys in transaction")
	}

	// Separate primary and secondary keys
	var secondaryKeys []string
	for key := range txn.WriteSet {
		if key != txn.PrimaryKey {
			secondaryKeys = append(secondaryKeys, key)
		}
	}
	txn.SecondaryKeys = secondaryKeys
	txn.mu.Unlock()

	// Prewrite secondary keys FIRST
	for _, key := range secondaryKeys {
		if err := c.prewriteKey(ctx, txn, key, false); err != nil {
			// Conflict or error — abort
			c.Cleanup(ctx, txn)
			return fmt.Errorf("txn: prewrite conflict on %s: %w", key, err)
		}
		txn.mu.Lock()
		txn.prewriteSuccess[key] = true
		txn.mu.Unlock()
	}

	// Prewrite PRIMARY KEY LAST
	// This is CRUCIAL: if primary succeeds, transaction can commit
	// If primary fails, entire transaction aborts cleanly
	if err := c.prewriteKey(ctx, txn, txn.PrimaryKey, true); err != nil {
		c.Cleanup(ctx, txn)
		return fmt.Errorf("txn: prewrite conflict on primary %s: %w",
			txn.PrimaryKey, err)
	}

	txn.mu.Lock()
	txn.prewriteSuccess[txn.PrimaryKey] = true
	txn.mu.Unlock()

	return nil
}

// prewriteKey writes a single key in the prewrite phase
func (c *PercolatorClient) prewriteKey(
	ctx context.Context,
	txn *PercolatorTransaction,
	key string,
	isPrimary bool,
) error {
	// intent := txn.WriteSet[key]

	// // Create lock
	// lock := &Lock{
	// 	Key:       key,
	// 	TxnID:     txn.ID,
	// 	Timestamp: txn.ReadTimestamp,
	// 	Primary:   txn.PrimaryKey,
	// 	ExpiresAt: time.Now().Add(c.lockTimeout),
	// }

	// Try to acquire lock
	// In production: storage.TryLock(key, lock)
	// For demo, we simulate

	fmt.Printf("[%s] PREWRITE %s (primary=%v)\n", txn.ID, key, isPrimary)

	// Simulate lock acquisition
	time.Sleep(5 * time.Millisecond)

	// Write the actual value (not visible until commit)
	// In production: storage.WriteIntent(key, intent, lock)

	return nil
}

// Commit executes the commit phase
// Primary key is committed first, then secondaries asynchronously
func (c *PercolatorClient) Commit(
	ctx context.Context,
	txn *PercolatorTransaction,
) error {
	txn.mu.Lock()
	if !txn.prewriteSuccess[txn.PrimaryKey] {
		txn.mu.Unlock()
		return fmt.Errorf("txn: primary key not prewritten")
	}

	txn.commitStarted = true
	commitTs := c.hlc.Now()
	txn.CommitTimestamp = commitTs
	txn.mu.Unlock()

	// Step 1: Commit PRIMARY KEY
	// This makes the transaction VISIBLE
	if err := c.commitKey(ctx, txn, txn.PrimaryKey, true); err != nil {
		return fmt.Errorf("txn: commit primary failed: %w", err)
	}

	// Step 2: Commit secondary keys asynchronously
	// Even if this fails, primary is committed — transaction is valid
	go func() {
		for _, key := range txn.SecondaryKeys {
			if err := c.commitKey(ctx, txn, key, false); err != nil {
				// Log error, but don't fail the transaction
				// The key can be cleaned up later
				fmt.Printf("[%s] commit secondary %s failed: %v\n",
					txn.ID, key, err)
			}
		}
	}()

	txn.mu.Lock()
	txn.State = TxnCommitted
	txn.mu.Unlock()

	return nil
}

// commitKey commits a single key
func (c *PercolatorClient) commitKey(
	ctx context.Context,
	txn *PercolatorTransaction,
	key string,
	isPrimary bool,
) error {
	fmt.Printf("[%s] COMMIT %s (primary=%v)\n", txn.ID, key, isPrimary)

	// In production:
	// 1. Write commit timestamp to key
	// 2. Make value visible to readers
	// 3. Remove lock

	time.Sleep(5 * time.Millisecond)

	return nil
}

// Abort aborts the transaction and releases all locks
func (c *PercolatorClient) Abort(
	ctx context.Context,
	txn *PercolatorTransaction,
) error {
	c.Cleanup(ctx, txn)

	txn.mu.Lock()
	txn.State = TxnAborted
	txn.mu.Unlock()

	return nil
}

// Cleanup releases all locks held by the transaction
func (c *PercolatorClient) Cleanup(
	ctx context.Context,
	txn *PercolatorTransaction,
) {
	fmt.Printf("[%s] CLEANUP (abort)\n", txn.ID)

	// In production: release all locks, discard write intents

	txn.mu.Lock()
	for key := range txn.WriteSet {
		delete(txn.prewriteSuccess, key)
	}
	txn.mu.Unlock()
}

// CheckAndResolve checks a lock and resolves it if stale
// This is used by readers who encounter a lock
//
// When a reader encounters a lock:
//  1. Check the primary key's status
//  2. If primary is committed: complete the commit
//  3. If primary is not committed: check if lock is expired
//  4. If expired: clean up the lock and retry
func (c *PercolatorClient) CheckAndResolve(
	ctx context.Context,
	key string,
	lock *Lock,
) (resolved bool, err error) {
	fmt.Printf("Checking lock on %s (txn=%s)\n", key, lock.TxnID)

	// Check if lock is expired
	if time.Now().After(lock.ExpiresAt) {
		// Lock is stale — check primary key
		fmt.Printf("Lock expired, checking primary %s\n", lock.Primary)

		// In production: read primary key to determine txn status
		// For demo, we simulate

		// If primary not committed, we can clean up this lock
		return true, nil
	}

	// Lock is still valid — reader must wait or abort
	return false, fmt.Errorf("txn: key %s is locked by %s", key, lock.TxnID)
}

// generateTxnID creates a unique transaction ID
func (c *PercolatorClient) generateTxnID() TxnID {
	ts := c.hlc.Now()
	return TxnID(fmt.Sprintf("percolator-%d-%d", ts, time.Now().UnixNano()))
}
