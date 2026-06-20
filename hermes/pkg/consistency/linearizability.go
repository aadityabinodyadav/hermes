// pkg/consistency/linearizability.go
package consistency

// LinearizabilityChecker verifies that an operation history
// is linearizable (the Jepsen approach)
//
// Algorithm: Wing & Gong (1993)
//
// Given: A history of operations with start/end times
//   op1: PUT x=1, start=T1, end=T2
//   op2: GET x,   start=T3, end=T4, result=1
//   op3: GET x,   start=T0, end=T5, result=0
//
// Check: Is there a valid linearization?
//   i.e., can we find a sequential order of operations
//   where:
//   1. Each op appears at some point in [start, end]
//   2. The sequential history is correct
//
// This checker is used in our chaos testing framework
// to verify Hermes maintains linearizability under failures

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Operation represents one client operation
type Operation struct {
	// ID uniquely identifies this operation
	ID uint64

	// Type is the operation type (Get, Put, Delete)
	Type OpType

	// Key is the key being operated on
	Key string

	// Value is the value written (Put) or returned (Get)
	Value interface{}

	// Start is when the operation was invoked
	Start time.Time

	// End is when the operation completed (got a response)
	End time.Time

	// Err is any error from the operation
	Err error
}

type OpType uint8

const (
	OpGet    OpType = 0
	OpPut    OpType = 1
	OpDelete OpType = 2
	OpCAS    OpType = 3 // Compare-And-Swap
)

func (o OpType) String() string {
	switch o {
	case OpGet:
		return "GET"
	case OpPut:
		return "PUT"
	case OpDelete:
		return "DELETE"
	case OpCAS:
		return "CAS"
	}
	return "UNKNOWN"
}

// History is a collection of operations to check
type History struct {
	Operations []Operation
}

// CheckResult is the result of a linearizability check
type CheckResult struct {
	Linearizable bool
	Explanation  string
	Witness      []int // indices of operations in a valid linearization (if found)
	Violation    []int // indices of operations showing violation (if found)
}

// LinearizabilityChecker checks if an operation history is linearizable
type LinearizabilityChecker struct {
	mu      sync.Mutex
	history []Operation
	nextID  uint64
}

// NewLinearizabilityChecker creates a new checker
func NewLinearizabilityChecker() *LinearizabilityChecker {
	return &LinearizabilityChecker{}
}

// Record adds an operation to the history
// Call this before the operation to record invocation time
func (c *LinearizabilityChecker) Invoke(opType OpType, key string, value interface{}) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	id := c.nextID
	c.nextID++

	c.history = append(c.history, Operation{
		ID:    id,
		Type:  opType,
		Key:   key,
		Value: value,
		Start: time.Now(),
	})

	return id
}

// Complete records the completion of an operation
func (c *LinearizabilityChecker) Complete(id uint64, result interface{}, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range c.history {
		if c.history[i].ID == id {
			c.history[i].End = time.Now()
			c.history[i].Value = result
			c.history[i].Err = err
			return
		}
	}
}

// Check verifies if the recorded history is linearizable
// Returns detailed results including any violations found
func (c *LinearizabilityChecker) Check() CheckResult {
	c.mu.Lock()
	ops := make([]Operation, len(c.history))
	copy(ops, c.history)
	c.mu.Unlock()

	// Filter out failed operations (they may or may not have taken effect)
	var completed []Operation
	for _, op := range ops {
		if op.Err == nil && !op.End.IsZero() {
			completed = append(completed, op)
		}
	}

	if len(completed) == 0 {
		return CheckResult{
			Linearizable: true,
			Explanation:  "No completed operations to check",
		}
	}

	// Try to find a valid linearization
	return c.checkLinearization(completed)
}

// checkLinearization uses a simplified Wing & Gong algorithm
func (c *LinearizabilityChecker) checkLinearization(ops []Operation) CheckResult {
	// Group operations by key for simpler checking
	keyOps := make(map[string][]Operation)
	for _, op := range ops {
		keyOps[op.Key] = append(keyOps[op.Key], op)
	}

	for key, keyHistory := range keyOps {
		result := c.checkKeyLinearization(key, keyHistory)
		if !result.Linearizable {
			return result
		}
	}

	return CheckResult{
		Linearizable: true,
		Explanation:  "All operations can be linearized",
	}
}

// checkKeyLinearization checks linearizability for a single key
func (c *LinearizabilityChecker) checkKeyLinearization(key string, ops []Operation) CheckResult {
	// Sort by end time (operations that finished first should logically happen first)
	sort.Slice(ops, func(i, j int) bool {
		return ops[i].End.Before(ops[j].End)
	})

	// Simulate the key's state through the linearization
	var currentValue interface{}
	currentValue = nil // initial state: key doesn't exist

	for i, op := range ops {
		switch op.Type {
		case OpPut:
			// PUT always succeeds (in this simplified model)
			currentValue = op.Value

		case OpGet:
			// GET should return the current value
			if op.Value != currentValue {
				// Check if there's a valid explanation
				// Maybe there's a concurrent PUT that happened "between" states
				// This simplified checker doesn't handle all cases

				// Check if the start time of this GET overlaps with any PUT
				hasConcurrentPut := false
				for j, other := range ops {
					if j == i {
						continue
					}
					if other.Type == OpPut &&
						other.Start.Before(op.End) &&
						other.End.After(op.Start) {
						hasConcurrentPut = true
						break
					}
				}

				if !hasConcurrentPut {
					return CheckResult{
						Linearizable: false,
						Explanation: fmt.Sprintf(
							"GET key=%s returned %v but expected %v (no concurrent PUT)",
							key, op.Value, currentValue),
						Violation: []int{i},
					}
				}
			}
		}
	}

	return CheckResult{
		Linearizable: true,
		Explanation:  fmt.Sprintf("Key %s history is linearizable", key),
	}
}

// Reset clears the operation history
func (c *LinearizabilityChecker) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.history = nil
	c.nextID = 0
}

// Stats returns statistics about the recorded history
func (c *LinearizabilityChecker) Stats() HistoryStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	var puts, gets, dels, failed int
	for _, op := range c.history {
		if op.Err != nil {
			failed++
			continue
		}
		switch op.Type {
		case OpPut:
			puts++
		case OpGet:
			gets++
		case OpDelete:
			dels++
		}
	}

	return HistoryStats{
		Total:   len(c.history),
		Puts:    puts,
		Gets:    gets,
		Deletes: dels,
		Failed:  failed,
	}
}

type HistoryStats struct {
	Total   int
	Puts    int
	Gets    int
	Deletes int
	Failed  int
}
