// pkg/consistency/crdt.go
package consistency

// CRDTs: Conflict-free Replicated Data Types
//
// The key property: any two replicas can be merged
// and the result is the same, regardless of merge order.
//
// Mathematical foundation: join-semilattice
// merge(a, b) = merge(b, a)          (commutativity)
// merge(a, merge(b, c)) = merge(merge(a,b), c) (associativity)
// merge(a, a) = a                     (idempotency)
//
// These properties guarantee convergence:
// No matter what order replicas exchange updates,
// they all end up with the same state.
//
// Hermes uses CRDTs for:
//   - Cluster membership (who's in the cluster)
//   - Configuration flags (enabled/disabled features)
//   - Statistics (request counts, error counts)
//   - Rate limiting tokens (approximate)

import (
	"sort"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// G-COUNTER (Grow-only Counter)
//
// Can only increment. Perfect for:
//   - Request counts
//   - Error counts
//   - Bytes transferred
//
// State: map[nodeID]count
// Value: sum of all counts
// Merge: take max of each node's count
//
// WHY MAX AND NOT SUM?
//   If node-1 has count=5, and we receive a state where node-1=3,
//   we're seeing an OLDER state. We take max to keep the highest seen value.
//   We DON'T sum because that would double-count.
// ─────────────────────────────────────────────────────────────────────────────

// GCounter is a grow-only counter CRDT
type GCounter struct {
	mu     sync.RWMutex
	nodeID string
	counts map[string]uint64 // nodeID → count
}

// NewGCounter creates a new grow-only counter
func NewGCounter(nodeID string) *GCounter {
	return &GCounter{
		nodeID: nodeID,
		counts: map[string]uint64{nodeID: 0},
	}
}

// Increment increases this node's count by n
func (c *GCounter) Increment(n uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[c.nodeID] += n
}

// Value returns the total count across all nodes
func (c *GCounter) Value() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := uint64(0)
	for _, count := range c.counts {
		total += count
	}
	return total
}

// Merge incorporates another counter's state
// Idempotent: merge(c, c) = c
// Commutative: merge(a, b) = merge(b, a)
func (c *GCounter) Merge(other *GCounter) {
	c.mu.Lock()
	defer c.mu.Unlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	for nodeID, count := range other.counts {
		if existing, ok := c.counts[nodeID]; !ok || count > existing {
			c.counts[nodeID] = count
		}
	}
}

// State returns a copy of the current state (for sending to peers)
func (c *GCounter) State() map[string]uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	state := make(map[string]uint64, len(c.counts))
	for k, v := range c.counts {
		state[k] = v
	}
	return state
}

// ─────────────────────────────────────────────────────────────────────────────
// PN-COUNTER (Positive-Negative Counter)
//
// Can both increment AND decrement.
// Implemented as two G-Counters: positive and negative.
// Value = positive.Value() - negative.Value()
//
// Use for: inventory levels, balance approximations, etc.
// ─────────────────────────────────────────────────────────────────────────────

// PNCounter is a CRDT counter that supports both increment and decrement
type PNCounter struct {
	mu       sync.RWMutex
	nodeID   string
	positive *GCounter
	negative *GCounter
}

// NewPNCounter creates a new PN-Counter
func NewPNCounter(nodeID string) *PNCounter {
	return &PNCounter{
		nodeID:   nodeID,
		positive: NewGCounter(nodeID),
		negative: NewGCounter(nodeID),
	}
}

// Increment increases the counter by n
func (c *PNCounter) Increment(n uint64) {
	c.positive.Increment(n)
}

// Decrement decreases the counter by n
func (c *PNCounter) Decrement(n uint64) {
	c.negative.Increment(n)
}

// Value returns the current counter value (may be negative)
func (c *PNCounter) Value() int64 {
	return int64(c.positive.Value()) - int64(c.negative.Value())
}

// Merge incorporates another counter's state
func (c *PNCounter) Merge(other *PNCounter) {
	c.positive.Merge(other.positive)
	c.negative.Merge(other.negative)
}

// ─────────────────────────────────────────────────────────────────────────────
// LWW-REGISTER (Last-Write-Wins Register)
//
// A register that resolves conflicts by keeping the most recently written value.
// "Most recent" determined by timestamp.
//
// Use for: configuration values, feature flags, user preferences
//
// Risk: concurrent writes from clocks with skew
// Solution: Use HLC timestamps for bounded skew
// ─────────────────────────────────────────────────────────────────────────────

// LWWRegister is a last-write-wins register CRDT
type LWWRegister struct {
	mu        sync.RWMutex
	nodeID    string
	value     interface{}
	timestamp int64  // unix nanoseconds
	writerID  string // for tie-breaking (node ID)
}

// NewLWWRegister creates a new LWW register
func NewLWWRegister(nodeID string) *LWWRegister {
	return &LWWRegister{
		nodeID:    nodeID,
		timestamp: 0,
	}
}

// Set writes a new value
func (r *LWWRegister) Set(value interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.value = value
	r.timestamp = time.Now().UnixNano()
	r.writerID = r.nodeID
}

// SetAt writes a value with an explicit timestamp
// Use with HLC timestamps for better distributed behavior
func (r *LWWRegister) SetAt(value interface{}, ts int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ts > r.timestamp || (ts == r.timestamp && r.nodeID > r.writerID) {
		r.value = value
		r.timestamp = ts
		r.writerID = r.nodeID
	}
}

// Get returns the current value and its timestamp
func (r *LWWRegister) Get() (interface{}, int64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.value, r.timestamp
}

// Merge takes the most recent value from two registers
func (r *LWWRegister) Merge(other *LWWRegister) {
	r.mu.Lock()
	defer r.mu.Unlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	// Higher timestamp wins
	if other.timestamp > r.timestamp {
		r.value = other.value
		r.timestamp = other.timestamp
		r.writerID = other.writerID
	} else if other.timestamp == r.timestamp && other.writerID > r.writerID {
		// Tie-break by writer ID (arbitrary but consistent)
		r.value = other.value
		r.writerID = other.writerID
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// OR-SET (Observed-Remove Set)
//
// A set where elements can be added and removed.
// Handles the add-remove-add case correctly.
//
// Problem with naive approach:
//   Node 1: Add "alice"  → {alice}
//   Node 2: Remove "alice" → {}
//   Concurrent merge: which wins?
//
// OR-Set solution:
//   Each add gives a unique tag: (element, tag)
//   Remove marks all existing tags for that element as removed
//   An element is "in the set" if it has at least one non-removed tag
//
//   Node 1: Add "alice" → {(alice, tag1)}
//   Node 2: Concurrent add "alice" → {(alice, tag2)}
//   Node 2: Remove "alice" → remove all existing tags → {(alice, tag2, REMOVED)}
//   Merge: (alice,tag1) not removed + (alice,tag2) removed
//         → alice IS in the set (tag1 not removed)
//
// This resolves: "concurrent add wins over remove"
// ─────────────────────────────────────────────────────────────────────────────

// ORSet is an observed-remove set CRDT
type ORSet struct {
	mu      sync.RWMutex
	nodeID  string
	nextTag uint64

	// elements: element → {tag → removed}
	elements map[string]map[uint64]bool // element → tag → isRemoved
}

// NewORSet creates a new OR-Set
func NewORSet(nodeID string) *ORSet {
	return &ORSet{
		nodeID:   nodeID,
		elements: make(map[string]map[uint64]bool),
	}
}

// Add adds an element to the set
// Each add creates a unique tag
func (s *ORSet) Add(element string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextTag++
	if s.elements[element] == nil {
		s.elements[element] = make(map[uint64]bool)
	}
	s.elements[element][s.nextTag] = false // false = not removed
}

// Remove removes an element from the set
// Marks all current tags for this element as removed
// (But leaves any FUTURE adds intact)
func (s *ORSet) Remove(element string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tags, exists := s.elements[element]; exists {
		for tag := range tags {
			s.elements[element][tag] = true // true = removed
		}
	}
}

// Contains returns true if the element is in the set
// An element is in the set if it has at least one non-removed tag
func (s *ORSet) Contains(element string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tags, exists := s.elements[element]
	if !exists {
		return false
	}

	for _, removed := range tags {
		if !removed {
			return true // at least one non-removed tag
		}
	}

	return false
}

// Elements returns all elements currently in the set
func (s *ORSet) Elements() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []string
	for element, tags := range s.elements {
		for _, removed := range tags {
			if !removed {
				result = append(result, element)
				break
			}
		}
	}

	sort.Strings(result)
	return result
}

// Merge combines two OR-Sets
func (s *ORSet) Merge(other *ORSet) {
	s.mu.Lock()
	defer s.mu.Unlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	for element, otherTags := range other.elements {
		if s.elements[element] == nil {
			s.elements[element] = make(map[uint64]bool)
		}

		for tag, removed := range otherTags {
			if existing, exists := s.elements[element][tag]; exists {
				// If either side removed it, it's removed
				s.elements[element][tag] = existing || removed
			} else {
				s.elements[element][tag] = removed
			}
		}
	}
}
