// pkg/partition/consistent_hash.go
package partition

// ConsistentHash implements consistent hashing with virtual nodes
//
// WHY CONSISTENT HASHING:
//   Standard hash partitioning: partition = hash(key) % N
//   Problem: when N changes (node added/removed),
//            MOST keys get reassigned (on average (N-1)/N of all keys)
//
//   Consistent hashing: when N changes,
//            only 1/N keys get reassigned (minimum possible!)
//
// HOW IT WORKS:
//   1. Hash the ring space: [0, 2^32)
//   2. Place nodes on ring: hash(nodeID) → position
//   3. For each key: hash(key) → position → find next node clockwise
//
//   Node removal: keys that pointed to removed node
//                 now point to the NEXT node clockwise
//                 Only those keys move → minimal disruption
//
// VIRTUAL NODES (vnodes):
//   Problem with basic consistent hashing:
//     hash(node1) = 0°, hash(node2) = 179°, hash(node3) = 181°
//     node1 owns 179°/360° = 49.7% of keyspace (too much!)
//     node2 owns 2°/360°   = 0.5% of keyspace (too little!)
//
//   Solution: each physical node has V virtual nodes
//     hash("node1-vnode-0") = 10°
//     hash("node1-vnode-1") = 130°
//     hash("node1-vnode-2") = 250°
//     → node1 owns ~30% (more balanced)
//
//   With V=150 vnodes: max imbalance ≈ 5% (acceptable)

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

const (
	// DefaultVirtualNodes is the default number of vnodes per physical node
	// Higher = better balance but more memory for ring
	// 150 is what Cassandra uses by default
	DefaultVirtualNodes = 150
)

// VirtualNode is a single point on the consistent hash ring
type VirtualNode struct {
	// Hash is the position on the ring [0, 2^32)
	Hash uint32

	// NodeID is the physical node this vnode belongs to
	NodeID string

	// VNodeIndex is which vnode this is for the physical node
	VNodeIndex int
}

// ConsistentHashRing implements the consistent hash ring
type ConsistentHashRing struct {
	mu           sync.RWMutex
	vnodes       []VirtualNode   // sorted by Hash
	nodeMap      map[string]bool // physical nodes
	virtualNodes int             // vnodes per physical node
}

// NewConsistentHashRing creates a new consistent hash ring
func NewConsistentHashRing(virtualNodes int) *ConsistentHashRing {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}
	return &ConsistentHashRing{
		nodeMap:      make(map[string]bool),
		virtualNodes: virtualNodes,
	}
}

// AddNode adds a physical node to the ring
// Creates virtualNodes virtual nodes for it
func (r *ConsistentHashRing) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.nodeMap[nodeID] {
		return // already exists
	}

	r.nodeMap[nodeID] = true

	// Create virtual nodes
	for i := 0; i < r.virtualNodes; i++ {
		vnode := VirtualNode{
			Hash:       r.hashKey(fmt.Sprintf("%s-vnode-%d", nodeID, i)),
			NodeID:     nodeID,
			VNodeIndex: i,
		}
		r.vnodes = append(r.vnodes, vnode)
	}

	// Keep sorted by hash for binary search
	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].Hash < r.vnodes[j].Hash
	})

	fmt.Printf("ConsistentHash: added node %s (%d vnodes, ring size=%d)\n",
		nodeID, r.virtualNodes, len(r.vnodes))
}

// RemoveNode removes a physical node from the ring
// Its virtual nodes are removed; its keys route to the next node
func (r *ConsistentHashRing) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.nodeMap[nodeID] {
		return
	}

	delete(r.nodeMap, nodeID)

	// Remove all virtual nodes for this physical node
	remaining := r.vnodes[:0]
	for _, vn := range r.vnodes {
		if vn.NodeID != nodeID {
			remaining = append(remaining, vn)
		}
	}
	r.vnodes = remaining

	fmt.Printf("ConsistentHash: removed node %s (ring size=%d)\n",
		nodeID, len(r.vnodes))
}

// GetNode returns the node responsible for the given key
// O(log n) binary search on the ring
func (r *ConsistentHashRing) GetNode(key string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return "", fmt.Errorf("consistent hash: ring is empty")
	}

	hash := r.hashKey(key)

	// Find first vnode with Hash >= hash (clockwise search)
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].Hash >= hash
	})

	// Wrap around: if hash > all vnodes, use the first vnode
	if idx == len(r.vnodes) {
		idx = 0
	}

	return r.vnodes[idx].NodeID, nil
}

// GetNodes returns the N nodes responsible for the key
// Used for replication: key is replicated to N distinct physical nodes
// starting from the key's primary node and going clockwise
func (r *ConsistentHashRing) GetNodes(key string, count int) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodeMap) == 0 {
		return nil, fmt.Errorf("consistent hash: ring is empty")
	}

	if count > len(r.nodeMap) {
		count = len(r.nodeMap)
	}

	hash := r.hashKey(key)

	// Find starting position
	startIdx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].Hash >= hash
	})
	if startIdx == len(r.vnodes) {
		startIdx = 0
	}

	// Walk clockwise, collecting distinct physical nodes
	seen := make(map[string]bool)
	var nodes []string

	for i := 0; len(nodes) < count; i++ {
		idx := (startIdx + i) % len(r.vnodes)
		nodeID := r.vnodes[idx].NodeID

		if !seen[nodeID] {
			seen[nodeID] = true
			nodes = append(nodes, nodeID)
		}

		// Safety: prevent infinite loop
		if i > len(r.vnodes)*2 {
			break
		}
	}

	return nodes, nil
}

// Distribution analyzes how evenly keys would be distributed
// Returns: nodeID → percentage of ring owned
func (r *ConsistentHashRing) Distribution() map[string]float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return nil
	}

	// Count how many ring "slots" each node owns
	// Slot = space between consecutive vnodes
	nodeSlots := make(map[string]uint64)

	for i := 0; i < len(r.vnodes); i++ {
		curr := r.vnodes[i]
		var slotSize uint64

		if i < len(r.vnodes)-1 {
			next := r.vnodes[i+1]
			slotSize = uint64(next.Hash - curr.Hash)
		} else {
			// Last vnode wraps around to first
			slotSize = uint64(^uint32(0) - curr.Hash + r.vnodes[0].Hash)
		}

		nodeSlots[curr.NodeID] += slotSize
	}

	// Convert to percentages
	result := make(map[string]float64)
	total := uint64(^uint32(0))
	for nodeID, slots := range nodeSlots {
		result[nodeID] = float64(slots) / float64(total) * 100
	}

	return result
}

// Nodes returns all physical nodes in the ring
func (r *ConsistentHashRing) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]string, 0, len(r.nodeMap))
	for nodeID := range r.nodeMap {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

// hashKey hashes a string key to a uint32 position on the ring
// Using MD5 for uniform distribution
// Production: use Murmur3 or xxHash for better performance
func (r *ConsistentHashRing) hashKey(key string) uint32 {
	h := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(h[:4])
}

// RingVisualization returns a text visualization of the ring
func (r *ConsistentHashRing) RingVisualization() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return "Ring is empty"
	}

	// Show first 20 vnodes as a sample
	result := fmt.Sprintf("Ring (%d vnodes, %d physical nodes):\n",
		len(r.vnodes), len(r.nodeMap))

	limit := 20
	if len(r.vnodes) < limit {
		limit = len(r.vnodes)
	}

	for i := 0; i < limit; i++ {
		vn := r.vnodes[i]
		pct := float64(vn.Hash) / float64(^uint32(0)) * 360
		result += fmt.Sprintf("  [%.1f°] %s (vnode-%d)\n",
			pct, vn.NodeID, vn.VNodeIndex)
	}

	if len(r.vnodes) > limit {
		result += fmt.Sprintf("  ... and %d more vnodes\n",
			len(r.vnodes)-limit)
	}

	return result
}
