// pkg/chaos/fault_injector.go
package chaos

// FaultInjector is the central controller for all chaos in Hermes
//
// It provides:
//   - Network fault injection (partition, delay, drop)
//   - Process fault injection (crash, pause)
//   - Storage fault injection (corruption, delay, full)
//   - Clock fault injection (skew, jump)
//
// Used in tests via: chaos.With(scenario, func() { runTest() })
//
// Architecture:
//   FaultInjector sits between components and intercepts calls
//   "Should this network call succeed?" → check active faults
//   "Should this disk write succeed?" → check active faults
//   "What time is it?" → check clock manipulations

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// FaultType categorizes injected faults
type FaultType string

const (
	FaultNetworkPartition FaultType = "network_partition"
	FaultNetworkDelay     FaultType = "network_delay"
	FaultNetworkDrop      FaultType = "network_drop"
	FaultNodeCrash        FaultType = "node_crash"
	FaultNodePause        FaultType = "node_pause"
	FaultDiskCorruption   FaultType = "disk_corruption"
	FaultDiskFull         FaultType = "disk_full"
	FaultDiskSlow         FaultType = "disk_slow"
	FaultClockSkew        FaultType = "clock_skew"
	FaultClockJump        FaultType = "clock_jump"
)

// Fault describes one injected fault
type Fault struct {
	ID          string
	Type        FaultType
	Description string
	StartedAt   time.Time
	EndsAt      time.Time // zero = indefinite
	Affects     []string  // nodeIDs affected
	Params      map[string]interface{}

	// Active controls whether this fault is currently active
	Active bool
}

// FaultStats tracks what the injector has done
type FaultStats struct {
	mu                  sync.Mutex
	TotalFaultsInjected int64
	NetworkDrops        int64
	NetworkDelays       int64
	NetworkPartitions   int64
	NodeCrashes         int64
	DiskErrors          int64
	ClockManipulations  int64
}

func (s *FaultStats) Incr(faultType FaultType) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalFaultsInjected++
	switch faultType {
	case FaultNetworkDrop:
		s.NetworkDrops++
	case FaultNetworkDelay:
		s.NetworkDelays++
	case FaultNetworkPartition:
		s.NetworkPartitions++
	case FaultNodeCrash:
		s.NodeCrashes++
	case FaultDiskCorruption, FaultDiskFull, FaultDiskSlow:
		s.DiskErrors++
	case FaultClockSkew, FaultClockJump:
		s.ClockManipulations++
	}
}

// FaultInjector is the main chaos controller
type FaultInjector struct {
	mu     sync.RWMutex
	faults map[string]*Fault
	stats  *FaultStats
	rng    *rand.Rand
	active int32 // atomic: 1 if injector is active

	// Callbacks for when faults are injected/cleared
	onFaultInjected func(f *Fault)
	onFaultCleared  func(f *Fault)
}

// NewFaultInjector creates a new fault injector
func NewFaultInjector() *FaultInjector {
	return &FaultInjector{
		faults: make(map[string]*Fault),
		stats:  &FaultStats{},
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Activate enables the fault injector
func (fi *FaultInjector) Activate() {
	atomic.StoreInt32(&fi.active, 1)
}

// Deactivate disables the fault injector
func (fi *FaultInjector) Deactivate() {
	atomic.StoreInt32(&fi.active, 0)
}

// IsActive returns true if the injector is running
func (fi *FaultInjector) IsActive() bool {
	return atomic.LoadInt32(&fi.active) == 1
}

// InjectFault adds a new fault to the system
func (fi *FaultInjector) InjectFault(fault *Fault) {
	fi.mu.Lock()
	defer fi.mu.Unlock()

	fault.StartedAt = time.Now()
	fault.Active = true
	fi.faults[fault.ID] = fault
	fi.stats.Incr(fault.Type)

	fmt.Printf("🔴 FAULT INJECTED: [%s] %s → %v\n",
		fault.Type, fault.ID, fault.Affects)

	if fi.onFaultInjected != nil {
		go fi.onFaultInjected(fault)
	}
}

// ClearFault removes a specific fault
func (fi *FaultInjector) ClearFault(faultID string) {
	fi.mu.Lock()
	defer fi.mu.Unlock()

	fault, exists := fi.faults[faultID]
	if !exists {
		return
	}

	fault.Active = false
	delete(fi.faults, faultID)

	fmt.Printf("🟢 FAULT CLEARED: [%s] %s\n", fault.Type, faultID)

	if fi.onFaultCleared != nil {
		go fi.onFaultCleared(fault)
	}
}

// ClearAll removes all active faults
func (fi *FaultInjector) ClearAll() {
	fi.mu.Lock()
	ids := make([]string, 0, len(fi.faults))
	for id := range fi.faults {
		ids = append(ids, id)
	}
	fi.mu.Unlock()

	for _, id := range ids {
		fi.ClearFault(id)
	}

	fmt.Println("🟢 ALL FAULTS CLEARED — system restored to normal")
}

// ─────────────────────────────────────────────────────────────────────────────
// NETWORK FAULT INJECTION
// ─────────────────────────────────────────────────────────────────────────────

// PartitionNodes creates a bidirectional network partition
func (fi *FaultInjector) PartitionNodes(groupA, groupB []string) string {
	faultID := fmt.Sprintf("partition-%d", time.Now().UnixNano())

	fi.InjectFault(&Fault{
		ID:          faultID,
		Type:        FaultNetworkPartition,
		Description: fmt.Sprintf("Partition %v ↔ %v", groupA, groupB),
		Affects:     append(groupA, groupB...),
		Params: map[string]interface{}{
			"groupA": groupA,
			"groupB": groupB,
		},
	})

	return faultID
}

// DropPackets configures packet loss between nodes
func (fi *FaultInjector) DropPackets(from, to string, dropRate float64) string {
	faultID := fmt.Sprintf("drop-%s-%s-%d", from, to, time.Now().UnixNano())

	fi.InjectFault(&Fault{
		ID:          faultID,
		Type:        FaultNetworkDrop,
		Description: fmt.Sprintf("%.0f%% packet drop %s → %s", dropRate*100, from, to),
		Affects:     []string{from, to},
		Params: map[string]interface{}{
			"from":      from,
			"to":        to,
			"drop_rate": dropRate,
		},
	})

	return faultID
}

// AddLatency adds artificial latency to a connection
func (fi *FaultInjector) AddLatency(from, to string, minDelay, maxDelay time.Duration) string {
	faultID := fmt.Sprintf("latency-%s-%s-%d", from, to, time.Now().UnixNano())

	fi.InjectFault(&Fault{
		ID:   faultID,
		Type: FaultNetworkDelay,
		Description: fmt.Sprintf("Latency %v-%v %s → %s",
			minDelay, maxDelay, from, to),
		Affects: []string{from, to},
		Params: map[string]interface{}{
			"from":      from,
			"to":        to,
			"min_delay": minDelay,
			"max_delay": maxDelay,
		},
	})

	return faultID
}

// ShouldDrop checks if a packet should be dropped
// Called by the network transport layer
func (fi *FaultInjector) ShouldDrop(from, to string) bool {
	if !fi.IsActive() {
		return false
	}

	fi.mu.RLock()
	defer fi.mu.RUnlock()

	for _, fault := range fi.faults {
		if !fault.Active {
			continue
		}

		switch fault.Type {
		case FaultNetworkPartition:
			groupA, okA := fault.Params["groupA"].([]string)
			groupB, okB := fault.Params["groupB"].([]string)
			if !okA || !okB {
				continue
			}
			if (contains(groupA, from) && contains(groupB, to)) ||
				(contains(groupB, from) && contains(groupA, to)) {
				return true
			}

		case FaultNetworkDrop:
			f, _ := fault.Params["from"].(string)
			t, _ := fault.Params["to"].(string)
			if f == from && t == to {
				dropRate, _ := fault.Params["drop_rate"].(float64)
				return fi.rng.Float64() < dropRate
			}
		}
	}

	return false
}

// GetDelay returns any additional delay for a connection
func (fi *FaultInjector) GetDelay(from, to string) time.Duration {
	if !fi.IsActive() {
		return 0
	}

	fi.mu.RLock()
	defer fi.mu.RUnlock()

	for _, fault := range fi.faults {
		if !fault.Active || fault.Type != FaultNetworkDelay {
			continue
		}

		f, _ := fault.Params["from"].(string)
		t, _ := fault.Params["to"].(string)

		if f == from && t == to {
			minDelay, _ := fault.Params["min_delay"].(time.Duration)
			maxDelay, _ := fault.Params["max_delay"].(time.Duration)

			if maxDelay > minDelay {
				return minDelay + time.Duration(fi.rng.Int63n(int64(maxDelay-minDelay)))
			}
			return minDelay
		}
	}

	return 0
}

// ─────────────────────────────────────────────────────────────────────────────
// NODE FAULT INJECTION
// ─────────────────────────────────────────────────────────────────────────────

// CrashNode simulates a node crash
func (fi *FaultInjector) CrashNode(nodeID string) string {
	faultID := fmt.Sprintf("crash-%s-%d", nodeID, time.Now().UnixNano())

	fi.InjectFault(&Fault{
		ID:          faultID,
		Type:        FaultNodeCrash,
		Description: fmt.Sprintf("Node %s crashed", nodeID),
		Affects:     []string{nodeID},
		Params: map[string]interface{}{
			"node_id": nodeID,
		},
	})

	return faultID
}

// PauseNode simulates a node pause (like a GC pause or overloaded system)
func (fi *FaultInjector) PauseNode(nodeID string, duration time.Duration) string {
	faultID := fmt.Sprintf("pause-%s-%d", nodeID, time.Now().UnixNano())

	fi.InjectFault(&Fault{
		ID:   faultID,
		Type: FaultNodePause,
		Description: fmt.Sprintf("Node %s paused for %v (GC/overload simulation)",
			nodeID, duration),
		Affects: []string{nodeID},
		EndsAt:  time.Now().Add(duration),
		Params: map[string]interface{}{
			"node_id":  nodeID,
			"duration": duration,
		},
	})

	// Auto-clear after duration
	go func() {
		time.Sleep(duration)
		fi.ClearFault(faultID)
	}()

	return faultID
}

// IsCrashed returns true if a node is currently crashed
func (fi *FaultInjector) IsCrashed(nodeID string) bool {
	if !fi.IsActive() {
		return false
	}

	fi.mu.RLock()
	defer fi.mu.RUnlock()

	for _, fault := range fi.faults {
		if !fault.Active || fault.Type != FaultNodeCrash {
			continue
		}
		n, _ := fault.Params["node_id"].(string)
		if n == nodeID {
			return true
		}
	}
	return false
}

// IsPaused returns true if a node is currently paused
func (fi *FaultInjector) IsPaused(nodeID string) bool {
	if !fi.IsActive() {
		return false
	}

	fi.mu.RLock()
	defer fi.mu.RUnlock()

	for _, fault := range fi.faults {
		if !fault.Active || fault.Type != FaultNodePause {
			continue
		}
		n, _ := fault.Params["node_id"].(string)
		if n == nodeID {
			if !fault.EndsAt.IsZero() && time.Now().After(fault.EndsAt) {
				return false // expired
			}
			return true
		}
	}
	return false
}

// ─────────────────────────────────────────────────────────────────────────────
// CLOCK FAULT INJECTION
// ─────────────────────────────────────────────────────────────────────────────

// SkewClock makes a node's clock run fast or slow
func (fi *FaultInjector) SkewClock(nodeID string, skew time.Duration) string {
	faultID := fmt.Sprintf("skew-%s-%d", nodeID, time.Now().UnixNano())

	fi.InjectFault(&Fault{
		ID:          faultID,
		Type:        FaultClockSkew,
		Description: fmt.Sprintf("Clock skew %v on %s", skew, nodeID),
		Affects:     []string{nodeID},
		Params: map[string]interface{}{
			"node_id": nodeID,
			"skew":    skew,
		},
	})

	return faultID
}

// GetClockSkew returns the current clock skew for a node
func (fi *FaultInjector) GetClockSkew(nodeID string) time.Duration {
	if !fi.IsActive() {
		return 0
	}

	fi.mu.RLock()
	defer fi.mu.RUnlock()

	for _, fault := range fi.faults {
		if !fault.Active || fault.Type != FaultClockSkew {
			continue
		}
		n, _ := fault.Params["node_id"].(string)
		if n == nodeID {
			skew, _ := fault.Params["skew"].(time.Duration)
			return skew
		}
	}
	return 0
}

// ─────────────────────────────────────────────────────────────────────────────
// DISK FAULT INJECTION
// ─────────────────────────────────────────────────────────────────────────────

// CorruptDisk causes disk reads to return corrupted data
func (fi *FaultInjector) CorruptDisk(nodeID string, corruptRate float64) string {
	faultID := fmt.Sprintf("corrupt-%s-%d", nodeID, time.Now().UnixNano())

	fi.InjectFault(&Fault{
		ID:   faultID,
		Type: FaultDiskCorruption,
		Description: fmt.Sprintf("%.0f%% disk corruption on %s",
			corruptRate*100, nodeID),
		Affects: []string{nodeID},
		Params: map[string]interface{}{
			"node_id":      nodeID,
			"corrupt_rate": corruptRate,
		},
	})

	return faultID
}

// SlowDisk makes disk I/O artificially slow
func (fi *FaultInjector) SlowDisk(nodeID string, delay time.Duration) string {
	faultID := fmt.Sprintf("slowdisk-%s-%d", nodeID, time.Now().UnixNano())

	fi.InjectFault(&Fault{
		ID:          faultID,
		Type:        FaultDiskSlow,
		Description: fmt.Sprintf("Disk slowed by %v on %s", delay, nodeID),
		Affects:     []string{nodeID},
		Params: map[string]interface{}{
			"node_id": nodeID,
			"delay":   delay,
		},
	})

	return faultID
}

// ShouldCorrupt checks if disk data should be corrupted
func (fi *FaultInjector) ShouldCorrupt(nodeID string) bool {
	if !fi.IsActive() {
		return false
	}

	fi.mu.RLock()
	defer fi.mu.RUnlock()

	for _, fault := range fi.faults {
		if !fault.Active || fault.Type != FaultDiskCorruption {
			continue
		}
		n, _ := fault.Params["node_id"].(string)
		if n == nodeID {
			rate, _ := fault.Params["corrupt_rate"].(float64)
			return fi.rng.Float64() < rate
		}
	}
	return false
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// ActiveFaults returns all currently active faults
func (fi *FaultInjector) ActiveFaults() []*Fault {
	fi.mu.RLock()
	defer fi.mu.RUnlock()

	result := make([]*Fault, 0, len(fi.faults))
	for _, f := range fi.faults {
		if f.Active {
			fCopy := *f
			result = append(result, &fCopy)
		}
	}
	return result
}

// Stats returns fault injection statistics
func (fi *FaultInjector) Stats() *FaultStats {
	return fi.stats
}
