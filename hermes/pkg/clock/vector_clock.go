package clock

import (
	"fmt"
	"strings"
	"sync"
)

type VectorClock struct {
	mu     sync.RWMutex
	nodeID string

	vector map[string]int64
}

func NewVectorClock(nodeID string, initialPeers []string) *VectorClock {
	vc := &VectorClock{
		nodeID: nodeID,
		vector: make(map[string]int64),
	}

	vc.vector[nodeID] = 0
	for _, peer := range initialPeers {
		vc.vector[peer] = 0
	}

	return vc
}

func (vc *VectorClock) Tick() map[string]int64 {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	vc.vector[vc.nodeID]++
	return vc.copyVector()
}

func (vc *VectorClock) Send() map[string]int64 {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	vc.vector[vc.nodeID]++
	return vc.copyVector()
}

func (vc *VectorClock) Receive(received map[string]int64) map[string]int64 {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	for nodeID, receivedTime := range received {
		if localTime, exists := vc.vector[nodeID]; !exists || receivedTime > localTime {
			vc.vector[nodeID] = receivedTime
		}
	}

	vc.vector[vc.nodeID]++

	return vc.copyVector()
}

func (vc *VectorClock) Now() map[string]int64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return vc.copyVector()
}

func (vc *VectorClock) copyVector() map[string]int64 {
	copy := make(map[string]int64, len(vc.vector))
	for k, v := range vc.vector {
		copy[k] = v
	}
	return copy
}

type Relation int

const (
	Before Relation = iota

	After

	Concurrent

	Equal
)

func (r Relation) String() string {
	switch r {
	case Before:
		return "happened-before (→)"
	case After:
		return "happened-after (←)"
	case Concurrent:
		return "concurrent (∥) ← CONFLICT!"
	case Equal:
		return "equal (=)"
	}
	return "unknown"
}
func Compare(a, b map[string]int64) Relation {
	allNodes := make(map[string]bool)
	for k := range a {
		allNodes[k] = true
	}
	for k := range b {
		allNodes[k] = true
	}

	aLessInSome := false
	bLessInSome := false

	for node := range allNodes {
		aVal := a[node]
		bVal := b[node]

		if aVal < bVal {
			aLessInSome = true
		} else if bVal < aVal {
			bLessInSome = true
		}

		if aLessInSome && bLessInSome {
			return Concurrent
		}
	}

	if !aLessInSome && !bLessInSome {
		return Equal
	}
	if aLessInSome {
		return Before
	}
	return After
}

func IsConcurrent(a, b map[string]int64) bool {
	return Compare(a, b) == Concurrent
}

func (vc *VectorClock) String() string {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	parts := make([]string, 0, len(vc.vector))
	for k, v := range vc.vector {
		parts = append(parts, fmt.Sprintf("%s:%d", k, v))
	}
	return fmt.Sprintf("[%s]", strings.Join(parts, ", "))
}

type VectorMessage struct {
	From        string
	To          string
	Content     string
	VectorClock map[string]int64
}
