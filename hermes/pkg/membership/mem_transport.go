// pkg/membership/mem_transport.go
package membership

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// MemSWIMTransport is an in-memory SWIM transport for testing
type MemSWIMTransport struct {
	mu     sync.RWMutex
	nodeID string
	peers  map[string]*MemSWIMTransport
	recvCh chan SWIMMessage

	// Chaos controls
	dropRate   float64
	partitions map[string]bool
	latencyMin time.Duration
	latencyMax time.Duration
	rng        *rand.Rand
}

func NewMemSWIMTransport(nodeID string) *MemSWIMTransport {
	return &MemSWIMTransport{
		nodeID:     nodeID,
		peers:      make(map[string]*MemSWIMTransport),
		recvCh:     make(chan SWIMMessage, 256),
		partitions: make(map[string]bool),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (t *MemSWIMTransport) Connect(other *MemSWIMTransport) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[other.nodeID] = other
}

func (t *MemSWIMTransport) Send(ctx context.Context, nodeID string, msg SWIMMessage) error {
	t.mu.RLock()
	peer, exists := t.peers[nodeID]
	partitioned := t.partitions[nodeID]
	dropRate := t.dropRate
	latMin := t.latencyMin
	latMax := t.latencyMax
	t.mu.RUnlock()

	if !exists {
		return fmt.Errorf("swim: unknown peer %s", nodeID)
	}

	if partitioned {
		return fmt.Errorf("swim: partitioned from %s", nodeID)
	}

	if dropRate > 0 && t.rng.Float64() < dropRate {
		return nil // drop silently
	}

	// Apply latency
	if latMax > latMin {
		delay := latMin + time.Duration(t.rng.Int63n(int64(latMax-latMin)))
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	select {
	case peer.recvCh <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("swim: peer %s recv buffer full", nodeID)
	}
}

func (t *MemSWIMTransport) Recv() <-chan SWIMMessage {
	return t.recvCh
}

func (t *MemSWIMTransport) Partition(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.partitions[nodeID] = true
}

func (t *MemSWIMTransport) Heal(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.partitions, nodeID)
}

func (t *MemSWIMTransport) SetDropRate(rate float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dropRate = rate
}

func (t *MemSWIMTransport) SetLatency(min, max time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.latencyMin = min
	t.latencyMax = max
}
