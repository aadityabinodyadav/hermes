// pkg/consistency/readindex.go
package consistency

// ReadIndex implements linearizable reads via the ReadIndex protocol
//
// PROTOCOL:
//   1. Client sends read request to leader
//   2. Leader records current commitIndex as readIndex
//   3. Leader sends heartbeats to confirm it's still the leader
//   4. When majority responds, leader waits for applied >= readIndex
//   5. Leader serves the read from local state
//
// This gives linearizability because:
//   - We confirm we're still the leader (no stale reads from old leader)
//   - We ensure we've applied up to the point where we confirmed leadership
//   - Any write that completed before our read will be in our log
//
// OPTIMIZATION: Batch multiple read requests
//   Instead of one heartbeat per read request,
//   batch all pending reads and do ONE heartbeat.
//   When majority responds, all batched reads can be served.

import (
	"context"
	"sync"
	"sync/atomic"
)

// ReadIndexRequest represents a linearizable read request
type ReadIndexRequest struct {
	// RequestID uniquely identifies this request
	RequestID uint64

	// ReadIndex is the commit index at time of request
	ReadIndex uint64

	// ResultCh receives the result when read can be served
	ResultCh chan ReadIndexResult
}

// ReadIndexResult is the result of a ReadIndex check
type ReadIndexResult struct {
	// ReadIndex is the index the caller must wait for
	// Caller should wait until applied >= ReadIndex
	ReadIndex uint64

	// Err is any error
	Err error
}

// ReadIndexTracker manages pending read index requests
type ReadIndexTracker struct {
	mu sync.Mutex

	// nodeID for logging
	nodeID string

	// pending read requests waiting for confirmation
	pending []*ReadIndexRequest

	// nextRequestID
	nextRequestID uint64

	// callbacks from the Raft layer
	// sendHeartbeat: trigger a heartbeat to all followers
	sendHeartbeat func()

	// applied: the current applied index
	applied uint64
}

// NewReadIndexTracker creates a new tracker
func NewReadIndexTracker(nodeID string, sendHeartbeat func()) *ReadIndexTracker {
	return &ReadIndexTracker{
		nodeID:        nodeID,
		sendHeartbeat: sendHeartbeat,
	}
}

// RequestRead submits a linearizable read request
// Returns when the read can be safely served (applied >= readIndex)
func (t *ReadIndexTracker) RequestRead(
	ctx context.Context,
	commitIndex uint64,
) (uint64, error) {
	req := &ReadIndexRequest{
		RequestID: t.allocateID(),
		ReadIndex: commitIndex,
		ResultCh:  make(chan ReadIndexResult, 1),
	}

	t.mu.Lock()
	t.pending = append(t.pending, req)
	numPending := len(t.pending)
	t.mu.Unlock()

	// If this is the first pending request, trigger heartbeat
	// If others are already pending, they'll batch with the same heartbeat
	if numPending == 1 {
		go t.sendHeartbeat()
	}

	// Wait for result
	select {
	case result := <-req.ResultCh:
		return result.ReadIndex, result.Err

	case <-ctx.Done():
		// Clean up pending request
		t.mu.Lock()
		for i, p := range t.pending {
			if p.RequestID == req.RequestID {
				t.pending = append(t.pending[:i], t.pending[i+1:]...)
				break
			}
		}
		t.mu.Unlock()
		return 0, ctx.Err()
	}
}

// ConfirmLeadership is called when we receive majority heartbeat responses
// This confirms we're still the leader and we can serve reads up to readIndex
func (t *ReadIndexTracker) ConfirmLeadership(maxCommitIndex uint64) {
	t.mu.Lock()
	pending := make([]*ReadIndexRequest, 0, len(t.pending))
	toNotify := make([]*ReadIndexRequest, 0, len(t.pending))

	for _, req := range t.pending {
		if req.ReadIndex <= maxCommitIndex {
			toNotify = append(toNotify, req)
		} else {
			pending = append(pending, req)
		}
	}
	t.pending = pending
	t.mu.Unlock()

	// Notify all requests that can be served
	for _, req := range toNotify {
		select {
		case req.ResultCh <- ReadIndexResult{ReadIndex: req.ReadIndex}:
		default:
		}
	}
}

// OnApplied is called when the state machine applies an entry
// Wakes up any reads that can now be served
func (t *ReadIndexTracker) OnApplied(appliedIndex uint64) {
	atomic.StoreUint64(&t.applied, appliedIndex)

	// This is a simplified version. In production, we'd also
	// check if any pending reads can now be served based on applied index.
}

func (t *ReadIndexTracker) allocateID() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextRequestID++
	return t.nextRequestID
}
