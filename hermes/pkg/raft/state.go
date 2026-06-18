package raft

import (
	"fmt"
	"sync/atomic"
)

type NodeState uint32

func (s NodeState) Load() NodeState {
	return s
}

const (
	Follower  NodeState = 0
	Candidate NodeState = 1
	Leader    NodeState = 2
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

// atomicState allows lock-free state reads
// Critical: reading state in hot paths (request routing) must be cheap
type atomicState struct {
	v uint32
}

func (s *atomicState) Load() NodeState {
	return NodeState(atomic.LoadUint32(&s.v))
}

func (s *atomicState) Store(state NodeState) {
	atomic.StoreUint32(&s.v, uint32(state))
}

// ─────────────────────────────────────────────────────────────────────────────

// LogEntry is one entry in the Raft replicated log
// This is THE unit of replication
type LogEntry struct {
	Index uint64    // position in log (1-based, 0 = sentinel)
	Term  uint64    // term when entry was created by leader
	Type  EntryType // what kind of command
	Data  []byte    // serialized command (storage.Command proto)
}

type EntryType uint8

const (
	EntryNormal  EntryType = 0 // regular KV operation
	EntryConfig  EntryType = 1 // cluster membership change
	EntryBarrier EntryType = 2 // read linearizability barrier
	EntryNoop    EntryType = 3 // leader no-op on election
)

func (e *LogEntry) String() string {
	return fmt.Sprintf("LogEntry{index=%d, term=%d, type=%d, len=%d}",
		e.Index, e.Term, e.Type, len(e.Data))
}

type Progress struct {
	NextIndex uint64

	MatchIndex uint64

	State ProgressState

	Inflight int

	RecentActive bool
}

type ProgressState uint8

const (
	ProgressProbe     ProgressState = 0
	ProgressReplicate ProgressState = 1
	ProgressSnapshot  ProgressState = 2
)

func (p *Progress) MaybeUpdate(newIndex uint64) bool {
	if newIndex <= p.MatchIndex {
		return false
	}
	p.MatchIndex = newIndex
	if p.NextIndex <= newIndex {
		p.NextIndex = newIndex + 1
	}
	return true
}

func (p *Progress) MaybeDecrTo(rejected, conflictIndex uint64) bool {
	if p.State == ProgressReplicate {
		p.State = ProgressProbe
		p.NextIndex = p.MatchIndex + 1
		return true
	}

	if rejected != p.NextIndex-1 {
		return false
	}

	newNext := conflictIndex
	if newNext < 1 {
		newNext = 1
	}
	if newNext < p.NextIndex {
		p.NextIndex = newNext
		return true
	}
	return false
}

func (p *Progress) BecomeReplicate() {
	p.State = ProgressReplicate
	p.NextIndex = p.MatchIndex + 1
	p.Inflight = 0
}

func (p *Progress) BecomeProbe() {
	p.State = ProgressProbe
	if p.State == ProgressSnapshot {
		p.NextIndex = p.MatchIndex + 1
	}
}
