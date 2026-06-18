package raft

import (
	"fmt"
	"sync"
)

/*
RaftLog manages the Raft log

The Raft log is the BACKBONE of consensus.
Every committed entry in this log WILL be applied to EVERY state machine.

Log structure:

	Index:  0    1    2    3    4    5
	        │    │    │    │    │    │
	Entry: [nil][{term:1}][{term:1}][{term:2}][{term:2}][{term:2}]
	            ├────────────────────┤└──────────────────────────┘
	            COMMITTED (applied)       UNCOMMITTED (not yet majority)
	            commitIndex=3             lastIndex=5

	Snapshot: entries 0..N are replaced by a snapshot
	After snapshot: log starts at snapshot.LastIndex+1

CRITICAL INVARIANTS:
 1. Entries are NEVER removed from committed region
 2. log[i].Index == i (index matches position)
 3. log[i].Term <= log[i+1].Term (terms never decrease)
 4. If two logs agree at index I and term T,
    they agree on all entries up to I
*/
type RaftLog struct {
	mu sync.RWMutex

	entries []LogEntry

	snapshotIndex uint64
	snapshotTerm  uint64

	committed uint64

	applied uint64
}

func newRaftLog() *RaftLog {
	return &RaftLog{
		entries: []LogEntry{{Index: 0, Term: 0}},
	}
}

func (l *RaftLog) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndex()
}

func (l *RaftLog) lastIndex() uint64 {
	return l.snapshotIndex + uint64(len(l.entries)) - 1
}

func (l *RaftLog) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.termOf(l.lastIndex())
}

func (l *RaftLog) Term(index uint64) uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.termOf(index)
}

func (l *RaftLog) termOf(index uint64) uint64 {
	if index < l.snapshotIndex {
		return 0
	}
	if index == l.snapshotIndex {
		return l.snapshotTerm
	}
	offset := index - l.snapshotIndex
	if offset >= uint64(len(l.entries)) {
		return 0
	}
	return l.entries[offset].Term
}

func (l *RaftLog) Append(entries ...LogEntry) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	for i, entry := range entries {
		if entry.Index <= l.lastIndex() {
			existingTerm := l.termOf(entry.Index)
			if existingTerm == entry.Term {
				continue
			}
			offset := entry.Index - l.snapshotIndex
			l.entries = l.entries[:offset]
		}

		l.entries = append(l.entries, entries[i:]...)
		break
	}

	return l.lastIndex()
}

func (l *RaftLog) AppendOne(entry LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	expected := l.lastIndex() + 1
	if entry.Index != expected {
		panic(fmt.Sprintf("raft: log append index mismatch: got %d want %d",
			entry.Index, expected))
	}

	l.entries = append(l.entries, entry)
}

func (l *RaftLog) CommitTo(newCommitted uint64) []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	if newCommitted <= l.committed {
		return nil
	}

	if newCommitted > l.lastIndex() {
		newCommitted = l.lastIndex()
	}

	from := l.committed + 1
	to := newCommitted + 1

	l.committed = newCommitted

	return l.slice(from, to)
}

func (l *RaftLog) AppliedTo(applied uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if applied > l.committed {
		panic(fmt.Sprintf("raft: applied(%d) > committed(%d)", applied, l.committed))
	}
	l.applied = applied
}

func (l *RaftLog) Entries(lo, hi uint64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.slice(lo, hi)
}

func (l *RaftLog) slice(lo, hi uint64) []LogEntry {
	if lo >= hi {
		return nil
	}

	if lo < l.snapshotIndex+1 {
		lo = l.snapshotIndex + 1
	}
	if hi > l.lastIndex()+1 {
		hi = l.lastIndex() + 1
	}

	loOffset := lo - l.snapshotIndex
	hiOffset := hi - l.snapshotIndex

	if loOffset >= uint64(len(l.entries)) {
		return nil
	}

	result := make([]LogEntry, 0, hi-lo)
	result = append(result, l.entries[loOffset:hiOffset]...)
	return result
}

func (l *RaftLog) IsUpToDate(lastIndex, lastTerm uint64) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	myLastTerm := l.termOf(l.lastIndex())
	myLastIndex := l.lastIndex()

	if lastTerm != myLastTerm {
		return lastTerm > myLastTerm
	}
	return lastIndex >= myLastIndex
}

func (l *RaftLog) MaybeCommit(matchIndexes []uint64, term, currentTerm uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := l.lastIndex()
	for n > l.committed {
		count := 1
		for _, mi := range matchIndexes {
			if mi >= n {
				count++
			}
		}

		majority := (len(matchIndexes)+1)/2 + 1
		if count >= majority && l.termOf(n) == currentTerm {
			l.committed = n
			return true
		}
		n--
	}
	return false
}

func (l *RaftLog) Compact(snapshotIndex, snapshotTerm uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if snapshotIndex <= l.snapshotIndex {
		return
	}

	offset := snapshotIndex - l.snapshotIndex
	if offset < uint64(len(l.entries)) {
		remaining := make([]LogEntry, len(l.entries)-int(offset))
		copy(remaining, l.entries[offset:])
		remaining[0] = LogEntry{Index: snapshotIndex, Term: snapshotTerm}
		l.entries = remaining
	} else {
		l.entries = []LogEntry{{Index: snapshotIndex, Term: snapshotTerm}}
	}

	l.snapshotIndex = snapshotIndex
	l.snapshotTerm = snapshotTerm

	if l.committed < snapshotIndex {
		l.committed = snapshotIndex
	}
	if l.applied < snapshotIndex {
		l.applied = snapshotIndex
	}
}

func (l *RaftLog) Stats() LogStats {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return LogStats{
		FirstIndex:    l.snapshotIndex + 1,
		LastIndex:     l.lastIndex(),
		CommitIndex:   l.committed,
		AppliedIndex:  l.applied,
		SnapshotIndex: l.snapshotIndex,
		EntryCount:    uint64(len(l.entries) - 1), // -1 for sentinel
	}
}

type LogStats struct {
	FirstIndex    uint64
	LastIndex     uint64
	CommitIndex   uint64
	AppliedIndex  uint64
	SnapshotIndex uint64
	EntryCount    uint64
}
