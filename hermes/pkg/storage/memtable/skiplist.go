package memtable

import (
	"math/rand"
	"sync"
	"time"

	"github.com/aadityabinodyadav/hermes/pkg/clock"
)

const (
	maxLevel = 12

	probability = 0.25
)

type Entry struct {
	Key       string
	Value     []byte
	Timestamp clock.HLCTimestamp // HLC timestamp for MVCC
	Deleted   bool               // tombstone marker for deletes
	Sequence  uint64             // WAL sequence number
}

type node struct {
	entry   *Entry
	forward []*node // forward pointers for each level
}

type SkipList struct {
	mu     sync.RWMutex
	head   *node // sentinel head node
	level  int   // current max level with data
	length int   // number of entries
	size   int64 // approximate memory usage in bytes
	rng    *rand.Rand
}

func NewSkipList() *SkipList {
	head := &node{
		forward: make([]*node, maxLevel),
	}
	return &SkipList{
		head:  head,
		level: 1,
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (sl *SkipList) randomLevel() int {
	level := 1
	for level < maxLevel && sl.rng.Float64() < probability {
		level++
	}
	return level
}

func (sl *SkipList) Put(key string, value []byte, ts clock.HLCTimestamp, seq uint64) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	update := make([]*node, maxLevel)
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil &&
			current.forward[i].entry.Key < key {
			current = current.forward[i]
		}
		update[i] = current
	}

	next := current.forward[0]
	if next != nil && next.entry.Key == key {
		next.entry = &Entry{
			Key:       key,
			Value:     value,
			Timestamp: ts,
			Sequence:  seq,
		}
		return
	}

	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	newNode := &node{
		entry: &Entry{
			Key:       key,
			Value:     value,
			Timestamp: ts,
			Sequence:  seq,
		},
		forward: make([]*node, newLevel),
	}

	for i := 0; i < newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	sl.length++
	sl.size += int64(len(key) + len(value) + 64)
}

func (sl *SkipList) Delete(key string, ts clock.HLCTimestamp, seq uint64) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil &&
			current.forward[i].entry.Key < key {
			current = current.forward[i]
		}
	}

	next := current.forward[0]
	if next != nil && next.entry.Key == key {
		next.entry = &Entry{
			Key:       key,
			Value:     nil,
			Timestamp: ts,
			Sequence:  seq,
			Deleted:   true,
		}
		return
	}

	sl.mu.Unlock()
	sl.Put(key, nil, ts, seq)
	sl.mu.Lock()
	current = sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil &&
			current.forward[i].entry.Key < key {
			current = current.forward[i]
		}
	}
	if current.forward[0] != nil && current.forward[0].entry.Key == key {
		current.forward[0].entry.Deleted = true
		current.forward[0].entry.Value = nil
	}
}

func (sl *SkipList) Get(key string) (*Entry, bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil &&
			current.forward[i].entry.Key < key {
			current = current.forward[i]
		}
	}

	next := current.forward[0]
	if next != nil && next.entry.Key == key {
		return next.entry, true
	}

	return nil, false
}

func (sl *SkipList) GetAtTimestamp(key string, readTS clock.HLCTimestamp) (*Entry, bool) {

	entry, found := sl.Get(key)
	if !found {
		return nil, false
	}
	if entry.Timestamp.After(readTS) {
		return nil, false
	}
	return entry, true
}

func (sl *SkipList) Scan(startKey, endKey string) []*Entry {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	var entries []*Entry
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil &&
			current.forward[i].entry.Key < startKey {
			current = current.forward[i]
		}
	}

	current = current.forward[0]
	for current != nil {
		if endKey != "" && current.entry.Key >= endKey {
			break
		}
		entries = append(entries, current.entry)
		current = current.forward[0]
	}

	return entries
}

func (sl *SkipList) All() []*Entry {
	return sl.Scan("", "")
}

func (sl *SkipList) Size() int64 {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.size
}

func (sl *SkipList) Len() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.length
}
