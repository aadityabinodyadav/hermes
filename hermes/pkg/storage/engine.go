package storage

/*
 The LSM-Tree Storage Engine
 This is the COMPLETE storage engine for Hermes

 It orchestrates:
   WAL → MemTable → SSTable flush → Compaction

 Public API (what the rest of Hermes sees):
   Put(key, value) error
   Get(key) (value, found, error)
   Delete(key) error
   Scan(start, end) (entries, error)
   GetAtTimestamp(key, ts) (value, found, error) ← MVCC
*/

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aadityabinodyadav/hermes/pkg/clock"
	"github.com/aadityabinodyadav/hermes/pkg/storage/memtable"
	"github.com/aadityabinodyadav/hermes/pkg/storage/sstable"
	"github.com/aadityabinodyadav/hermes/pkg/storage/wal"
)

type Config struct {
	DataDir string

	MemTableSize int64

	MaxLevel0SSTables int

	CompactionConcurrency int
}

func DefaultConfig(dataDir string) Config {
	return Config{
		DataDir:               dataDir,
		MemTableSize:          64 * 1024 * 1024,
		MaxLevel0SSTables:     4,
		CompactionConcurrency: 2,
	}
}

type Engine struct {
	config Config
	hlc    *clock.HLC
	wal    *wal.WAL

	mu        sync.RWMutex
	memTable  *memtable.SkipList
	immutable []*memtable.SkipList

	levels [][]*sstable.SSTableMeta

	flushCh   chan *memtable.SkipList
	compactCh chan int

	sequence uint64

	writeCount int64
	readCount  int64
	flushCount int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed bool
}

func Open(config Config, hlc *clock.HLC) (*Engine, error) {
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("storage: failed to create data dir: %w", err)
	}

	walDir := filepath.Join(config.DataDir, "wal")
	sstDir := filepath.Join(config.DataDir, "sst")
	for _, dir := range []string{walDir, sstDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}

	w, err := wal.Open(walDir, hlc)
	if err != nil {
		return nil, fmt.Errorf("storage: failed to open WAL: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	engine := &Engine{
		config:    config,
		hlc:       hlc,
		wal:       w,
		memTable:  memtable.NewSkipList(),
		levels:    make([][]*sstable.SSTableMeta, 7), // 7 levels (L0-L6)
		flushCh:   make(chan *memtable.SkipList, 4),
		compactCh: make(chan int, 10),
		ctx:       ctx,
		cancel:    cancel,
	}

	if err := engine.loadSSTables(sstDir); err != nil {
		w.Close()
		cancel()
		return nil, fmt.Errorf("storage: failed to load SSTables: %w", err)
	}

	if err := engine.replayWAL(); err != nil {
		w.Close()
		cancel()
		return nil, fmt.Errorf("storage: WAL replay failed: %w", err)
	}

	engine.wg.Add(1)
	go engine.flushWorker()

	engine.wg.Add(1)
	go engine.compactionWorker()

	fmt.Printf("Storage engine opened: dir=%s\n", config.DataDir)
	return engine, nil
}

func (e *Engine) Put(key string, value []byte) error {
	return e.putWithTimestamp(key, value, false)
}

func (e *Engine) Delete(key string) error {
	return e.putWithTimestamp(key, nil, true)
}

func (e *Engine) putWithTimestamp(key string, value []byte, deleted bool) error {
	if e.closed {
		return fmt.Errorf("storage: engine is closed")
	}

	cmd := encodeCommand(key, value, deleted)

	entry, err := e.wal.Write(cmd)
	if err != nil {
		return fmt.Errorf("storage: WAL write failed: %w", err)
	}

	e.mu.Lock()

	if deleted {
		e.memTable.Delete(key, entry.Timestamp, entry.Sequence)
	} else {
		e.memTable.Put(key, value, entry.Timestamp, entry.Sequence)
	}

	shouldFlush := e.memTable.Size() >= e.config.MemTableSize
	var toFlush *memtable.SkipList
	if shouldFlush {
		toFlush = e.memTable
		e.immutable = append(e.immutable, toFlush)
		e.memTable = memtable.NewSkipList()
	}

	e.mu.Unlock()

	if shouldFlush {
		select {
		case e.flushCh <- toFlush:
		default:
			fmt.Println("Storage: flush channel full, write stall!")
		}
	}

	atomic.AddInt64(&e.writeCount, 1)
	return nil
}
func (e *Engine) Get(key string) ([]byte, bool, error) {
	return e.getAtTimestamp(key, 0) // 0 = latest
}

func (e *Engine) GetAtTimestamp(key string, readTS clock.HLCTimestamp) ([]byte, bool, error) {
	return e.getAtTimestamp(key, readTS)
}

func (e *Engine) getAtTimestamp(key string, readTS clock.HLCTimestamp) ([]byte, bool, error) {
	if e.closed {
		return nil, false, fmt.Errorf("storage: engine is closed")
	}

	atomic.AddInt64(&e.readCount, 1)

	e.mu.RLock()
	defer e.mu.RUnlock()

	if readTS == 0 {
		if entry, found := e.memTable.Get(key); found {
			if entry.Deleted {
				return nil, false, nil
			}
			return entry.Value, true, nil
		}
	} else {
		if entry, found := e.memTable.GetAtTimestamp(key, readTS); found {
			if entry.Deleted {
				return nil, false, nil
			}
			return entry.Value, true, nil
		}
	}

	for i := len(e.immutable) - 1; i >= 0; i-- {
		var entry *memtable.Entry
		var found bool

		if readTS == 0 {
			entry, found = e.immutable[i].Get(key)
		} else {
			entry, found = e.immutable[i].GetAtTimestamp(key, readTS)
		}

		if found {
			if entry.Deleted {
				return nil, false, nil
			}
			return entry.Value, true, nil
		}
	}

	for level, tables := range e.levels {
		if level == 0 {
			for i := len(tables) - 1; i >= 0; i-- {
				entry, err := e.readFromSSTable(tables[i], key)
				if err != nil {
					return nil, false, err
				}
				if entry != nil {
					if entry.Deleted {
						return nil, false, nil
					}
					return entry.Value, true, nil
				}
			}
		} else {
			entry, err := e.readFromLevel(tables, key)
			if err != nil {
				return nil, false, err
			}
			if entry != nil {
				if entry.Deleted {
					return nil, false, nil
				}
				return entry.Value, true, nil
			}
		}
	}

	return nil, false, nil
}

func (e *Engine) readFromSSTable(meta *sstable.SSTableMeta, key string) (*memtable.Entry, error) {
	if !meta.MightContain(key) {
		return nil, nil
	}

	reader, err := sstable.OpenReader(meta.Path)
	if err != nil {
		return nil, fmt.Errorf("storage: failed to open SSTable %s: %w",
			meta.Path, err)
	}
	defer reader.Close()

	entry, found, err := reader.Get(key)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}

	return entry, nil
}

func (e *Engine) readFromLevel(tables []*sstable.SSTableMeta, key string) (*memtable.Entry, error) {
	idx := sort.Search(len(tables), func(i int) bool {
		return tables[i].MaxKey >= key
	})

	if idx >= len(tables) {
		return nil, nil
	}

	table := tables[idx]
	if key < table.MinKey || key > table.MaxKey {
		return nil, nil
	}

	return e.readFromSSTable(table, key)
}

func (e *Engine) Scan(startKey, endKey string) ([]*memtable.Entry, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	results := make(map[string]*memtable.Entry)

	for i := len(e.levels) - 1; i >= 0; i-- {
		for _, table := range e.levels[i] {
			if table.MaxKey < startKey || table.MinKey > endKey {
				continue
			}

			reader, err := sstable.OpenReader(table.Path)
			if err != nil {
				continue
			}

			iter := reader.Iterator()
			for {
				entry, err := iter.Next()
				if err != nil || entry == nil {
					break
				}
				if entry.Key >= startKey && (endKey == "" || entry.Key < endKey) {
					results[entry.Key] = entry
				}
			}
			reader.Close()
		}
	}

	for _, entry := range e.memTable.Scan(startKey, endKey) {
		results[entry.Key] = entry
	}

	var entries []*memtable.Entry
	for _, entry := range results {
		if !entry.Deleted {
			entries = append(entries, entry)
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	return entries, nil
}

func (e *Engine) flushWorker() {
	defer e.wg.Done()

	for {
		select {
		case <-e.ctx.Done():
			return
		case table := <-e.flushCh:
			if err := e.flushMemTable(table); err != nil {
				fmt.Printf("Storage: flush error: %v\n", err)
			}
		}
	}
}

func (e *Engine) flushMemTable(table *memtable.SkipList) error {
	entries := table.All()
	if len(entries) == 0 {
		return nil
	}

	sstDir := filepath.Join(e.config.DataDir, "sst", "L0")
	if err := os.MkdirAll(sstDir, 0755); err != nil {
		return err
	}

	path := filepath.Join(sstDir,
		fmt.Sprintf("L0_%d.sst", time.Now().UnixNano()))

	writer, err := sstable.NewWriter(path, len(entries))
	if err != nil {
		return fmt.Errorf("flush: failed to create SSTable writer: %w", err)
	}

	for _, entry := range entries {
		if err := writer.Add(entry); err != nil {
			return fmt.Errorf("flush: failed to add entry: %w", err)
		}
	}

	meta, err := writer.Finish()
	if err != nil {
		return fmt.Errorf("flush: failed to finish SSTable: %w", err)
	}

	meta.Level = 0

	e.mu.Lock()
	e.levels[0] = append(e.levels[0], meta)

	for i, t := range e.immutable {
		if t == table {
			e.immutable = append(e.immutable[:i], e.immutable[i+1:]...)
			break
		}
	}
	e.mu.Unlock()

	atomic.AddInt64(&e.flushCount, 1)
	fmt.Printf("Storage: flushed MemTable → %s (%d entries, %.2fMB)\n",
		filepath.Base(path), len(entries),
		float64(meta.FileSize)/1024/1024)

	if len(e.levels[0]) >= e.config.MaxLevel0SSTables {
		select {
		case e.compactCh <- 0:
		default:
		}
	}

	return nil
}

func (e *Engine) compactionWorker() {
	defer e.wg.Done()

	for {
		select {
		case <-e.ctx.Done():
			return
		case level := <-e.compactCh:
			if err := e.compact(level); err != nil {
				fmt.Printf("Storage: compaction error at level %d: %v\n", level, err)
			}
		}
	}
}

func (e *Engine) compact(level int) error {
	if level >= len(e.levels)-1 {
		return nil
	}

	e.mu.RLock()
	sourceTables := make([]*sstable.SSTableMeta, len(e.levels[level]))
	copy(sourceTables, e.levels[level])
	e.mu.RUnlock()

	if len(sourceTables) == 0 {
		return nil
	}

	fmt.Printf("Storage: compacting %d SSTables from L%d → L%d\n",
		len(sourceTables), level, level+1)

	var iterators []*sstable.SSTableIterator
	var readers []*sstable.Reader
	for _, meta := range sourceTables {
		reader, err := sstable.OpenReader(meta.Path)
		if err != nil {
			continue
		}
		readers = append(readers, reader)
		iterators = append(iterators, reader.Iterator())
	}
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

	nextLevel := level + 1
	sstDir := filepath.Join(e.config.DataDir, "sst", fmt.Sprintf("L%d", nextLevel))
	os.MkdirAll(sstDir, 0755)

	path := filepath.Join(sstDir,
		fmt.Sprintf("L%d_%d.sst", nextLevel, time.Now().UnixNano()))

	writer, err := sstable.NewWriter(path, 0)
	if err != nil {
		return err
	}

	merged, err := kWayMerge(iterators)
	if err != nil {
		return err
	}

	for _, entry := range merged {
		if entry.Deleted && nextLevel == len(e.levels)-1 {
			continue
		}
		writer.Add(entry)
	}

	newMeta, err := writer.Finish()
	if err != nil {
		return err
	}
	newMeta.Level = nextLevel

	e.mu.Lock()
	e.levels[level] = nil
	e.levels[nextLevel] = append(e.levels[nextLevel], newMeta)
	e.mu.Unlock()

	for _, meta := range sourceTables {
		os.Remove(meta.Path)
	}

	fmt.Printf("Storage: compaction complete → %s (%.2fMB)\n",
		filepath.Base(path), float64(newMeta.FileSize)/1024/1024)

	return nil
}

func kWayMerge(iterators []*sstable.SSTableIterator) ([]*memtable.Entry, error) {

	seen := make(map[string]*memtable.Entry)

	for _, iter := range iterators {
		for {
			entry, err := iter.Next()
			if err != nil {
				return nil, err
			}
			if entry == nil {
				break
			}

			if existing, ok := seen[entry.Key]; !ok ||
				entry.Timestamp.After(existing.Timestamp) {
				seen[entry.Key] = entry
			}
		}
	}

	entries := make([]*memtable.Entry, 0, len(seen))
	for _, e := range seen {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	return entries, nil
}

func (e *Engine) replayWAL() error {
	entries, err := e.wal.ReadFrom(e.ctx, 1)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		key, value, deleted := decodeCommand(entry.Data)
		if deleted {
			e.memTable.Delete(key, entry.Timestamp, entry.Sequence)
		} else {
			e.memTable.Put(key, value, entry.Timestamp, entry.Sequence)
		}
		if entry.Sequence > e.sequence {
			e.sequence = entry.Sequence
		}
	}

	fmt.Printf("Storage: replayed %d WAL entries\n", len(entries))
	return nil
}

func (e *Engine) loadSSTables(sstDir string) error {
	for level := 0; level < len(e.levels); level++ {
		levelDir := filepath.Join(sstDir, fmt.Sprintf("L%d", level))
		if _, err := os.Stat(levelDir); os.IsNotExist(err) {
			continue
		}

		files, err := filepath.Glob(filepath.Join(levelDir, "*.sst"))
		if err != nil {
			return err
		}

		for _, path := range files {
			reader, err := sstable.OpenReader(path)
			if err != nil {
				fmt.Printf("Storage: failed to load SSTable %s: %v\n", path, err)
				continue
			}
			meta := reader.Meta()
			meta.Level = level
			e.levels[level] = append(e.levels[level], meta)
			reader.Close()
		}

		fmt.Printf("Storage: loaded %d SSTables at L%d\n",
			len(e.levels[level]), level)
	}
	return nil
}

func (e *Engine) Stats() EngineStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	sstCount := 0
	for _, level := range e.levels {
		sstCount += len(level)
	}

	return EngineStats{
		WriteCount:   atomic.LoadInt64(&e.writeCount),
		ReadCount:    atomic.LoadInt64(&e.readCount),
		FlushCount:   atomic.LoadInt64(&e.flushCount),
		MemTableSize: e.memTable.Size(),
		MemTableLen:  int64(e.memTable.Len()),
		SSTableCount: sstCount,
		WALStats:     e.wal.Stats(),
	}
}

func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()

	e.cancel()
	e.wg.Wait()

	if e.memTable.Len() > 0 {
		if err := e.flushMemTable(e.memTable); err != nil {
			fmt.Printf("Storage: final flush error: %v\n", err)
		}
	}

	return e.wal.Close()
}

type EngineStats struct {
	WriteCount   int64
	ReadCount    int64
	FlushCount   int64
	MemTableSize int64
	MemTableLen  int64
	SSTableCount int
	WALStats     wal.WALStats
}

func encodeCommand(key string, value []byte, deleted bool) []byte {
	keyBytes := []byte(key)
	size := 1 + 4 + len(keyBytes) + 4 + len(value)
	buf := make([]byte, size)

	offset := 0
	if deleted {
		buf[0] = 1
	}
	offset++

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(keyBytes)))
	offset += 4
	copy(buf[offset:], keyBytes)
	offset += len(keyBytes)

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(value)))
	offset += 4
	copy(buf[offset:], value)

	return buf
}

func decodeCommand(data []byte) (key string, value []byte, deleted bool) {
	if len(data) < 5 {
		return "", nil, false
	}
	deleted = data[0] == 1
	offset := 1

	keyLen := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	key = string(data[offset : offset+keyLen])
	offset += keyLen

	valLen := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	if valLen > 0 && offset+valLen <= len(data) {
		value = data[offset : offset+valLen]
	}

	return
}

func init() {
	_ = fmt.Sprintf
}
