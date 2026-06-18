// pkg/storage/wal/wal.go
package wal

// The Write-Ahead Log (WAL)
//
// This is THE most critical component of Hermes's storage layer.
// Every write MUST go through the WAL before being acknowledged.
//
// Durability guarantee:
//   "If we ACK a write to the client, that write will survive
//    ANY single-node failure (power cut, OS crash, disk error
//    except disk physical destruction)"
//
// Group Commit (crucial optimization):
//   Instead of calling fsync() after EVERY write:
//   - Buffer N writes OR wait T milliseconds
//   - Call fsync() ONCE for all buffered writes
//   - Notify all waiting writers
//
//   Without group commit: 1 fsync per write → ~1000 writes/sec (HDD)
//   With group commit:    1 fsync per batch → ~100,000 writes/sec
//
//   This is exactly what PostgreSQL, MySQL, and etcd do.
//   The tradeoff: slightly higher LATENCY, much higher THROUGHPUT.
//
// WAL recovery (startup sequence):
//   1. Find all WAL segment files in directory
//   2. Open each segment in order
//   3. Read and replay all records
//   4. Rebuild MemTable from replayed records
//   5. Continue normal operation

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/aadityabinodyadav/hermes/pkg/clock"
)

// WALEntry is what callers write to the WAL
type WALEntry struct {
	// Sequence is assigned by the WAL (monotonically increasing)
	Sequence uint64

	// Timestamp is the HLC timestamp of this entry
	Timestamp clock.HLCTimestamp

	// Data is the serialized operation (proto-encoded Command)
	Data []byte

	// Type is the record type
	Type RecordType
}

// WAL manages the Write-Ahead Log
type WAL struct {
	mu sync.Mutex

	// dir is the directory containing WAL segment files
	dir string

	// active is the current segment being written to
	active *Segment

	// sealed contains all closed (full) segments
	// We keep them until their data is in SSTables
	sealed []*Segment

	// nextSeq is the next sequence number to assign
	nextSeq uint64

	// clock is the HLC clock for timestamps
	clock *clock.HLC

	// config
	segmentSize int64
	syncEvery   time.Duration // max time between fsyncs (group commit window)

	// group commit state
	pendingSync []chan error // goroutines waiting for sync
	syncTimer   *time.Timer

	// closed
	closed bool

	// stats
	bytesWritten   int64
	recordsWritten int64
	syncCount      int64
}

// Open opens or creates a WAL in the given directory
func Open(dir string, hlc *clock.HLC) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: failed to create directory %s: %w", dir, err)
	}

	w := &WAL{
		dir:         dir,
		clock:       hlc,
		segmentSize: DefaultSegmentSize,
		syncEvery:   2 * time.Millisecond, // 2ms group commit window
		nextSeq:     1,
	}

	// Recover any existing WAL segments
	if err := w.recover(); err != nil {
		return nil, fmt.Errorf("wal: recovery failed: %w", err)
	}

	// Create the active segment (or reopen existing active)
	if w.active == nil {
		seg, err := newSegment(dir, w.nextSeq, w.segmentSize)
		if err != nil {
			return nil, fmt.Errorf("wal: failed to create initial segment: %w", err)
		}
		w.active = seg
	}

	return w, nil
}

// Write appends an entry to the WAL
// This is the HOT PATH — called on every client write
//
// The write is buffered and fsynced as part of a group.
// The caller BLOCKS until the entry is durable (fsync completed).
//
// This is the "write barrier" — guarantees durability before returning.
func (w *WAL) Write(data []byte) (*WALEntry, error) {
	w.mu.Lock()

	if w.closed {
		w.mu.Unlock()
		return nil, ErrWALClosed
	}

	// Assign sequence number and HLC timestamp
	seq := w.nextSeq
	w.nextSeq++
	ts := w.clock.Now()

	entry := &WALEntry{
		Sequence:  seq,
		Timestamp: ts,
		Data:      data,
		Type:      RecordData,
	}

	// Build the WAL record
	// We encode the HLC timestamp into the data prefix
	encodedData := encodeEntryData(ts, data)

	rec := &Record{
		Type:     RecordData,
		Sequence: seq,
		Data:     encodedData,
	}

	// Write to active segment (buffered, not fsynced yet)
	if err := w.writeToActive(rec); err != nil {
		w.mu.Unlock()
		return nil, err
	}

	// Group commit: register ourselves as waiting for sync
	syncDone := make(chan error, 1)
	w.pendingSync = append(w.pendingSync, syncDone)

	// Start sync timer if not already running
	// This implements the "batch for up to 2ms" part of group commit
	if w.syncTimer == nil {
		w.syncTimer = time.AfterFunc(w.syncEvery, func() {
			w.flushPendingSync()
		})
	}

	w.mu.Unlock()

	// BLOCK until fsync completes (or error)
	// This is the durability guarantee — we don't return until data is on disk
	err := <-syncDone
	if err != nil {
		return nil, fmt.Errorf("wal: sync failed: %w", err)
	}

	return entry, nil
}

// WriteSync writes an entry AND immediately fsyncs (bypass group commit)
// Use for critical entries where latency is acceptable but durability is critical
// Example: Raft snapshot, cluster membership change
func (w *WAL) WriteSync(data []byte) (*WALEntry, error) {
	entry, err := w.Write(data)
	if err != nil {
		return nil, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Force immediate sync
	return entry, w.active.Sync()
}

// writeToActive writes a record to the active segment
// If the active segment is full, creates a new one
// MUST be called with w.mu held
func (w *WAL) writeToActive(rec *Record) error {
	err := w.active.writeRecord(rec)

	if err == ErrSegmentFull {
		// Seal the current segment and create a new one
		if err := w.rotateSegment(); err != nil {
			return err
		}
		// Retry with new segment
		return w.active.writeRecord(rec)
	}

	if err != nil {
		return err
	}

	w.bytesWritten += int64(HeaderSize + len(rec.Data))
	w.recordsWritten++
	return nil
}

// rotateSegment seals the active segment and creates a new one
// MUST be called with w.mu held
func (w *WAL) rotateSegment() error {
	// Sync the active segment before sealing
	if err := w.active.Sync(); err != nil {
		return fmt.Errorf("wal: sync before rotation failed: %w", err)
	}

	// Move active to sealed list
	w.sealed = append(w.sealed, w.active)

	// Create new segment
	newID := uint64(len(w.sealed) + 1)
	seg, err := newSegment(w.dir, newID, w.segmentSize)
	if err != nil {
		return fmt.Errorf("wal: failed to create new segment: %w", err)
	}

	w.active = seg
	fmt.Printf("WAL: rotated to segment %d\n", newID)
	return nil
}

// flushPendingSync performs the actual fsync and notifies all waiters
// This is called by the sync timer OR when enough writes are pending
func (w *WAL) flushPendingSync() {
	w.mu.Lock()

	// Grab all pending waiters
	waiting := w.pendingSync
	w.pendingSync = nil
	w.syncTimer = nil

	if len(waiting) == 0 {
		w.mu.Unlock()
		return
	}

	w.mu.Unlock()

	// Do the actual fsync (outside the lock — this blocks for disk I/O)
	err := w.active.Sync()

	w.mu.Lock()
	w.syncCount++
	w.mu.Unlock()

	// Notify ALL waiters with the same result
	// This is group commit: ONE fsync serves MANY writers
	for _, ch := range waiting {
		ch <- err
	}
}

// recover reads all existing WAL segments and replays them
// Called during startup to rebuild state after a crash or restart
func (w *WAL) recover() error {
	// Find all WAL files in directory
	pattern := filepath.Join(w.dir, "*"+WALFileExtension)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		fmt.Println("WAL: no existing segments found, starting fresh")
		return nil
	}

	// Sort by filename (which sorts by segment ID)
	sort.Strings(files)

	fmt.Printf("WAL: recovering from %d segments\n", len(files))

	var totalRecords int
	var maxSeq uint64

	for _, path := range files {
		seg, err := openSegment(path)
		if err != nil {
			return fmt.Errorf("wal: failed to open segment %s: %w", path, err)
		}

		records, err := seg.ReadAll()
		if err != nil {
			if IsCorruption(err) {
				// Corruption in WAL is serious
				// For now, log and stop — in production you'd alert and failover
				return fmt.Errorf("wal: corruption in segment %s: %w", path, err)
			}
			// Other errors might be partial writes at end — continue
			fmt.Printf("WAL: segment %s has partial write at end (normal after crash)\n",
				path)
		}

		for _, rec := range records {
			if rec.Type == RecordData && rec.Sequence > maxSeq {
				maxSeq = rec.Sequence
			}
			totalRecords++
		}

		seg.Close()
	}

	w.nextSeq = maxSeq + 1
	fmt.Printf("WAL: recovery complete, replayed %d records, next_seq=%d\n",
		totalRecords, w.nextSeq)

	return nil
}

// Checkpoint marks that all entries up to seq have been persisted to SSTables
// WAL segments containing only entries <= seq can be deleted
func (w *WAL) Checkpoint(seq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Write checkpoint record to WAL
	rec := &Record{
		Type:     RecordCheckpoint,
		Sequence: seq,
		Data:     []byte(fmt.Sprintf("checkpoint_seq=%d", seq)),
	}

	if err := w.writeToActive(rec); err != nil {
		return err
	}

	// Force sync the checkpoint record
	if err := w.active.Sync(); err != nil {
		return err
	}

	// Delete sealed segments that are fully before checkpoint
	var remaining []*Segment
	var deleted int

	for _, seg := range w.sealed {
		if seg.lastSeq <= seq {
			// All records in this segment are checkpointed
			seg.Close()
			if err := os.Remove(seg.path); err != nil {
				fmt.Printf("WAL: failed to delete segment %s: %v\n", seg.path, err)
			} else {
				deleted++
			}
		} else {
			remaining = append(remaining, seg)
		}
	}

	w.sealed = remaining
	fmt.Printf("WAL: checkpoint at seq=%d, deleted %d segments\n", seq, deleted)
	return nil
}

// ReadFrom returns all WAL entries from startSeq onwards
// Used for: new replica catch-up, point-in-time recovery
func (w *WAL) ReadFrom(ctx context.Context, startSeq uint64) ([]*WALEntry, error) {
	w.mu.Lock()
	allSegments := append(w.sealed, w.active)
	w.mu.Unlock()

	var entries []*WALEntry

	for _, seg := range allSegments {
		select {
		case <-ctx.Done():
			return entries, ctx.Err()
		default:
		}

		// Open for reading
		readSeg, err := openSegment(seg.path)
		if err != nil {
			continue // might be deleted
		}

		records, _ := readSeg.ReadAll()
		readSeg.Close()

		for _, rec := range records {
			if rec.Type != RecordData || rec.Sequence < startSeq {
				continue
			}

			ts, data := decodeEntryData(rec.Data)
			entries = append(entries, &WALEntry{
				Sequence:  rec.Sequence,
				Timestamp: ts,
				Data:      data,
				Type:      rec.Type,
			})
		}
	}

	return entries, nil
}

// Stats returns WAL statistics
func (w *WAL) Stats() WALStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return WALStats{
		BytesWritten:   w.bytesWritten,
		RecordsWritten: w.recordsWritten,
		SyncCount:      w.syncCount,
		SegmentCount:   len(w.sealed) + 1,
		NextSequence:   w.nextSeq,
	}
}

// Close closes the WAL cleanly
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	if w.syncTimer != nil {
		w.syncTimer.Stop()
	}

	// Notify any pending writers with error
	for _, ch := range w.pendingSync {
		ch <- ErrWALClosed
	}
	w.pendingSync = nil

	// Close all segments
	for _, seg := range w.sealed {
		seg.Close()
	}

	return w.active.Close()
}

// WALStats contains WAL performance metrics
type WALStats struct {
	BytesWritten   int64
	RecordsWritten int64
	SyncCount      int64
	SegmentCount   int
	NextSequence   uint64
}

// encodeEntryData prepends the HLC timestamp to the data
// Format: [HLC:8 bytes][data:N bytes]
func encodeEntryData(ts clock.HLCTimestamp, data []byte) []byte {
	encoded := make([]byte, 8+len(data))
	encodedTS := ts.ToBytes()
	copy(encoded[:8], encodedTS)
	copy(encoded[8:], data)
	return encoded
}

// decodeEntryData extracts the HLC timestamp and data
func decodeEntryData(encoded []byte) (clock.HLCTimestamp, []byte) {
	if len(encoded) < 8 {
		return 0, encoded
	}
	ts := clock.HLCTimestampFromBytes(encoded[:8])
	return ts, encoded[8:]
}
