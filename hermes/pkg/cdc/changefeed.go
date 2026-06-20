// pkg/cdc/changefeed.go
package cdc

// ChangeFeed streams changes from Hermes to external systems
//
// Reading from the WAL:
//   Hermes WAL has every write in order.
//   CDC reads the WAL and publishes changes as events.
//   This is zero-overhead: WAL is already written.
//
// Exactly-once delivery challenge:
//   CDC must track what it has already delivered.
//   If CDC process crashes, it resumes from checkpoint.
//   Downstream must be idempotent (or we use Kafka transactions).
//
// OUTBOX PATTERN (most reliable):
//   Instead of reading WAL directly:
//   Application writes to 'changes' table in same transaction.
//   CDC reads from 'changes' table.
//   Guarantees: if write committed, change event will be produced.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ChangeEvent represents one change to the data store
type ChangeEvent struct {
	// EventID is a monotonically increasing ID for this event
	EventID uint64

	// Timestamp is the HLC timestamp of the change
	Timestamp int64

	// Operation type
	Op ChangeOp

	// Key that changed
	Key string

	// Before image (nil for inserts)
	BeforeValue []byte

	// After image (nil for deletes)
	AfterValue []byte

	// Version/sequence for ordering
	Version uint64

	// ShardID where the change happened
	ShardID uint64

	// NodeID that was the leader when change happened
	NodeID string
}

type ChangeOp uint8

const (
	OpInsert ChangeOp = 0
	OpUpdate ChangeOp = 1
	OpDelete ChangeOp = 2
)

func (op ChangeOp) String() string {
	switch op {
	case OpInsert:
		return "INSERT"
	case OpUpdate:
		return "UPDATE"
	case OpDelete:
		return "DELETE"
	}
	return "UNKNOWN"
}

// ─────────────────────────────────────────────────────────────────────────────
// CHANGEFEED
// ─────────────────────────────────────────────────────────────────────────────

// ChangeFeedConfig configures a change feed
type ChangeFeedConfig struct {
	// ID uniquely identifies this feed
	ID string

	// KeyPrefix: only emit changes for keys with this prefix
	// Empty = all keys
	KeyPrefix string

	// StartAt: resume from this version (0 = start from now)
	StartAt uint64

	// BatchSize: max events per batch
	BatchSize int

	// FlushInterval: how often to flush even if batch isn't full
	FlushInterval time.Duration
}

// DefaultChangeFeedConfig returns sensible defaults
func DefaultChangeFeedConfig(id string) ChangeFeedConfig {
	return ChangeFeedConfig{
		ID:            id,
		BatchSize:     100,
		FlushInterval: 100 * time.Millisecond,
	}
}

// Sink is the destination for change events
type Sink interface {
	// Publish sends a batch of events to the sink
	Publish(ctx context.Context, events []*ChangeEvent) error

	// Checkpoint saves the last processed event ID
	// On restart, feed resumes from this point
	Checkpoint(ctx context.Context, eventID uint64) error
}

// ChangeFeed reads changes from the WAL and delivers to a Sink
type ChangeFeed struct {
	config    ChangeFeedConfig
	sink      Sink
	walReader WALReader

	// State
	lastEventID uint64
	lastVersion uint64
	running     int32

	// Stats
	eventsDelivered uint64
	bytesDelivered  uint64

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// WALReader reads from the Write-Ahead Log
type WALReader interface {
	// ReadFrom returns events starting from the given version
	ReadFrom(ctx context.Context, fromVersion uint64) (<-chan *WALEntry, error)
}

// WALEntry is one entry from the WAL
type WALEntry struct {
	Sequence  uint64
	Timestamp int64
	Key       string
	Value     []byte
	Deleted   bool
	ShardID   uint64
	NodeID    string
}

// NewChangeFeed creates a new change feed
func NewChangeFeed(config ChangeFeedConfig, sink Sink, walReader WALReader) *ChangeFeed {
	ctx, cancel := context.WithCancel(context.Background())

	return &ChangeFeed{
		config:    config,
		sink:      sink,
		walReader: walReader,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Start begins streaming changes
func (f *ChangeFeed) Start() error {
	if !atomic.CompareAndSwapInt32(&f.running, 0, 1) {
		return fmt.Errorf("changefeed %s already running", f.config.ID)
	}

	f.wg.Add(1)
	go f.run()

	fmt.Printf("[CDC] Feed %s started (prefix=%q, startAt=%d)\n",
		f.config.ID, f.config.KeyPrefix, f.config.StartAt)

	return nil
}

// Stop gracefully stops the feed
func (f *ChangeFeed) Stop() {
	f.cancel()
	f.wg.Wait()
	fmt.Printf("[CDC] Feed %s stopped (delivered=%d events)\n",
		f.config.ID, atomic.LoadUint64(&f.eventsDelivered))
}

// run is the main loop for reading and publishing changes
func (f *ChangeFeed) run() {
	defer f.wg.Done()
	defer atomic.StoreInt32(&f.running, 0)

	// Start from configured position
	fromVersion := f.config.StartAt

	entryCh, err := f.walReader.ReadFrom(f.ctx, fromVersion)
	if err != nil {
		fmt.Printf("[CDC] Failed to start reading WAL: %v\n", err)
		return
	}

	batch := make([]*ChangeEvent, 0, f.config.BatchSize)
	ticker := time.NewTicker(f.config.FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		// Publish batch
		if err := f.sink.Publish(f.ctx, batch); err != nil {
			fmt.Printf("[CDC] Failed to publish batch: %v\n", err)
			return
		}

		// Checkpoint the last event
		lastID := batch[len(batch)-1].EventID
		if err := f.sink.Checkpoint(f.ctx, lastID); err != nil {
			fmt.Printf("[CDC] Failed to checkpoint: %v\n", err)
		}

		atomic.AddUint64(&f.eventsDelivered, uint64(len(batch)))
		f.lastEventID = lastID

		batch = batch[:0]
	}

	for {
		select {
		case <-f.ctx.Done():
			flush() // final flush
			return

		case entry, ok := <-entryCh:
			if !ok {
				// WAL stream ended (shouldn't happen normally)
				flush()
				return
			}

			// Filter by key prefix
			if f.config.KeyPrefix != "" {
				if len(entry.Key) < len(f.config.KeyPrefix) ||
					entry.Key[:len(f.config.KeyPrefix)] != f.config.KeyPrefix {
					continue
				}
			}

			// Build change event
			f.lastEventID++
			event := &ChangeEvent{
				EventID:   f.lastEventID,
				Timestamp: entry.Timestamp,
				Key:       entry.Key,
				Version:   entry.Sequence,
				ShardID:   entry.ShardID,
				NodeID:    entry.NodeID,
			}

			if entry.Deleted {
				event.Op = OpDelete
			} else if entry.Sequence <= fromVersion {
				// We have a before image only if we can read previous version
				event.Op = OpUpdate
				event.AfterValue = entry.Value
			} else {
				event.Op = OpInsert
				event.AfterValue = entry.Value
			}

			batch = append(batch, event)

			// Flush if batch is full
			if len(batch) >= f.config.BatchSize {
				flush()
			}

		case <-ticker.C:
			// Periodic flush
			flush()
		}
	}
}

// Stats returns current feed statistics
func (f *ChangeFeed) Stats() ChangeFeedStats {
	return ChangeFeedStats{
		FeedID:          f.config.ID,
		EventsDelivered: atomic.LoadUint64(&f.eventsDelivered),
		LastEventID:     f.lastEventID,
		IsRunning:       atomic.LoadInt32(&f.running) == 1,
	}
}

type ChangeFeedStats struct {
	FeedID          string
	EventsDelivered uint64
	LastEventID     uint64
	IsRunning       bool
}

// ─────────────────────────────────────────────────────────────────────────────
// BUILT-IN SINKS
// ─────────────────────────────────────────────────────────────────────────────

// LogSink writes changes to a logger (for debugging)
type LogSink struct {
	prefix string
}

func NewLogSink(prefix string) *LogSink {
	return &LogSink{prefix: prefix}
}

func (s *LogSink) Publish(_ context.Context, events []*ChangeEvent) error {
	for _, e := range events {
		fmt.Printf("[%s] %s key=%s version=%d\n",
			s.prefix, e.Op, e.Key, e.Version)
		if e.AfterValue != nil {
			fmt.Printf("  after: %s\n", truncate(string(e.AfterValue), 50))
		}
	}
	return nil
}

func (s *LogSink) Checkpoint(_ context.Context, eventID uint64) error {
	fmt.Printf("[%s] Checkpoint: eventID=%d\n", s.prefix, eventID)
	return nil
}

// ChannelSink sends changes to a Go channel (for testing)
type ChannelSink struct {
	ch chan *ChangeEvent
}

func NewChannelSink(bufSize int) *ChannelSink {
	return &ChannelSink{ch: make(chan *ChangeEvent, bufSize)}
}

func (s *ChannelSink) Publish(_ context.Context, events []*ChangeEvent) error {
	for _, e := range events {
		select {
		case s.ch <- e:
		default:
			return fmt.Errorf("channel sink buffer full")
		}
	}
	return nil
}

func (s *ChannelSink) Checkpoint(_ context.Context, eventID uint64) error {
	return nil // no-op for channel sink
}

func (s *ChannelSink) Events() <-chan *ChangeEvent {
	return s.ch
}

// FanOutSink distributes changes to multiple sinks
type FanOutSink struct {
	sinks []Sink
}

func NewFanOutSink(sinks ...Sink) *FanOutSink {
	return &FanOutSink{sinks: sinks}
}

func (s *FanOutSink) Publish(ctx context.Context, events []*ChangeEvent) error {
	for _, sink := range s.sinks {
		if err := sink.Publish(ctx, events); err != nil {
			return err
		}
	}
	return nil
}

func (s *FanOutSink) Checkpoint(ctx context.Context, eventID uint64) error {
	for _, sink := range s.sinks {
		if err := sink.Checkpoint(ctx, eventID); err != nil {
			return err
		}
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
