package wal

import (
	"sync"
	"time"

	"github.com/aadityabinodyadav/hermes/pkg/clock"
)

/* The Write-Ahead Log (WAL)

This is THE most critical component of Hermes's storage layer.
Every write MUST go through the WAL before being acknowledged.

Durability guarantee:
  "If we ACK a write to the client, that write will survive
   ANY single-node failure (power cut, OS crash, disk error
   except disk physical destruction)"

Group Commit (crucial optimization):
  Instead of calling fsync() after EVERY write:
  - Buffer N writes OR wait T milliseconds
  - Call fsync() ONCE for all buffered writes
  - Notify all waiting writers

  Without group commit: 1 fsync per write → ~1000 writes/sec (HDD)
  With group commit:    1 fsync per batch → ~100,000 writes/sec

  This is exactly what PostgreSQL, MySQL, and etcd do.
  The tradeoff: slightly higher LATENCY, much higher THROUGHPUT.

WAL recovery (startup sequence):
  1. Find all WAL segment files in directory
  2. Open each segment in order
  3. Read and replay all records
  4. Rebuild MemTable from replayed records
 5. Continue normal operation
*/

type WALEntry struct {
	Sequence  uint64
	Timestamp clock.HLCTimestamp
	Data      []byte
	Type      RecordType
}

type WAL struct {
	mu             sync.Mutex
	dir            string
	active         *Segment
	sealed         []*Segment
	nextSeq        uint64
	clock          *clock.HLC
	segmentSize    int64
	syncEvery      time.Duration
	pendingSync    []chan error
	syncTimer      *time.Timer
	closed         bool
	bytesWritten   int64
	recordsWritten int64
	syncCount      int64
}

//open
//write
//writesync
//writetoactive
//rotatesegment
//flushpendingsync
//recover
//checkpoint
//readfrom
//stats
//close

type WALState struct {
	BytesWritten   int64
	RecordsWritten int64
	SyncCount      int64
	SegmentCount   int
	NextSequence   uint64
}

//encodeentrydata
//decodeentrydata
//
