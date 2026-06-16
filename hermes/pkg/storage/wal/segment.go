package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

/* WAL Segment Management

The WAL is split into segments (individual files).
Each segment has a max size (e.g., 64MB).
When a segment is full, we create a new one.

WHY SEGMENTS?
  - Easier to delete old data (just delete old segment files)
  - Bounded recovery time (don't replay entire history, just recent segments)
  - Parallel compaction (compact old segments while writing new ones)

File naming: 000000001.wal, 000000002.wal, etc.
The number is the FIRST sequence number in that segment.

Segment lifecycle:
  ACTIVE   → currently being written to
  SEALED   → full, closed, being compacted or waiting for deletion
  DELETED  → safe to delete (all entries are in SSTables)

  ┌──────────────────────────────────────────────────────┐
  │  000000001.wal │ 000000002.wal │ 000000003.wal(ACTIVE)│
  │  [SEALED]      │ [SEALED]      │                      │
  │  can delete    │ keep for now  │ writing here         │
  └──────────────────────────────────────────────────────┘
             ↑                             ↑
       last checkpoint              current write position

*/

const (
	DefaultSegmentSize = 64 * 1024 * 1024
	WALFileExtension   = ".wal"
)

type Segment struct {
	mu         sync.RWMutex
	id         uint64
	file       *os.File
	path       string
	size       int64
	maxSize    int64
	firstSeq   uint64
	lastSeq    uint64
	closed     bool
	syncNeeded bool
}

func newSegment(dir string, id uint64, maxSize int64) (*Segment, error) {
	filename := fmt.Sprintf("%020d%s", id, WALFileExtension)
	path := filepath.Join(dir, filename)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment %s: %w", path, err)
	}

	seg := &Segment{
		id:      id,
		file:    file,
		path:    path,
		maxSize: maxSize,
	}

	header := &Record{
		Type:     RecordSegmentHeader,
		Sequence: id,
		Data:     []byte(fmt.Sprintf("segment_id=%d", id)),
	}

	if err := seg.writeRecord(header); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write segment header: %w", err)
	}

	return seg, nil
}

func openSegment(path string) (*Segment, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open  segment %s: %w", path, err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	return &Segment{
		file: file,
		path: path,
		size: info.Size(),
	}, nil
}

func (s *Segment) writeRecord(rec *Record) error {
	encoded := rec.Encode()

	if s.size+int64(len(encoded)) > s.maxSize {
		return ErrSegmentFull
	}

	n, err := s.file.Write(encoded)
	if err != nil {
		return fmt.Errorf("segment write failed: %w", err)
	}
	if n != len(encoded) {
		return fmt.Errorf("segment short write: wrote %d of %d bytes", n, len(encoded))
	}

	s.size += int64(n)
	s.lastSeq = rec.Sequence
	s.syncNeeded = true

	if s.firstSeq == 0 && rec.Type == RecordData {
		s.firstSeq = rec.Sequence
	}

	return nil

}

func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.syncNeeded || s.closed {
		return nil
	}

	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("segment sync failed: %w", err)
	}

	s.syncNeeded = false
	return nil
}

func (s *Segment) IsFull() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size >= s.maxSize
}

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	if s.syncNeeded {
		if err := s.file.Sync(); err != nil {
			s.file.Close()
			return err
		}
	}

	return s.file.Close()
}

func (s *Segment) ReadAll() ([]*Record, error) {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var records []*Record

	for {
		var payloadLen uint32
		err := binary.Read(s.file, binary.LittleEndian, &payloadLen)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Truncated length field — partial write at end, stop here
			// This is EXPECTED after a crash — not an error
			break
		}

		if payloadLen > MaxRecordSize {
			// Clearly corrupted — stop reading
			break
		}

		fullRecord := make([]byte, 4+payloadLen)
		binary.LittleEndian.PutUint32(fullRecord[0:4], payloadLen)

		_, err = io.ReadFull(s.file, fullRecord[4:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read error: %w", err)
		}

		rec, err := Decode(fullRecord)
		if err != nil {
			if IsCorruption(err) {
				return nil, err
			}
			break
		}

		records = append(records, rec)
	}

	return records, nil
}
