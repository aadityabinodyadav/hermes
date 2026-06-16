package wal

import "fmt"

var (
	ErrRecordTooShort  = fmt.Errorf("wal: record too short")
	ErrSegmentFull     = fmt.Errorf("wal: segment is full")
	ErrWALClosed       = fmt.Errorf("wal: WAL is closed")
	ErrInvalidSequence = fmt.Errorf("wal: invalid sequence number")
)

type ErrCorruption struct {
	Sequence    uint64
	StoredCRC   uint32
	ComputedCRC uint32
}

func (e *ErrCorruption) Error() string {
	return fmt.Sprintf(
		"wal: corruption detected at sequence=%d: stored_crc=0x%08x computed_crc=0x%08x",
		e.Sequence, e.StoredCRC, e.ComputedCRC,
	)
}

func IsCorruption(err error) bool {
	_, ok := err.(*ErrCorruption)
	return ok
}
