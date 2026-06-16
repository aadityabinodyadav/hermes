package wal

import (
	"encoding/binary"
	"hash/crc32"
)

/*
summarisation

wal record format is:
length (4 bytes) | CRC32 (4bytes) | type (1 byte) | sequence (8 bytes) | data (length based) |

Headers --length   -> how many bytes is the data field
    		CRC32  -> checksum of (type + sequence + data)
       		type   -> what kind of record is it (data, segment boundary ...)
       --sequence -> monotonically increasing ID for ordering
Data     -> data nai ho lol but serialised command proto

why crc3?
- disks can silently corrupt data due to bugs or bit corruption
- ssd can return stale data after power loss

why length prefix?
- wal is append only, we read it sequentiallly during recovery
- without length: we dont know where one record ends and next begins
- with length: read 4 bytes -> know exactly how many bytes to read next
*/

type RecordType uint32

const (
	RecordData          RecordType = 1
	RecordCheckpoint    RecordType = 2
	RecordSegmentHeader RecordType = 3
)

const (
	HeaderSize    = 17
	MaxRecordSize = 64 * 1024 * 1024
)

type Record struct {
	CRC      uint32
	Type     RecordType
	Sequence uint64
	Data     []byte
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func (r *Record) Encode() []byte {
	dataLen := len(r.Data)
	buf := make([]byte, dataLen+HeaderSize)

	payloadLen := uint32(4 + 1 + 8 + dataLen)
	binary.LittleEndian.PutUint32(buf[0:4], payloadLen)

	crcBuf := make([]byte, 1+8+dataLen)
	crcBuf[0] = byte(r.Type)
	binary.BigEndian.PutUint64(crcBuf[1:9], r.Sequence)
	copy(crcBuf[9:], r.Data)
	r.CRC = crc32.Checksum(crcBuf, crcTable)

	binary.BigEndian.PutUint32(buf[4:8], r.CRC)
	buf[8] = byte(r.Type)

	binary.LittleEndian.PutUint64(buf[9:17], r.Sequence)

	copy(buf[17:], r.Data)

	return buf

}

func Decode(buf []byte) (*Record, error) {
	if len(buf) < HeaderSize {
		return nil, ErrRecordTooShort
	}

	storedCRC := binary.LittleEndian.Uint32(buf[4:8])

	recordType := RecordType(buf[8])

	sequence := binary.LittleEndian.Uint64(buf[9:17])

	data := buf[17:]

	crcBuf := make([]byte, 1+8+len(data))
	crcBuf[0] = byte(recordType)
	binary.BigEndian.PutUint64(crcBuf[1:9], sequence)
	copy(crcBuf[9:], data)
	computedCRC := crc32.Checksum(crcBuf, crcTable)

	if computedCRC != storedCRC {
		return nil, &ErrCorruption{
			Sequence:    sequence,
			StoredCRC:   storedCRC,
			ComputedCRC: computedCRC,
		}
	}

	return &Record{
		Type:     recordType,
		Sequence: sequence,
		Data:     data,
		CRC:      storedCRC,
	}, nil

}
