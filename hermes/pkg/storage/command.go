// pkg/storage/command.go
package storage

// Command is the unit of replication in Hermes.
// Every client write becomes a Command, which gets:
//   1. Serialized here
//   2. Written to WAL
//   3. Proposed to Raft
//   4. Applied to storage engine on commit
//
// This file is the SINGLE SOURCE OF TRUTH for command encoding.
// Every package that needs to encode/decode a command imports THIS.
// No more scattered encoding logic.

import (
	"encoding/binary"
	"fmt"
)

// CommandType identifies the operation
type CommandType uint8

const (
	CmdPut    CommandType = 1
	CmdDelete CommandType = 2
	CmdCAS    CommandType = 3 // Compare-And-Swap
)

// Command is a single KV operation ready for replication
type Command struct {
	Type    CommandType
	Key     string
	Value   []byte
	Version int64 // for CAS: expected version (-1 = don't check)
}

// Encode serializes a Command to bytes
// Format: [type:1][key_len:4][key:N][val_len:4][val:N][version:8]
func (c *Command) Encode() []byte {
	keyBytes := []byte(c.Key)
	size := 1 + 4 + len(keyBytes) + 4 + len(c.Value) + 8
	buf := make([]byte, size)

	offset := 0
	buf[offset] = byte(c.Type)
	offset++

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(keyBytes)))
	offset += 4
	copy(buf[offset:], keyBytes)
	offset += len(keyBytes)

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(c.Value)))
	offset += 4
	copy(buf[offset:], c.Value)
	offset += len(c.Value)

	binary.LittleEndian.PutUint64(buf[offset:], uint64(c.Version))

	return buf
}

// DecodeCommand deserializes bytes back to key, value, deleted
// This is the function referenced from pkg/server/node.go
func DecodeCommand(data []byte) (key string, value []byte, deleted bool) {
	if len(data) < 5 {
		return "", nil, false
	}

	cmdType := CommandType(data[0])
	offset := 1

	keyLen := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	if offset+keyLen > len(data) {
		return "", nil, false
	}
	key = string(data[offset : offset+keyLen])
	offset += keyLen

	if offset+4 > len(data) {
		return key, nil, cmdType == CmdDelete
	}
	valLen := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4

	if valLen > 0 && offset+valLen <= len(data) {
		value = data[offset : offset+valLen]
	}

	return key, value, cmdType == CmdDelete
}

// EncodeCommand is the convenience function used by the Raft state machine
func EncodeCommand(key string, value []byte, deleted bool) []byte {
	cmdType := CmdPut
	if deleted {
		cmdType = CmdDelete
	}
	cmd := &Command{
		Type:    cmdType,
		Key:     key,
		Value:   value,
		Version: -1,
	}
	return cmd.Encode()
}

// NewPutCommand creates a PUT command
func NewPutCommand(key string, value []byte) []byte {
	return EncodeCommand(key, value, false)
}

// NewDeleteCommand creates a DELETE command
func NewDeleteCommand(key string) []byte {
	return EncodeCommand(key, nil, true)
}

// ValidateCommand checks if encoded bytes are a valid command
func ValidateCommand(data []byte) error {
	if len(data) < 5 {
		return fmt.Errorf("command too short: %d bytes", len(data))
	}
	cmdType := CommandType(data[0])
	switch cmdType {
	case CmdPut, CmdDelete, CmdCAS:
		return nil
	default:
		return fmt.Errorf("unknown command type: %d", cmdType)
	}
}
