package sstable

/*
 SSTable (Sorted String Table) — the immutable on-disk file

 Structure of an SSTable file:

  ┌────────────────────────────────────────────────────────┐
  │  DATA BLOCKS                                            │
  │  ┌──────────────────┐                                  │
  │  │ Block 0          │ ← 4KB of sorted key-value pairs  │
  │  │ [key1,val1]      │                                  │
  │  │ [key2,val2]      │                                  │
  │  │ [key3,val3]      │                                  │
  │  │ CRC32            │ ← block-level checksum           │
  │  └──────────────────┘                                  │
  │  ┌──────────────────┐                                  │
  │  │ Block 1          │                                  │
  │  │ [key4,val4]      │                                  │
  │  │ ...              │                                  │
  │  └──────────────────┘                                  │
  │  ...                                                    │
  ├────────────────────────────────────────────────────────┤
  │  INDEX BLOCK                                            │
  │  [key1 → block0_offset]                               │
  │  [key4 → block1_offset]                               │
  │  [keyN → blockN_offset]                               │
  │  (first key of each block → block offset)             │
  ├────────────────────────────────────────────────────────┤
  │  BLOOM FILTER                                           │
  │  [serialized bloom filter bytes]                       │
  │  (all keys in this SSTable)                           │
  ├────────────────────────────────────────────────────────┤
  │  FOOTER (fixed size, always at end of file)            │
  │  [index_offset:8][index_len:8]                        │
  │  [bloom_offset:8][bloom_len:8]                        │
  │  [min_key_len:4][min_key:N]                           │
  │  [max_key_len:4][max_key:N]                           │
  │  [entry_count:8]                                      │
  │  [magic:8] ← 0xHERMESDB for validation               │
  └────────────────────────────────────────────────────────┘

 WHY IMMUTABLE?
   - Once written, never modified → no locking needed for reads!
   - Multiple readers at full speed (no contention)
   - Corruption detection: if file changes, it's wrong
   - Simple compaction: just merge and create new file
*/
import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sort"

	"github.com/aadityabinodyadav/hermes/pkg/clock"
	"github.com/aadityabinodyadav/hermes/pkg/storage/bloom"
	"github.com/aadityabinodyadav/hermes/pkg/storage/memtable"
)

const (
	BlockSize = 4 * 1024 // 4KB

	FooterSize = 64

	MagicNumber = uint64(0x4845524D45534442) // "HERMESDB"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type IndexEntry struct {
	Key    string // first key in the block
	Offset int64  // byte offset of block in file
	Length int32  // length of block in bytes
}

type SSTableMeta struct {
	Path       string
	MinKey     string // smallest key in this SSTable
	MaxKey     string // largest key in this SSTable
	EntryCount int64
	FileSize   int64
	Level      int // which LSM-Tree level (0, 1, 2, ...)

	Filter *bloom.BloomFilter

	Index []IndexEntry
}

func (m *SSTableMeta) MightContain(key string) bool {
	if key < m.MinKey || key > m.MaxKey {
		return false
	}
	if m.Filter != nil && !m.Filter.Contains(key) {
		return false
	}
	return true
}

func (m *SSTableMeta) FindBlock(key string) (offset int64, length int32) {
	idx := sort.Search(len(m.Index), func(i int) bool {
		return m.Index[i].Key > key
	}) - 1

	if idx < 0 {
		return -1, 0
	}
	return m.Index[idx].Offset, m.Index[idx].Length
}

type Writer struct {
	file   *os.File
	writer *bufio.Writer

	offset      int64
	blockBuf    []byte
	blockOffset int64
	blockKeys   []string

	index []IndexEntry

	filter *bloom.BloomFilter

	entryCount int64
	minKey     string
	maxKey     string
}

func NewWriter(path string, expectedKeys int) (*Writer, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: failed to create %s: %w", path, err)
	}

	return &Writer{
		file:   file,
		writer: bufio.NewWriterSize(file, 256*1024),   // 256KB write buffer
		filter: bloom.New(uint64(expectedKeys), 0.01), // 1% false positive rate
	}, nil
}

func (w *Writer) Add(entry *memtable.Entry) error {
	encoded := encodeEntry(entry)

	if len(w.blockBuf)+len(encoded) > BlockSize && len(w.blockBuf) > 0 {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	if len(w.blockBuf) == 0 {
		w.blockOffset = w.offset
		w.blockKeys = nil
	}

	w.blockBuf = append(w.blockBuf, encoded...)
	w.blockKeys = append(w.blockKeys, entry.Key)

	w.filter.Add(entry.Key)

	w.entryCount++
	if w.minKey == "" || entry.Key < w.minKey {
		w.minKey = entry.Key
	}
	if entry.Key > w.maxKey {
		w.maxKey = entry.Key
	}

	return nil
}

func (w *Writer) flushBlock() error {
	if len(w.blockBuf) == 0 {
		return nil
	}

	crc := crc32.Checksum(w.blockBuf, crcTable)

	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:4], crc)
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(w.blockBuf)))

	n, err := w.writer.Write(header)
	if err != nil {
		return err
	}
	w.offset += int64(n)

	n, err = w.writer.Write(w.blockBuf)
	if err != nil {
		return err
	}
	w.offset += int64(n)

	if len(w.blockKeys) > 0 {
		blockLen := int32(8 + len(w.blockBuf)) // header + data
		w.index = append(w.index, IndexEntry{
			Key:    w.blockKeys[0],
			Offset: w.blockOffset,
			Length: blockLen,
		})
	}

	w.blockBuf = w.blockBuf[:0]
	w.blockKeys = nil

	return nil
}

func (w *Writer) Finish() (*SSTableMeta, error) {
	if err := w.flushBlock(); err != nil {
		return nil, err
	}

	indexOffset := w.offset
	if err := w.writeIndex(); err != nil {
		return nil, err
	}
	indexLen := w.offset - indexOffset

	bloomOffset := w.offset
	bloomData := w.filter.Serialize()
	n, err := w.writer.Write(bloomData)
	if err != nil {
		return nil, err
	}
	w.offset += int64(n)
	bloomLen := int64(n)

	if err := w.writeFooter(
		indexOffset, indexLen,
		bloomOffset, bloomLen,
	); err != nil {
		return nil, err
	}

	if err := w.writer.Flush(); err != nil {
		return nil, err
	}

	if err := w.file.Sync(); err != nil {
		return nil, fmt.Errorf("sstable: fsync failed: %w", err)
	}

	fileInfo, _ := w.file.Stat()
	fileSize := int64(0)
	if fileInfo != nil {
		fileSize = fileInfo.Size()
	}

	w.file.Close()

	return &SSTableMeta{
		Path:       w.file.Name(),
		MinKey:     w.minKey,
		MaxKey:     w.maxKey,
		EntryCount: w.entryCount,
		FileSize:   fileSize,
		Filter:     w.filter,
		Index:      w.index,
	}, nil
}

func (w *Writer) writeIndex() error {
	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(len(w.index)))
	n, err := w.writer.Write(countBuf)
	if err != nil {
		return err
	}
	w.offset += int64(n)

	for _, entry := range w.index {
		keyLen := make([]byte, 4)
		binary.LittleEndian.PutUint32(keyLen, uint32(len(entry.Key)))
		w.writer.Write(keyLen)
		w.offset += 4

		w.writer.Write([]byte(entry.Key))
		w.offset += int64(len(entry.Key))

		offsetBuf := make([]byte, 12)
		binary.LittleEndian.PutUint64(offsetBuf[0:8], uint64(entry.Offset))
		binary.LittleEndian.PutUint32(offsetBuf[8:12], uint32(entry.Length))
		w.writer.Write(offsetBuf)
		w.offset += 12
	}

	return nil
}

func (w *Writer) writeFooter(
	indexOffset, indexLen,
	bloomOffset, bloomLen int64,
) error {
	footer := make([]byte, FooterSize)

	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(indexLen))
	binary.LittleEndian.PutUint64(footer[16:24], uint64(bloomOffset))
	binary.LittleEndian.PutUint64(footer[24:32], uint64(bloomLen))

	minKeyLen := len(w.minKey)
	if minKeyLen > 12 {
		minKeyLen = 12
	}
	binary.LittleEndian.PutUint32(footer[32:36], uint32(minKeyLen))
	copy(footer[36:48], w.minKey[:minKeyLen])

	maxKeyLen := len(w.maxKey)
	if maxKeyLen > 12 {
		maxKeyLen = 12
	}
	binary.LittleEndian.PutUint32(footer[48:52], uint32(maxKeyLen))
	copy(footer[52:64-8-4], w.maxKey[:maxKeyLen])

	binary.LittleEndian.PutUint64(footer[FooterSize-8:], MagicNumber)

	n, err := w.writer.Write(footer)
	w.offset += int64(n)
	return err
}

type Reader struct {
	file *os.File
	meta *SSTableMeta
}

func OpenReader(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: failed to open %s: %w", path, err)
	}

	reader := &Reader{file: file}

	if err := reader.loadMeta(path); err != nil {
		file.Close()
		return nil, err
	}

	return reader, nil
}

func (r *Reader) loadMeta(path string) error {
	fileInfo, err := r.file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	footer := make([]byte, FooterSize)
	if _, err := r.file.ReadAt(footer, fileSize-FooterSize); err != nil {
		return fmt.Errorf("sstable: failed to read footer: %w", err)
	}

	magic := binary.LittleEndian.Uint64(footer[FooterSize-8:])
	if magic != MagicNumber {
		return fmt.Errorf("sstable: invalid magic number: 0x%x", magic)
	}

	indexOffset := int64(binary.LittleEndian.Uint64(footer[0:8]))
	indexLen := int64(binary.LittleEndian.Uint64(footer[8:16]))
	bloomOffset := int64(binary.LittleEndian.Uint64(footer[16:24]))
	bloomLen := int64(binary.LittleEndian.Uint64(footer[24:32]))

	bloomData := make([]byte, bloomLen)
	if _, err := r.file.ReadAt(bloomData, bloomOffset); err != nil {
		return fmt.Errorf("sstable: failed to read bloom filter: %w", err)
	}
	filter, err := bloom.Deserialize(bloomData)
	if err != nil {
		return fmt.Errorf("sstable: failed to deserialize bloom filter: %w", err)
	}

	indexData := make([]byte, indexLen)
	if _, err := r.file.ReadAt(indexData, indexOffset); err != nil {
		return fmt.Errorf("sstable: failed to read index: %w", err)
	}
	index, err := decodeIndex(indexData)
	if err != nil {
		return fmt.Errorf("sstable: failed to decode index: %w", err)
	}

	r.meta = &SSTableMeta{
		Path:     path,
		FileSize: fileSize,
		Filter:   filter,
		Index:    index,
	}

	return nil
}

func (r *Reader) Get(key string) (*memtable.Entry, bool, error) {
	if !r.meta.MightContain(key) {
		return nil, false, nil
	}

	blockOffset, blockLen := r.meta.FindBlock(key)
	if blockOffset < 0 {
		return nil, false, nil
	}

	block, err := r.readBlock(blockOffset, blockLen)
	if err != nil {
		return nil, false, err
	}

	entry := searchBlock(block, key)
	if entry == nil {
		return nil, false, nil
	}

	return entry, true, nil
}

func (r *Reader) readBlock(offset int64, length int32) ([]byte, error) {
	buf := make([]byte, length)
	if _, err := r.file.ReadAt(buf, offset); err != nil {
		return nil, fmt.Errorf("sstable: failed to read block: %w", err)
	}

	storedCRC := binary.LittleEndian.Uint32(buf[0:4])
	dataLen := binary.LittleEndian.Uint32(buf[4:8])
	data := buf[8 : 8+dataLen]

	computedCRC := crc32.Checksum(data, crcTable)
	if computedCRC != storedCRC {
		return nil, fmt.Errorf("sstable: block corruption at offset %d: crc mismatch", offset)
	}

	return data, nil
}

func (r *Reader) Iterator() *SSTableIterator {
	return &SSTableIterator{
		reader:     r,
		blockIndex: 0,
		entries:    nil,
		entryIndex: 0,
	}
}

func (r *Reader) Close() error {
	return r.file.Close()
}

func (r *Reader) Meta() *SSTableMeta {
	return r.meta
}

type SSTableIterator struct {
	reader     *Reader
	blockIndex int
	entries    []*memtable.Entry
	entryIndex int
}

func (it *SSTableIterator) Next() (*memtable.Entry, error) {
	for it.entryIndex >= len(it.entries) {
		if it.blockIndex >= len(it.reader.meta.Index) {
			return nil, nil
		}

		indexEntry := it.reader.meta.Index[it.blockIndex]
		it.blockIndex++

		block, err := it.reader.readBlock(indexEntry.Offset, indexEntry.Length)
		if err != nil {
			return nil, err
		}

		it.entries = decodeBlock(block)
		it.entryIndex = 0
	}

	entry := it.entries[it.entryIndex]
	it.entryIndex++
	return entry, nil
}

func encodeEntry(e *memtable.Entry) []byte {
	keyBytes := []byte(e.Key)

	size := 4 + len(keyBytes) + 4 + len(e.Value) + 8 + 1
	buf := make([]byte, size)

	offset := 0

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(keyBytes)))
	offset += 4
	copy(buf[offset:], keyBytes)
	offset += len(keyBytes)

	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(e.Value)))
	offset += 4
	copy(buf[offset:], e.Value)
	offset += len(e.Value)

	tsBytes := e.Timestamp.ToBytes()
	copy(buf[offset:offset+8], tsBytes)
	offset += 8

	if e.Deleted {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}

	return buf
}

func decodeBlock(data []byte) []*memtable.Entry {
	var entries []*memtable.Entry
	offset := 0

	for offset < len(data) {
		if offset+4 > len(data) {
			break
		}

		keyLen := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4

		if offset+keyLen > len(data) {
			break
		}
		key := string(data[offset : offset+keyLen])
		offset += keyLen

		if offset+4 > len(data) {
			break
		}
		valLen := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4

		if offset+valLen > len(data) {
			break
		}
		value := make([]byte, valLen)
		copy(value, data[offset:offset+valLen])
		offset += valLen

		if offset+8 > len(data) {
			break
		}
		ts := clock.HLCTimestampFromBytes(data[offset : offset+8])
		offset += 8

		deleted := false
		if offset < len(data) {
			deleted = data[offset] == 1
			offset++
		}

		entries = append(entries, &memtable.Entry{
			Key:       key,
			Value:     value,
			Timestamp: ts,
			Deleted:   deleted,
		})
	}

	return entries
}

func searchBlock(data []byte, key string) *memtable.Entry {
	entries := decodeBlock(data)
	for _, e := range entries {
		if e.Key == key {
			return e
		}
	}
	return nil
}

func decodeIndex(data []byte) ([]IndexEntry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("index data too short")
	}

	count := int(binary.LittleEndian.Uint32(data[0:4]))
	offset := 4

	entries := make([]IndexEntry, 0, count)

	for i := 0; i < count; i++ {
		if offset+4 > len(data) {
			break
		}
		keyLen := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4

		if offset+keyLen > len(data) {
			break
		}
		key := string(data[offset : offset+keyLen])
		offset += keyLen

		if offset+12 > len(data) {
			break
		}
		blockOffset := int64(binary.LittleEndian.Uint64(data[offset:]))
		blockLen := int32(binary.LittleEndian.Uint32(data[offset+8:]))
		offset += 12

		entries = append(entries, IndexEntry{
			Key:    key,
			Offset: blockOffset,
			Length: blockLen,
		})
	}

	return entries, nil
}
