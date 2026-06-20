package bloom


import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
)

type BloomFilter struct {
	bitset []uint64

	m uint64

	k uint64

	count uint64
}

func New(n uint64, p float64) *BloomFilter {
	m := uint64(math.Ceil(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2))))

	m = ((m + 63) / 64) * 64

	k := uint64(math.Round(float64(m) / float64(n) * math.Log(2)))
	if k < 1 {
		k = 1
	}
	if k > 20 {
		k = 20
	}

	return &BloomFilter{
		bitset: make([]uint64, m/64),
		m:      m,
		k:      k,
	}
}

func NewWithParams(m, k uint64) *BloomFilter {
	m = ((m + 63) / 64) * 64
	return &BloomFilter{
		bitset: make([]uint64, m/64),
		m:      m,
		k:      k,
	}
}

func (bf *BloomFilter) Add(key string) {
	h1, h2 := hash128(key)

	for i := uint64(0); i < bf.k; i++ {
		pos := (h1 + i*h2) % bf.m
		bf.setBit(pos)
	}

	bf.count++
}

func (bf *BloomFilter) Contains(key string) bool {
	h1, h2 := hash128(key)

	for i := uint64(0); i < bf.k; i++ {
		pos := (h1 + i*h2) % bf.m
		if !bf.getBit(pos) {
			return false
		}
	}

	return true
}

func (bf *BloomFilter) setBit(pos uint64) {
	wordIdx := pos / 64
	bitIdx := pos % 64
	bf.bitset[wordIdx] |= 1 << bitIdx
}

func (bf *BloomFilter) getBit(pos uint64) bool {
	wordIdx := pos / 64
	bitIdx := pos % 64
	return (bf.bitset[wordIdx]>>bitIdx)&1 == 1
}

func (bf *BloomFilter) FalsePositiveRate() float64 {
	if bf.count == 0 {
		return 0
	}
	exponent := -float64(bf.k) * float64(bf.count) / float64(bf.m)
	return math.Pow(1-math.Exp(exponent), float64(bf.k))
}

func (bf *BloomFilter) BitCount() int {
	count := 0
	for _, word := range bf.bitset {
		count += bits.OnesCount64(word)
	}
	return count
}

func (bf *BloomFilter) Serialize() []byte {
	buf := make([]byte, 16+len(bf.bitset)*8)
	binary.LittleEndian.PutUint64(buf[0:8], bf.m)
	binary.LittleEndian.PutUint64(buf[8:16], bf.k)
	for i, word := range bf.bitset {
		binary.LittleEndian.PutUint64(buf[16+i*8:], word)
	}
	return buf
}

func Deserialize(data []byte) (*BloomFilter, error) {
	if len(data) < 16 {
		return nil, errorf("bloom: data too short")
	}

	m := binary.LittleEndian.Uint64(data[0:8])
	k := binary.LittleEndian.Uint64(data[8:16])

	wordCount := m / 64
	bitset := make([]uint64, wordCount)

	for i := uint64(0); i < wordCount; i++ {
		offset := 16 + i*8
		if offset+8 > uint64(len(data)) {
			break
		}
		bitset[i] = binary.LittleEndian.Uint64(data[offset : offset+8])
	}

	return &BloomFilter{
		bitset: bitset,
		m:      m,
		k:      k,
	}, nil
}

func hash128(key string) (uint64, uint64) {
	const (
		fnvBasis64 = 14695981039346656037
		fnvPrime64 = 1099511628211
	)

	h1 := uint64(fnvBasis64)
	for i := 0; i < len(key); i++ {
		h1 ^= uint64(key[i])
		h1 *= fnvPrime64
	}

	h2 := h1 ^ (h1 >> 33)
	h2 *= 0xff51afd7ed558ccd
	h2 ^= h2 >> 33
	h2 *= 0xc4ceb9fe1a85ec53
	h2 ^= h2 >> 33

	return h1, h2
}

func errorf(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}
