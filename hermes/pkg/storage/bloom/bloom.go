package bloom

/*
 Bloom Filter: "Is this key DEFINITELY NOT in this SSTable?"

 A Bloom filter is a space-efficient probabilistic data structure.
 It can tell you:
   - "DEFINITELY NOT in set" (no false negatives)
   - "PROBABLY in set" (false positives possible)

 For Hermes:
   - Before reading an SSTable from disk: ask Bloom filter
   - If filter says "NOT IN": skip SSTable entirely (save disk I/O!)
   - If filter says "MAYBE IN": read SSTable and check

 How it works:
   - An array of M bits (all start as 0)
   - K hash functions
   - Add(key): set bits at h1(key), h2(key), ..., hk(key)
   - Contains(key): check bits at all hash positions
     if ANY bit is 0 → DEFINITELY NOT in set
     if ALL bits are 1 → PROBABLY in set (maybe false positive)

   ┌─────────────────────────────────────────┐
   │  Bit array: [1,0,1,1,0,1,0,0,1,0,1,0]  │
   │                                          │
   │  Add("alice"): h1=2, h2=5, h3=8        │
   │  Set bits 2,5,8 → [1,0,1,1,0,1,0,0,1,..]│
   │                                          │
   │  Add("bob"):   h1=0, h2=3, h3=10       │
   │  Set bits 0,3,10 → [1,0,1,1,0,1,0,0,1,0,1,0]│
   │                                          │
   │  Contains("charlie"): h1=4, h2=7, h3=2  │
   │  Bit 4 = 0 → DEFINITELY NOT IN SET ✅   │
   │  No disk read needed!                    │
   └─────────────────────────────────────────┘

 False positive rate: p = (1 - e^(-kn/m))^k
 where n=items, m=bits, k=hash functions

 Optimal k = (m/n) * ln(2)

 For p=1% (1 false positive per 100 checks):
   m/n ≈ 9.6 bits per item
   k ≈ 6.7 hash functions (round to 7)

 RocksDB default: ~10 bits per key, ~6 hash functions
*/
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
