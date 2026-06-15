package clock

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

const (
	MaxClockSkew = 500 * time.Millisecond

	LogicalBits = 16

	LogicalMax = (1 << LogicalBits) - 1 // 65535
)

type HLCTimestamp int64

func Pack(physicalMs int64, logical uint16) HLCTimestamp {
	return HLCTimestamp((physicalMs << LogicalBits) | int64(logical))
}

func (ts HLCTimestamp) Unpack() (physicalMs int64, logical uint16) {
	physicalMs = int64(ts) >> LogicalBits
	logical = uint16(int64(ts) & LogicalMax)
	return
}

func (ts HLCTimestamp) Physical() time.Time {
	physMs, _ := ts.Unpack()
	return time.UnixMilli(physMs)
}

func (ts HLCTimestamp) Logical() uint16 {
	_, l := ts.Unpack()
	return l
}

func (ts HLCTimestamp) Before(other HLCTimestamp) bool {
	return ts < other
}

func (ts HLCTimestamp) After(other HLCTimestamp) bool {
	return ts > other
}

func (ts HLCTimestamp) Equal(other HLCTimestamp) bool {
	return ts == other
}

func (ts HLCTimestamp) String() string {
	physMs, logical := ts.Unpack()
	t := time.UnixMilli(physMs)
	return fmt.Sprintf("HLC{%s, logical=%d}", t.Format("15:04:05.000"), logical)
}

func (ts HLCTimestamp) ToBytes() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(ts))
	return b
}

func HLCTimestampFromBytes(b []byte) HLCTimestamp {
	return HLCTimestamp(binary.BigEndian.Uint64(b))
}

type HLC struct {
	mu sync.Mutex

	nodeID string

	l int64

	c uint16

	wallClock func() time.Time

	maxSkewAllowed time.Duration
}

func NewHLC(nodeID string) *HLC {
	return &HLC{
		nodeID:         nodeID,
		l:              0,
		c:              0,
		wallClock:      time.Now, // real wall clock by default
		maxSkewAllowed: MaxClockSkew,
	}
}

func NewHLCWithClock(nodeID string, wallClock func() time.Time) *HLC {
	return &HLC{
		nodeID:         nodeID,
		l:              0,
		c:              0,
		wallClock:      wallClock,
		maxSkewAllowed: MaxClockSkew,
	}
}

func (h *HLC) Now() HLCTimestamp {
	h.mu.Lock()
	defer h.mu.Unlock()

	ptMs := h.wallClock().UnixMilli()

	lNew := ptMs
	if h.l > lNew {
		lNew = h.l
	}

	if lNew == h.l {
		h.c++

		if h.c > LogicalMax {
			lNew++
			h.c = 0
		}
	} else {
		h.l = lNew
		h.c = 0
	}

	h.l = lNew
	return Pack(h.l, h.c)
}

func (h *HLC) Update(msgTS HLCTimestamp) (HLCTimestamp, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	msgL, msgC := msgTS.Unpack()
	ptMs := h.wallClock().UnixMilli()

	maxFutureMs := ptMs + h.maxSkewAllowed.Milliseconds()
	if msgL > maxFutureMs {
		return 0, fmt.Errorf(
			"HLC: received timestamp too far in future: msg_l=%d, our_pt=%d, max_allowed=%d (skew=%.0fms)",
			msgL, ptMs, maxFutureMs, float64(msgL-ptMs),
		)
	}

	lNew := ptMs
	if h.l > lNew {
		lNew = h.l
	}
	if msgL > lNew {
		lNew = msgL
	}

	var cNew uint16
	switch {
	case lNew == h.l && lNew == msgL:
		cNew = h.c
		if msgC > cNew {
			cNew = msgC
		}
		cNew++

	case lNew == h.l:
		cNew = h.c + 1

	case lNew == msgL:
		cNew = msgC + 1

	default:
		cNew = 0
	}

	h.l = lNew
	h.c = cNew

	return Pack(h.l, h.c), nil
}

func (h *HLC) IsWithinSkewBound(ts HLCTimestamp) bool {
	now := h.wallClock().UnixMilli()
	physMs, _ := ts.Unpack()
	skew := physMs - now
	if skew < 0 {
		skew = -skew
	}
	return skew <= h.maxSkewAllowed.Milliseconds()
}

func (h *HLC) PhysicalNow() time.Time {
	return h.wallClock()
}

func (h *HLC) ClockSkew() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()

	ptMs := h.wallClock().UnixMilli()
	skewMs := h.l - ptMs
	return time.Duration(skewMs) * time.Millisecond
}
