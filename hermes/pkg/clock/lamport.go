package clock

import (
	"fmt"
	"sync/atomic"
)

type LamportClock struct {
	counter int64

	nodeID string
}

func NewLamportClock(nodeID string) *LamportClock {
	return &LamportClock{
		counter: 0,
		nodeID:  nodeID,
	}
}

func (c *LamportClock) Tick() int64 {

	return atomic.AddInt64(&c.counter, 1)
}

func (c *LamportClock) Send() int64 {
	return atomic.AddInt64(&c.counter, 1)
}

func (c *LamportClock) Receive(received int64) int64 {
	for {
		current := atomic.LoadInt64(&c.counter)

		newVal := received + 1
		if current >= received {
			newVal = current + 1
		}

		if atomic.CompareAndSwapInt64(&c.counter, current, newVal) {
			return newVal
		}
	}
}

func (c *LamportClock) Now() int64 {
	return atomic.LoadInt64(&c.counter)
}

func HappenedBefore(a, b int64) bool {
	return a < b
}

func (c *LamportClock) String() string {
	return fmt.Sprintf("LamportClock{node=%s, t=%d}", c.nodeID, c.Now())
}

type LamportMessage struct {
	From      string
	To        string
	Content   string
	Timestamp int64
}
