package clock

import (
	"testing"
	"time"
)

func TestLamportClock_Ordering(t *testing.T) {
	node1 := NewLamportClock("node-1")
	node2 := NewLamportClock("node-2")

	t1 := node1.Tick()

	sendTS := node1.Send()
	recvTS := node2.Receive(sendTS)

	if sendTS >= recvTS {
		t.Errorf("Send timestamp (%d) should be < receive timestamp (%d)",
			sendTS, recvTS)
	}
	_ = t1
}

func TestLamportClock_ConcurrentSafe(t *testing.T) {
	clock := NewLamportClock("test")

	done := make(chan int64, 1000)

	for i := 0; i < 1000; i++ {
		go func() {
			done <- clock.Tick()
		}()
	}

	seen := make(map[int64]bool)
	for i := 0; i < 1000; i++ {
		ts := <-done
		if seen[ts] {
			t.Errorf("Duplicate timestamp: %d", ts)
		}
		seen[ts] = true
	}
}

func TestVectorClock_Concurrency(t *testing.T) {
	nodes := []string{"node-1", "node-2", "node-3"}

	node1 := NewVectorClock("node-1", nodes)
	node2 := NewVectorClock("node-2", nodes)

	vc1 := node1.Tick()
	vc2 := node2.Tick()

	rel := Compare(vc1, vc2)
	if rel != Concurrent {
		t.Errorf("Independent events should be Concurrent, got %s", rel)
	}
}

func TestVectorClock_CausalOrdering(t *testing.T) {
	nodes := []string{"node-1", "node-2"}

	node1 := NewVectorClock("node-1", nodes)
	node2 := NewVectorClock("node-2", nodes)

	vc1 := node1.Send()
	vc2 := node2.Receive(vc1)

	rel := Compare(vc1, vc2)
	if rel != Before {
		t.Errorf("vc1 should be Before vc2, got %s", rel)
	}
}

func TestHLC_Monotonic(t *testing.T) {
	callCount := 0
	baseTime := time.Now()

	hlc := NewHLCWithClock("test", func() time.Time {
		callCount++
		if callCount%2 == 0 {
			return baseTime.Add(-100 * time.Millisecond)
		}
		return baseTime
	})

	prev := hlc.Now()
	for i := 0; i < 100; i++ {
		curr := hlc.Now()
		if curr.Before(prev) {
			t.Errorf("HLC went backwards: %s < %s", curr, prev)
		}
		prev = curr
	}
}

func TestHLC_CausalOrdering(t *testing.T) {
	sender := NewHLC("sender")
	receiver := NewHLC("receiver")

	sendTS := sender.Now()
	recvTS, err := receiver.Update(sendTS)

	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if !sendTS.Before(recvTS) {
		t.Errorf("Send (%s) should be before receive (%s)", sendTS, recvTS)
	}
}
func TestHLC_RejectsSkewedTimestamps(t *testing.T) {
	hlc := NewHLC("test")

	futureTS := Pack(
		time.Now().Add(2*time.Second).UnixMilli(),
		0,
	)

	_, err := hlc.Update(futureTS)
	if err == nil {
		t.Error("Should have rejected timestamp too far in the future")
	}
}

func TestHLC_PackUnpack(t *testing.T) {
	physMs := time.Now().UnixMilli()
	logical := uint16(42)

	ts := Pack(physMs, logical)
	gotPhys, gotLogical := ts.Unpack()

	if gotPhys != physMs {
		t.Errorf("Physical time mismatch: got %d, want %d", gotPhys, physMs)
	}
	if gotLogical != logical {
		t.Errorf("Logical counter mismatch: got %d, want %d", gotLogical, logical)
	}
}

func BenchmarkHLC_Now(b *testing.B) {
	hlc := NewHLC("bench")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = hlc.Now()
		}
	})
}

func BenchmarkHLC_Update(b *testing.B) {
	sender := NewHLC("sender")
	receiver := NewHLC("receiver")

	sendTS := sender.Now()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = receiver.Update(sendTS)
	}
}
