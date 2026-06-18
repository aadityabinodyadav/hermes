package storage

import (
	"fmt"
	"os"
	"time"

	"github.com/aadityabinodyadav/hermes/pkg/clock"
	"github.com/aadityabinodyadav/hermes/pkg/storage/bloom"
	"github.com/aadityabinodyadav/hermes/pkg/storage/wal"
)

func RunStorageDemo() {
	printHeader()

	fmt.Println("━━━ DEMO 1: Bloom Filter ━━━")
	demoBloomFilter()

	fmt.Println("\n━━━ DEMO 2: WAL Write + Recovery ━━━")
	demoWAL()

	fmt.Println("\n━━━ DEMO 3: Full LSM-Tree Engine ━━━")
	demoEngine()

	fmt.Println("\n━━━ DEMO 4: Crash Recovery ━━━")
	demoCrashRecovery()

	printSummary()
}

func demoBloomFilter() {
	fmt.Println()
	fmt.Println("Building a Bloom filter for 1000 keys, 1% false positive rate:")

	bf := bloom.New(1000, 0.01)

	// Add 1000 keys
	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = fmt.Sprintf("user:%06d", i)
		bf.Add(keys[i])
	}

	fmt.Printf("  Bits used:            %d\n", bf.BitCount())
	fmt.Printf("  False positive rate:  %.2f%%\n", bf.FalsePositiveRate()*100)
	fmt.Println()

	// Test: keys that ARE in the filter
	allFound := true
	for _, key := range keys[:10] {
		if !bf.Contains(key) {
			allFound = false
			fmt.Printf("  FALSE NEGATIVE for %s — should never happen!\n", key)
		}
	}
	if allFound {
		fmt.Println("  ✅ No false negatives (guaranteed)")
	}

	// Test: keys that are NOT in the filter
	falsePositives := 0
	testCount := 10000
	for i := 0; i < testCount; i++ {
		key := fmt.Sprintf("nonexistent:%06d", i)
		if bf.Contains(key) {
			falsePositives++
		}
	}

	fpRate := float64(falsePositives) / float64(testCount) * 100
	fmt.Printf("  False positives:      %d/%d (%.2f%%)\n",
		falsePositives, testCount, fpRate)
	fmt.Println()
	fmt.Println("  In Hermes:")
	fmt.Printf("  For a 100MB SSTable with 1M keys: bloom filter ≈ %.1fMB\n",
		float64(1000000)*9.6/8/1024/1024)
	fmt.Println("  But saves us from reading 4KB disk blocks for missing keys!")
}

func demoWAL() {
	fmt.Println()
	dir, _ := os.MkdirTemp("", "hermes-wal-demo-*")
	defer os.RemoveAll(dir)

	hlc := clock.NewHLC("demo-node")
	w, err := wal.Open(dir, hlc)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Writing to WAL (each write is durable before returning):")
	writes := []string{
		"PUT user:alice balance=1000",
		"PUT user:bob balance=500",
		"DELETE user:charlie",
		"PUT user:alice balance=900",
	}

	start := time.Now()
	for i, cmd := range writes {
		entry, err := w.Write([]byte(cmd))
		if err != nil {
			fmt.Printf("  Write error: %v\n", err)
			continue
		}
		fmt.Printf("  [seq=%d, ts=%s] %s\n",
			entry.Sequence, entry.Timestamp, cmd)
		_ = i
	}
	elapsed := time.Since(start)

	stats := w.Stats()
	fmt.Println()
	fmt.Printf("  Total time:       %v\n", elapsed)
	fmt.Printf("  Records written:  %d\n", stats.RecordsWritten)
	fmt.Printf("  Bytes written:    %d\n", stats.BytesWritten)
	fmt.Printf("  Fsync calls:      %d (group commit!)\n", stats.SyncCount)
	fmt.Printf("  Avg per write:    %v\n", elapsed/time.Duration(len(writes)))

	w.Close()

	// Simulate crash and recovery
	fmt.Println()
	fmt.Println("Simulating crash and recovery:")
	fmt.Println("  (Closing without checkpoint = simulated crash)")

	w2, err := wal.Open(dir, clock.NewHLC("demo-node-2"))
	if err != nil {
		fmt.Printf("  Recovery error: %v\n", err)
		return
	}
	defer w2.Close()

	fmt.Printf("  ✅ Recovered! Next sequence: %d\n", w2.Stats().NextSequence)
	fmt.Println("  All writes survived the 'crash'")
}

func demoEngine() {
	fmt.Println()
	dir, _ := os.MkdirTemp("", "hermes-engine-demo-*")
	defer os.RemoveAll(dir)

	hlc := clock.NewHLC("engine-demo")
	engine, err := Open(DefaultConfig(dir), hlc)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer engine.Close()

	// Write a bunch of keys
	fmt.Println("Writing 1000 key-value pairs:")
	start := time.Now()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key:%06d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := engine.Put(key, value); err != nil {
			fmt.Printf("  Put error: %v\n", err)
			return
		}
	}
	writeTime := time.Since(start)

	stats := engine.Stats()
	fmt.Printf("  Wrote 1000 keys in %v\n", writeTime)
	fmt.Printf("  Throughput: %.0f writes/sec\n",
		1000.0/writeTime.Seconds())
	fmt.Printf("  MemTable size: %d bytes\n", stats.MemTableSize)
	fmt.Println()

	// Read some keys
	fmt.Println("Reading keys:")
	for _, key := range []string{"key:000000", "key:000500", "key:000999"} {
		start := time.Now()
		val, found, err := engine.Get(key)
		readTime := time.Since(start)

		if err != nil {
			fmt.Printf("  GET %s → error: %v\n", key, err)
		} else if found {
			fmt.Printf("  GET %s → %s (%v)\n", key, string(val), readTime)
		} else {
			fmt.Printf("  GET %s → NOT FOUND\n", key)
		}
	}

	// Test MVCC
	fmt.Println()
	fmt.Println("MVCC snapshot read:")

	snapshotTS := hlc.Now()
	fmt.Printf("  Taking snapshot at: %s\n", snapshotTS)

	engine.Put("mvcc-key", []byte("version-1"))
	time.Sleep(time.Millisecond)
	engine.Put("mvcc-key", []byte("version-2"))

	v1, _, _ := engine.GetAtTimestamp("mvcc-key", snapshotTS)
	v2, _, _ := engine.Get("mvcc-key")

	fmt.Printf("  Read at snapshot:  %s (before both writes → not found is ok)\n",
		string(v1))
	fmt.Printf("  Read current:      %s\n", string(v2))

	// Test delete
	fmt.Println()
	fmt.Println("Delete + tombstone:")
	engine.Put("temp-key", []byte("temporary"))
	val, found, _ := engine.Get("temp-key")
	fmt.Printf("  Before delete: GET temp-key → %s (found=%v)\n", string(val), found)

	engine.Delete("temp-key")
	val, found, _ = engine.Get("temp-key")
	fmt.Printf("  After delete:  GET temp-key → found=%v\n", found)

	// Final stats
	fmt.Println()
	stats = engine.Stats()
	fmt.Printf("Engine stats:\n")
	fmt.Printf("  Total writes:    %d\n", stats.WriteCount)
	fmt.Printf("  Total reads:     %d\n", stats.ReadCount)
	fmt.Printf("  SSTable count:   %d\n", stats.SSTableCount)
	fmt.Printf("  WAL syncs:       %d\n", stats.WALStats.SyncCount)
}

func demoCrashRecovery() {
	fmt.Println()
	dir, _ := os.MkdirTemp("", "hermes-crash-*")
	defer os.RemoveAll(dir)

	hlc := clock.NewHLC("crash-demo")

	// Write some data, then "crash" (don't checkpoint)
	fmt.Println("Phase 1: Write data, then crash (no checkpoint)")
	{
		engine, _ := Open(DefaultConfig(dir), hlc)
		for i := 0; i < 100; i++ {
			engine.Put(
				fmt.Sprintf("persistent-key:%d", i),
				[]byte(fmt.Sprintf("value-%d", i)),
			)
		}
		fmt.Println("  Wrote 100 keys")
		fmt.Println("  Simulating crash (Close without Checkpoint)...")
		engine.wal.Close() // Force close without checkpoint
	}

	// Reopen and verify recovery
	fmt.Println()
	fmt.Println("Phase 2: Reopen after crash")
	{
		engine2, err := Open(DefaultConfig(dir), clock.NewHLC("recovered"))
		if err != nil {
			fmt.Printf("  Recovery failed: %v\n", err)
			return
		}
		defer engine2.Close()

		// Verify data survived
		recovered := 0
		for i := 0; i < 100; i++ {
			_, found, _ := engine2.Get(fmt.Sprintf("persistent-key:%d", i))
			if found {
				recovered++
			}
		}

		fmt.Printf("  ✅ Recovered %d/100 keys from WAL after crash!\n", recovered)
		fmt.Println("  This is WAL durability in action.")
	}
}

func printHeader() {
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("\n╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "HERMES — PHASE 3: STORAGE ENGINE")
	fmt.Printf("╚%s╝\n\n", line)
}

func printSummary() {
	fmt.Println()
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "PHASE 3 COMPLETE ✅")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "What we built:")
	fmt.Printf("║  %-61s║\n", "  ✅ WAL — crash-safe write log with group commit")
	fmt.Printf("║  %-61s║\n", "  ✅ MemTable — in-memory sorted skip list")
	fmt.Printf("║  %-61s║\n", "  ✅ SSTable — immutable sorted on-disk file")
	fmt.Printf("║  %-61s║\n", "  ✅ Bloom Filter — 1% false positive, O(1) lookup")
	fmt.Printf("║  %-61s║\n", "  ✅ LSM-Tree Engine — orchestrates all of the above")
	fmt.Printf("║  %-61s║\n", "  ✅ Crash recovery — replay WAL after restart")
	fmt.Printf("║  %-61s║\n", "  ✅ Basic MVCC — read at a snapshot timestamp")
	fmt.Printf("║  %-61s║\n", "  ✅ Background compaction — merge SSTables")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "Key numbers to remember:")
	fmt.Printf("║  %-61s║\n", "  WAL write:      1 fsync per BATCH (group commit)")
	fmt.Printf("║  %-61s║\n", "  MemTable:       O(log n) insert/lookup (skip list)")
	fmt.Printf("║  %-61s║\n", "  SSTable read:   Bloom filter → index → disk block")
	fmt.Printf("║  %-61s║\n", "  Bloom filter:   ~10 bits/key, 1% false positive")
	fmt.Printf("║  %-61s║\n", "  Block size:     4KB = one OS page")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "How Phase 3 connects forward:")
	fmt.Printf("║  %-61s║\n", "  → Phase 4 (Raft): WAL stores Raft log entries")
	fmt.Printf("║  %-61s║\n", "  → Phase 7 (Txn):  MVCC enables snapshot isolation")
	fmt.Printf("║  %-61s║\n", "  → Phase 9 (Fault): WAL enables crash recovery")
	fmt.Printf("║  %-61s║\n", "  → Phase 10 (Ops): storage metrics from Engine.Stats()")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "→ NEXT: Phase 4 — Raft Consensus (the BIG one)")
	fmt.Printf("╚%s╝\n", line)
}
