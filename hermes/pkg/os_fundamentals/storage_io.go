package os_fundamentals

import (
	"fmt"
	"os"
	"time"
)



func StorageIOFundamentals() {
	fmt.Println("=== STORAGE & I/O FUNDAMENTALS ===")
	demonstrateIOHierarchy()
	demonstrateWritePatterns()
	demonstrateFsyncCost()
	demonstratePageCache()
}



func tempFile(pattern string) (*os.File, func()) {
	f, _ := os.CreateTemp("", pattern)
	cleanup := func() {
		f.Close()
		os.Remove(f.Name())
	}
	return f, cleanup
}

func throughputMBs(bytes int, d time.Duration) float64 {
	return float64(bytes) / d.Seconds() / 1024 / 1024
}

func throughputGBs(bytes int, d time.Duration) float64 {
	return throughputMBs(bytes, d) / 1024
}



func demonstrateIOHierarchy() {
	fmt.Println("--- RAM vs Disk (measured on this machine) ---")

	measureRAM()
	measureDiskSequential()
	measureDiskWithFsync()
}

func measureRAM() {
	const size = 100 * 1024 * 1024 
	data := make([]byte, size)

	start := time.Now()
	for i := range data {
		data[i] = byte(i)
	}
	writeTime := time.Since(start)

	start = time.Now()
	var sink byte
	for _, b := range data {
		sink += b
	}
	readTime := time.Since(start)
	_ = sink

	fmt.Printf("RAM write (100MB): %v (%.1f GB/s)\n", writeTime, throughputGBs(size, writeTime))
	fmt.Printf("RAM read  (100MB): %v (%.1f GB/s)\n\n", readTime, throughputGBs(size, readTime))
}

func measureDiskSequential() {
	f, cleanup := tempFile("hermes-io-seq-*")
	defer cleanup()

	const (
		blockSize = 4096
		ops       = 1000 
	)
	block := make([]byte, blockSize)

	start := time.Now()
	for i := 0; i < ops; i++ {
		f.Write(block)
	}
	d := time.Since(start)

	fmt.Printf("Disk sequential (4MB, buffered): %v (%.1f MB/s)\n", d, throughputMBs(ops*blockSize, d))
}

func measureDiskWithFsync() {
	f, cleanup := tempFile("hermes-io-fsync-*")
	defer cleanup()

	const ops = 20
	block := make([]byte, 4096)

	start := time.Now()
	for i := 0; i < ops; i++ {
		f.Write(block)
		f.Sync()
	}
	d := time.Since(start)

	fmt.Printf("Disk + fsync    (%d ops):         %v (avg %v/fsync)\n\n", ops, d, d/ops)
	fmt.Println("HERMES IMPLICATION:")
	fmt.Printf("  WAL write + fsync ≈ %v per entry (minimum linearizable write latency)\n", d/ops)
	fmt.Println("  Network round-trip to replicas adds on top of this.")
	fmt.Println("  OPTIMIZATION: group commit — one fsync covers many WAL entries.")
}



func demonstrateWritePatterns() {
	fmt.Println("--- Sequential vs Random Writes ---")
	fmt.Println("LSM-Tree converts random writes → sequential. That is its entire point.")

	const (
		blockSize = 4096
		ops       = 500
	)
	block := make([]byte, blockSize)

	
	seqFile, seqCleanup := tempFile("hermes-seq-*")
	defer seqCleanup()

	start := time.Now()
	for i := 0; i < ops; i++ {
		seqFile.Write(block)
	}
	seqTime := time.Since(start)

	
	randFile, randCleanup := tempFile("hermes-rand-*")
	defer randCleanup()

	randFile.Write(make([]byte, ops*blockSize)) 
	randFile.Sync()

	start = time.Now()
	for i := 0; i < ops; i++ {
		offset := int64((i * 17 % ops) * blockSize) 
		randFile.WriteAt(block, offset)
	}
	randTime := time.Since(start)

	fmt.Printf("Sequential (%d × 4KB): %v (%.1f MB/s)\n", ops, seqTime, throughputMBs(ops*blockSize, seqTime))
	fmt.Printf("Random     (%d × 4KB): %v (%.1f MB/s)\n\n", ops, randTime, throughputMBs(ops*blockSize, randTime))

	fmt.Println("LSM-Tree write path:")
	fmt.Println("  1. Write → MemTable (RAM, instant)")
	fmt.Println("  2. MemTable full → flush to SSTable sequentially (fast)")
	fmt.Println("  3. No random disk seeks during writes")
	fmt.Println("  Tradeoff: reads check multiple SSTables (read amplification)")
}



func demonstrateFsyncCost() {
	fmt.Println("--- fsync() and Group Commit ---")
	fmt.Println("fsync() flushes OS buffers → disk controller → physical media.")
	fmt.Println("Without it, a crash can lose 'written' data.")

	f, cleanup := tempFile("hermes-fsync-*")
	defer cleanup()

	data := make([]byte, 1024)

	
	f.Write(data)
	start := time.Now()
	f.Sync()
	singleFsync := time.Since(start)

	fmt.Printf("Single fsync:          %v\n", singleFsync)
	fmt.Printf("Max throughput:        %.0f writes/sec (one fsync per write)\n\n", 1.0/singleFsync.Seconds())

	
	const groupSize = 100
	start = time.Now()
	for i := 0; i < groupSize; i++ {
		f.Write(data)
	}
	f.Sync()
	groupTime := time.Since(start)

	fmt.Printf("Group commit (%d writes, 1 fsync): %v\n", groupSize, groupTime)
	fmt.Printf("Effective throughput:  %.0f writes/sec\n\n", float64(groupSize)/groupTime.Seconds())

	fmt.Println("Hermes WAL group commit strategy:")
	fmt.Println("  Accumulate entries until N entries OR T milliseconds, then one fsync.")
	fmt.Println("  Result: 10-100x better throughput vs per-write fsync.")
}



func demonstratePageCache() {
	fmt.Println("--- OS Page Cache ---")
	fmt.Println("The OS caches disk pages in RAM automatically.")
	fmt.Println("First read hits disk; subsequent reads hit RAM.")

	const size = 10 * 1024 * 1024 

	
	f, cleanup := tempFile("hermes-pagecache-*")
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	f.Write(payload)
	f.Sync()
	name := f.Name()
	cleanup()

	readFile := func() time.Duration {
		start := time.Now()
		f, _ := os.Open(name)
		buf := make([]byte, size)
		f.Read(buf)
		f.Close()
		return time.Since(start)
	}
	defer os.Remove(name)

	coldRead := readFile() 
	warmRead := readFile() 

	fmt.Printf("Cold read (10MB, first):  %v\n", coldRead)
	fmt.Printf("Warm read (10MB, cached): %v\n", warmRead)
	if warmRead > 0 {
		fmt.Printf("Page cache speedup:       %.1fx\n\n", float64(coldRead)/float64(warmRead))
	}

	fmt.Println("Hermes SSTable read path:")
	fmt.Println("  Hot SSTables  → always in page cache → fast")
	fmt.Println("  Cold SSTables → cache miss on first read → slow")
	fmt.Println("  Block cache   → Hermes-managed LRU on top of page cache")
	fmt.Println("  Bloom filter  → skips disk entirely when key doesn't exist")

	fmt.Println("Gray failure risk:")
	fmt.Println("  Memory pressure evicts cached pages → next read is slow.")
	fmt.Println("  Node is 'up' but P99 latency spikes.")
	fmt.Println("  Hermes monitors P99 read latency, not just error rate.")
}
