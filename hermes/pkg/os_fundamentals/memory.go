//go:build !windows
// +build !windows

package os_fundamentals

import (
	"fmt"
	"os"
	"runtime"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)


func MemoryModel() {
	fmt.Println("=== VIRTUAL MEMORY & MEMORY MANAGEMENT ===")
	fmt.Println()

	demonstrateMemoryLayout()
	demonstrateMmap()
	demonstrateGCPressure()
}

func demonstrateMemoryLayout() {
	fmt.Println("--- Memory Layout of a Hermes Process ---")
	fmt.Println()
	fmt.Println("  HIGH ADDRESS")
	fmt.Println("  ┌─────────────────┐")
	fmt.Println("  │   Kernel Space  │  ← OS code, not accessible to us")
	fmt.Println("  │   (1GB on 32-bit│")
	fmt.Println("  │   128TB on 64-bit│")
	fmt.Println("  ├─────────────────┤")
	fmt.Println("  │   Stack         │  ← Local vars, function frames")
	fmt.Println("  │   (grows down)  │    Goroutine stacks are here")
	fmt.Println("  │       ↓         │")
	fmt.Println("  │   ...           │")
	fmt.Println("  │       ↑         │")
	fmt.Println("  │   Heap          │  ← Dynamic allocation (new, make)")
	fmt.Println("  │   (grows up)    │    Most of Hermes's data is here")
	fmt.Println("  ├─────────────────┤")
	fmt.Println("  │   mmap region   │  ← Memory-mapped files (SSTables!)")
	fmt.Println("  ├─────────────────┤")
	fmt.Println("  │   BSS segment   │  ← Uninitialized global vars")
	fmt.Println("  ├─────────────────┤")
	fmt.Println("  │   Data segment  │  ← Initialized global vars")
	fmt.Println("  ├─────────────────┤")
	fmt.Println("  │   Text segment  │  ← Executable code (read-only)")
	fmt.Println("  └─────────────────┘")
	fmt.Println("  LOW ADDRESS (0x0 = NULL)")
	fmt.Println()

	// Show actual addresses
	var stackVar int = 42
	heapVar := new(int)
	*heapVar = 42

	fmt.Printf("Stack variable address:  %p\n", &stackVar)
	fmt.Printf("Heap variable address:   %p\n", heapVar)
	fmt.Printf("Code address (approx):   %p\n", demonstrateMemoryLayout)
	fmt.Println()

	// This is why Go's escape analysis matters!
	// If a variable "escapes" to heap (because we return its address),
	// Go allocates it there, creating GC pressure
	fmt.Println("Go Escape Analysis: Variable on stack vs heap")
	fmt.Println("  go build -gcflags='-m' ./... shows escape analysis decisions")
	fmt.Println("  Stack allocation: FREE (just move stack pointer)")
	fmt.Println("  Heap allocation:  COSTS (GC must track and collect it)")
	fmt.Println()
	fmt.Println("In Hermes, we'll minimize heap allocations in hot paths:")
	fmt.Println("  - Raft message handling: use sync.Pool for message objects")
	fmt.Println("  - WAL writes: pre-allocated buffers")
	fmt.Println("  - Network encoding: reuse byte slices")

	_ = unsafe.Pointer(heapVar) // suppress unused warning
}

func demonstrateMmap() {
	fmt.Println("--- Memory-Mapped Files (mmap) ---")
	fmt.Println()
	fmt.Println("mmap maps a file directly into virtual address space")
	fmt.Println("Reading a 'byte slice' actually reads from disk (via page cache)")
	fmt.Println()

	// Create a temp file to mmap
	tmpFile, err := os.CreateTemp("", "hermes-mmap-*")
	if err != nil {
		fmt.Printf("Error creating temp file: %v\n", err)
		return
	}
	defer os.Remove(tmpFile.Name())

	// Write some data
	data := make([]byte, 4096) // One page = 4096 bytes
	copy(data, []byte("Hermes SSTable Block Data"))
	for i := 24; i < 4096; i++ {
		data[i] = byte(i % 256)
	}
	tmpFile.Write(data)
	tmpFile.Sync()

	// Memory map the file
	fd := int(tmpFile.Fd())
	mmapData, err := unix.Mmap(
		fd,
		0,    // offset
		4096, // length
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)
	if err != nil {
		fmt.Printf("mmap failed: %v\n", err)
		tmpFile.Close()
		return
	}
	// Use unix.Munmap when available; fallback to syscall package for older
	// Go versions. This avoids undefined: syscall.Munmap on some platforms.
	// We'll attempt to unmap using unix.Munmap if present.
	unmap := func(b []byte) error { return nil }
	// Try to use unix.Munmap (most systems)
	unmap = func(b []byte) error { return unix.Munmap(b) }
	// Defer unmap but ignore error in this demo
	defer func() { _ = unmap(mmapData) }()
	tmpFile.Close()

	fmt.Printf("File size:        4096 bytes\n")
	fmt.Printf("mmap address:     %p\n", &mmapData[0])
	fmt.Printf("First 25 bytes:   %s\n", string(mmapData[:25]))
	fmt.Println()
	fmt.Println("Hermes uses mmap for:")
	fmt.Println("  - SSTable reading: OS manages caching (page cache)")
	fmt.Println("  - Fast random access without explicit read() calls")
	fmt.Println("  - But careful: mmap + disk errors = SIGBUS signal!")
	fmt.Println()
	fmt.Println("Hermes does NOT use mmap for:")
	fmt.Println("  - WAL writes: we need explicit fsync control")
	fmt.Println("  - When we need durability guarantees")
}

func demonstrateGCPressure() {
	fmt.Println("--- Garbage Collection & Distributed Systems ---")
	fmt.Println()
	fmt.Println("GC pauses are the ENEMY of distributed systems!")
	fmt.Println("A GC pause of 50ms can look like a node failure (if timeout < 50ms)")
	fmt.Println()

	// Show GC stats before
	var beforeStats, afterStats runtime.MemStats
	runtime.ReadMemStats(&beforeStats)

	// Create GC pressure by allocating many small objects
	// This simulates what happens if Hermes is sloppy with allocations
	var trash [][]byte
	start := time.Now()

	for i := 0; i < 100000; i++ {
		b := make([]byte, 1024) // 1KB each
		b[0] = byte(i)
		trash = append(trash, b)
	}

	// Force GC
	beforeGC := time.Now()
	trash = nil  // make garbage
	runtime.GC() // force collection
	gcTime := time.Since(beforeGC)

	runtime.ReadMemStats(&afterStats)

	allocationTime := beforeGC.Sub(start)

	fmt.Printf("Allocated 100MB of 1KB objects: %v\n", allocationTime)
	fmt.Printf("GC collection time:             %v\n", gcTime)
	fmt.Printf("GC runs:                        %d\n", afterStats.NumGC-beforeStats.NumGC)
	fmt.Println()
	fmt.Println("Hermes GC mitigation strategies:")
	fmt.Println("  1. sync.Pool for frequently allocated objects (Raft messages)")
	fmt.Println("  2. GOGC=200 (GC less frequently, accept higher memory usage)")
	fmt.Println("  3. Pre-allocated byte slices for network I/O buffers")
	fmt.Println("  4. Avoid small, short-lived heap allocations in hot paths")
	fmt.Println("  5. Use GOMEMLIMIT (Go 1.19+) to control GC trigger by memory limit")
	fmt.Printf("\n  Example: GOGC=200 GOMEMLIMIT=4GiB ./hermes-server\n")
}
