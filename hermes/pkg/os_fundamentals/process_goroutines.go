package os_fundamentals

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)



func ProcessInfo() {
	fmt.Println("=== PROCESS INFORMATION ===")
	fmt.Printf("Process ID (PID):         %d\n", os.Getpid())
	fmt.Printf("Parent Process ID (PPID): %d\n", os.Getppid())

	// In distributed systems, we often need to know our own identity
	// PID + hostname uniquely identifies a process in a cluster
	hostname, _ := os.Hostname()
	fmt.Printf("Hostname:                 %s\n", hostname)
	fmt.Printf("Node Identity:            %s:%d\n", hostname, os.Getpid())

	fmt.Println()
	fmt.Println("=== GO RUNTIME / SCHEDULER INFO ===")
	// GOMAXPROCS = number of OS threads that can run Go code simultaneously
	// = number of P's (Processors) in the scheduler
	// Default = number of CPU cores
	fmt.Printf("GOMAXPROCS (logical CPUs used):  %d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("Number of CPU cores available:   %d\n", runtime.NumCPU())
	fmt.Printf("Current goroutine count:          %d\n", runtime.NumGoroutine())

	// Memory stats - critical for distributed system health
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	fmt.Printf("Heap allocated:                  %d MB\n", memStats.HeapAlloc/1024/1024)
	fmt.Printf("Heap system:                     %d MB\n", memStats.HeapSys/1024/1024)
	fmt.Printf("Next GC at:                      %d MB\n", memStats.NextGC/1024/1024)
	fmt.Printf("GC runs so far:                  %d\n", memStats.NumGC)
}



func GoroutineVsOSThread(count int) {
	fmt.Printf("\n=== GOROUTINE CREATION: %d goroutines ===\n", count)

	start := time.Now()

	var wg sync.WaitGroup
	var goroutinesRunning int64

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			atomic.AddInt64(&goroutinesRunning, 1)
			// Simulate a goroutine doing something (like waiting for network I/O)
			time.Sleep(100 * time.Millisecond)
			atomic.AddInt64(&goroutinesRunning, -1)
		}(i)
	}

	// Show peak goroutine count
	time.Sleep(10 * time.Millisecond)
	fmt.Printf("Peak goroutines running:  %d\n", atomic.LoadInt64(&goroutinesRunning))
	fmt.Printf("Go scheduler goroutines:  %d\n", runtime.NumGoroutine())

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("Time to create+run %d goroutines: %v\n", count, elapsed)
	fmt.Printf("Average per goroutine:            %v\n", elapsed/time.Duration(count))

	
	fmt.Printf("\nKEY INSIGHT: We can handle %d concurrent connections\n", count)
	fmt.Printf("using only %d actual OS threads (GOMAXPROCS)\n", runtime.GOMAXPROCS(0))
}


func ContextSwitchDemo() {
	fmt.Println("\n=== CONTEXT SWITCH & SCHEDULING DEMO ===")
	fmt.Println("Showing goroutine interleaving on CPU cores...")

	var wg sync.WaitGroup
	results := make([]string, 0, 20)
	var mu sync.Mutex

	// Simulate three "subsystems" of Hermes running concurrently
	subsystems := []string{"Raft-Timer", "Client-Handler", "Compaction"}

	for _, name := range subsystems {
		wg.Add(1)
		go func(subsystem string) {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				// This is a preemption point - scheduler can switch here
				runtime.Gosched() // explicitly yield to scheduler

				mu.Lock()
				results = append(results, fmt.Sprintf("%s (iteration %d)", subsystem, i+1))
				mu.Unlock()

				time.Sleep(time.Millisecond)
			}
		}(name)
	}

	wg.Wait()

	fmt.Println("Execution order (shows interleaving):")
	for i, r := range results {
		fmt.Printf("  %2d: %s\n", i+1, r)
	}

	fmt.Println()
	fmt.Println("DISTRIBUTED SYSTEM IMPLICATION:")
	fmt.Println("  Raft election timeout = 150-300ms")
	fmt.Println("  If Raft-Timer goroutine is starved, we get spurious elections!")
	fmt.Println("  Solution: Raft ticker gets its own goroutine, never blocks on user work")
}

// StackGrowthDemo shows Go's dynamic stack - why goroutines start small
// Traditional OS threads have FIXED stack (usually 8MB)
// This means: 1000 OS threads = 8GB just for stacks!
// Go goroutines start at 2KB and grow as needed
func StackGrowthDemo() {
	fmt.Println("\n=== GOROUTINE STACK GROWTH ===")
	fmt.Printf("Initial goroutine stack: ~2KB (vs ~8MB for OS thread)\n")
	fmt.Printf("Stack grows dynamically as needed\n")
	fmt.Printf("Maximum goroutine stack: controlled by runtime.SetMaxStack\n")

	var measureStack func(depth int) int
	measureStack = func(depth int) int {
		if depth == 0 {
			// Capture current stack usage
			var buf [1 << 20]byte // 1MB buffer for stack trace
			n := runtime.Stack(buf[:], false)
			_ = n
			return depth
		}
		// Each recursive call grows the stack
		_ = make([]byte, 1024) // 1KB local variable forces stack allocation
		return measureStack(depth - 1)
	}

	start := time.Now()
	measureStack(100) // 100 levels of 1KB = ~100KB of stack
	fmt.Printf("Recursive 100-deep stack: %v\n", time.Since(start))
	fmt.Println("Stack grew automatically, no pre-allocation needed!")
}
