package os_fundamentals

import "fmt"

func RunAll() {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║         HERMES - PHASE 0: OS FUNDAMENTALS                 ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	ProcessInfo()
	fmt.Println("\n" + divider())

	GoroutineVsOSThread(10000)
	fmt.Println("\n" + divider())

	ContextSwitchDemo()
	fmt.Println("\n" + divider())

	StackGrowthDemo()
	fmt.Println("\n" + divider())

	SystemCallsDemo()
	fmt.Println("\n" + divider())

	memoryModelIfAvailable()
	fmt.Println("\n" + divider())

	ContextCancellationDemo()
	fmt.Println("\n" + divider())

	FanOutFanIn()
	fmt.Println("\n" + divider())

	SelectPriorityDemo()
	fmt.Println("\n" + divider())

	AtomicMetricsDemo()
	fmt.Println("\n" + divider())

	SyncPoolDemo()
	fmt.Println("\n" + divider())

	RawTCPDemo()
	TCPProperties()
	NetworkConceptsDemo()
	fmt.Println("\n" + divider())

	StorageIOFundamentals()
	fmt.Println("\n" + divider())

	printSummary()

}

func divider() string {
	return "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

func printHeader(title string) {
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("\n╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", title)
	fmt.Printf("╚%s╝\n\n", line)
}

func printSection(name string) {
	fmt.Printf("\n--- %s ---\n", name)
}

func printSummary() {
	fmt.Println()
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "PHASE 0 COMPLETE ✅")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "Concepts mastered:")
	fmt.Printf("║  %-61s║\n", "  ✅ Process/Thread/Goroutine - the node IS a process")
	fmt.Printf("║  %-61s║\n", "  ✅ Go M:N scheduler - how we handle 100K connections")
	fmt.Printf("║  %-61s║\n", "  ✅ Syscalls: fsync (durability), socket (network)")
	fmt.Printf("║  %-61s║\n", "  ✅ Virtual memory, mmap, page cache")
	fmt.Printf("║  %-61s║\n", "  ✅ File descriptors and OS limits")
	fmt.Printf("║  %-61s║\n", "  ✅ GC pressure mitigation (sync.Pool, atomics)")
	fmt.Printf("║  %-61s║\n", "  ✅ TCP properties (ordering, keepalive, framing)")
	fmt.Printf("║  %-61s║\n", "  ✅ Network partitions (the key distributed systems problem)")
	fmt.Printf("║  %-61s║\n", "  ✅ Storage hierarchy (RAM vs SSD vs HDD)")
	fmt.Printf("║  %-61s║\n", "  ✅ fsync cost and group commit")
	fmt.Printf("║  %-61s║\n", "  ✅ Sequential vs random I/O (why LSM-Tree)")
	fmt.Printf("║  %-61s║\n", "  ✅ OS page cache behavior")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "How this connects to Hermes:")
	fmt.Printf("║  %-61s║\n", "  → Each Hermes node = a Go process")
	fmt.Printf("║  %-61s║\n", "  → Goroutines handle concurrent connections (cheap!)")
	fmt.Printf("║  %-61s║\n", "  → WAL must fsync before ACK (durability)")
	fmt.Printf("║  %-61s║\n", "  → Network partitions are why we need Raft")
	fmt.Printf("║  %-61s║\n", "  → Group commit makes WAL fast")
	fmt.Printf("║  %-61s║\n", "  → Page cache makes SSTable reads fast")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "→ NEXT: Phase 1 - gRPC, Protobuf, Network Transport")
	fmt.Printf("╚%s╝\n", line)
}
