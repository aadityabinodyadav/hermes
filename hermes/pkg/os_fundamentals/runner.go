package os_fundamentals

import "fmt"

func RunAll() {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║         HERMES - PHASE 0.1: OS FUNDAMENTALS                 ║")
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

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║  Phase 0.1 Complete. Concepts learned:                       ║")
	fmt.Println("║  ✅ Processes vs Threads vs Goroutines                       ║")
	fmt.Println("║  ✅ Go scheduler (M:N threading model)                       ║")
	fmt.Println("║  ✅ System calls (read, write, fsync, socket)                ║")
	fmt.Println("║  ✅ Virtual memory and memory layout                         ║")
	fmt.Println("║  ✅ File descriptors and OS limits                           ║")
	fmt.Println("║  ✅ GC pressure and mitigation                               ║")
	fmt.Println("║  ✅ Concurrency patterns for distributed systems             ║")
	fmt.Println("║                                                               ║")
	fmt.Println("║  Next: Phase 0.2 - Networking Fundamentals                  ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
}

func divider() string {
	return "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}
