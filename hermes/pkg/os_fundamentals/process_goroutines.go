package osfundamentals

import {
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
}

func ProcessInfo(){
	fmt.Println("==== Process Information ====")
	fmt.Println("Process ID (PID):    %d\n", os.Getpid())
	fmt.Println("Parent Process ID (PPID):     %d\n", os.Getppid())

	hostname, _ := os.Hostname()
	fmt.Printf("Hostname:     %s\n", hostname)
	fmt.Printf("Node Identity:            %s:%d\n", hostname, os.Getpid())

	fmt.Println()
	fmt.Println("=== GO RUNTIME / SCHEDULER INFO ===")
	fmt.Printf("GOMAXPROCS (logical CPUS used): %d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("Number of CPU cores available:  %d/n", runtime.NumCPU())
	fmt.Printf("Current goroutine count:   %d/n", runtime.NumGoroutine())
	




}