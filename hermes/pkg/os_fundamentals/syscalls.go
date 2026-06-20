// pkg/os_fundamentals/syscalls.go
package os_fundamentals

import (
	"fmt"
	"os"
	"runtime"
	"time"
)



func SystemCallsDemo() {
	fmt.Println("=== SYSTEM CALLS: THE OS BOUNDARY ===")
	fmt.Println()

	tmpFile, err := os.CreateTemp("", "hermes-syscall-demo-*")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	data := []byte("Hermes WAL Entry: {term:1, index:1, command:'PUT key1 value1'}\n")

	

	fmt.Println("1. Buffered Write (to OS page cache):")
	start := time.Now()
	for i := 0; i < 1000; i++ {
		_, err := tmpFile.Write(data)
		if err != nil {
			fmt.Printf("Write error: %v\n", err)
			return
		}
	}
	bufferedTime := time.Since(start)
	fmt.Printf("   1000 buffered writes: %v (FAST - just writing to RAM)\n", bufferedTime)
	fmt.Printf("   Data is in OS page cache, NOT necessarily on disk!\n")
	fmt.Printf("   Risk: Machine crash = data loss\n")
	fmt.Println()

	

	fmt.Println("2. fsync() - Force data to physical disk:")
	start = time.Now()
	err = tmpFile.Sync()
	fsyncTime := time.Since(start)
	fmt.Printf("   One fsync(): %v (SLOW - waiting for disk hardware)\n", fsyncTime)
	fmt.Printf("   Now data is GUARANTEED to survive power loss\n")
	fmt.Println()

	fmt.Println("3. System call numbers (Linux x86-64):")
	if runtime.GOOS == "linux" {
		fmt.Printf("   read:   syscall #%d\n", 0)
		fmt.Printf("   write:  syscall #%d\n", 1)
		fmt.Printf("   open:   syscall #%d\n", 2)
		fmt.Printf("   fsync:  syscall #%d\n", 74)
		fmt.Printf("   socket: syscall #%d\n", 41)
	} else {
		fmt.Println("   (Syscall numbers vary by platform; shown on Linux)")
		fmt.Println("   read: 0, write: 1, open: 2, fsync: 74, socket: 41")
	}
	fmt.Println()

	

	fmt.Println("4. File Descriptors:")
	fmt.Printf("   stdin fd:  %d\n", int(os.Stdin.Fd()))
	fmt.Printf("   stdout fd: %d\n", int(os.Stdout.Fd()))
	fmt.Printf("   stderr fd: %d\n", int(os.Stderr.Fd()))
	fmt.Printf("   tmpFile fd: %d\n", int(tmpFile.Fd()))
	fmt.Println()
	fmt.Println("   In Hermes:")
	fmt.Println("   - Each peer connection = 1 fd")
	fmt.Println("   - Each client connection = 1 fd")
	fmt.Println("   - WAL file = 1 fd")
	fmt.Println("   - Each SSTable file = 1 fd")
	fmt.Println("   - OS default fd limit: 1024 (WAY too low for production)")
	fmt.Println("   - Production Hermes needs: ulimit -n 65536 minimum")

	demonstrateFdLimit()
}

func demonstrateFdLimit() {
	if runtime.GOOS == "windows" {
		fmt.Println("   File descriptor limits are not exposed via syscall.Getrlimit on Windows.")
		fmt.Println("   Run this demo on Linux/macOS for rlimit output.")
		return
	}

	fmt.Println("   Current soft limit: unavailable in this demo build")
	fmt.Println("   Current hard limit: unavailable in this demo build")
	fmt.Println("   $ ulimit -n 65536                    # shell session")
	fmt.Println("   $ echo '* soft nofile 65536' >> /etc/security/limits.conf  # system")
}
