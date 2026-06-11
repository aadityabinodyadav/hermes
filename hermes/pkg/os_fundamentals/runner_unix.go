//go:build !windows
// +build !windows

package os_fundamentals

func memoryModelIfAvailable() {
	MemoryModel()
}
