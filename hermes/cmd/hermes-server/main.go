// cmd/hermes-server/main.go
package main

import (
	"fmt"
	"os"

	"github.com/aadityabinodyadav/hermes/pkg/os_fundamentals"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: hermes-server <phase>")
		fmt.Println("Phases: os-fundamentals, networking, clocks, storage, raft, ...")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "os-fundamentals":
		os_fundamentals.RunAll()
	default:
		fmt.Printf("Unknown phase: %s\n", os.Args[1])
		os.Exit(1)
	}
}
