package main

import (
	"fmt"
	"os"

	"github.com/aadityabinodyadav/hermes/pkg/clock"
	"github.com/aadityabinodyadav/hermes/pkg/os_fundamentals"
	"github.com/aadityabinodyadav/hermes/pkg/raft"
	"github.com/aadityabinodyadav/hermes/pkg/storage"
	"github.com/aadityabinodyadav/hermes/pkg/transport"
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
	case "transport":
		transport.RunTransportDemo()
	case "clocks":
		clock.RunClockDemo()
	case "storage":
		storage.RunStorageDemo()
	case "raft":
		raft.RunRaftDemo()
	default:
		fmt.Printf("Unknown phase: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Hermes Distributed Database")
	fmt.Println()
	fmt.Println("Usage: hermes-server <command>")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  os-fundamentals   Run Phase 0: OS fundamentals demo")
	fmt.Println("  transport         Run Phase 1: Transport layer demo")
	fmt.Println("  server            Run a Hermes server node (Phase 4+)")
}
