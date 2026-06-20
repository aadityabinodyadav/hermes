// cmd/hermes-server/main.go (complete final version)
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aadityabinodyadav/hermes/pkg/advanced"
	"github.com/aadityabinodyadav/hermes/pkg/chaos"
	"github.com/aadityabinodyadav/hermes/pkg/clock"
	"github.com/aadityabinodyadav/hermes/pkg/consistency"
	"github.com/aadityabinodyadav/hermes/pkg/membership"
	"github.com/aadityabinodyadav/hermes/pkg/observability"
	"github.com/aadityabinodyadav/hermes/pkg/os_fundamentals"
	"github.com/aadityabinodyadav/hermes/pkg/partition"
	"github.com/aadityabinodyadav/hermes/pkg/raft"
	"github.com/aadityabinodyadav/hermes/pkg/server"
	"github.com/aadityabinodyadav/hermes/pkg/storage"
	"github.com/aadityabinodyadav/hermes/pkg/transport"
	"github.com/aadityabinodyadav/hermes/pkg/txn"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	// ── Learning demos (one per phase) ──────────────────────────────────
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
	case "partition":
		partition.RunPartitionDemo()
	case "membership":
		membership.RunMembershipDemo()
	case "txn":
		txn.RunTransactionDemo()
	case "consistency":
		consistency.RunConsistencyDemo()
	case "chaos":
		chaos.RunChaosDemo()
	case "observability":
		observability.RunObservabilityDemo()
	case "advanced":
		advanced.RunAdvancedDemo()

	case "all-demos":
		// Run ALL demos sequentially (the full course!)
		demos := []func(){
			os_fundamentals.RunAll,
			transport.RunTransportDemo,
			clock.RunClockDemo,
			storage.RunStorageDemo,
			raft.RunRaftDemo,
			partition.RunPartitionDemo,
			membership.RunMembershipDemo,
			txn.RunTransactionDemo,
			consistency.RunConsistencyDemo,
			chaos.RunChaosDemo,
			observability.RunObservabilityDemo,
			advanced.RunAdvancedDemo,
		}
		for _, demo := range demos {
			demo()
			fmt.Println("\nPress Enter for next phase...")
			fmt.Scanln()
		}

	// ── Production server mode ────────────────────────────────────────────
	case "server":
		runServer()

	default:
		fmt.Printf("Unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func runServer() {
	// Parse flags (simplified)
	nodeID := getEnv("HERMES_NODE_ID", "hermes-0")
	listenAddr := getEnv("HERMES_LISTEN_ADDR", "0.0.0.0:7001") // gRPC
	httpAddr := getEnv("HERMES_HTTP_ADDR", "0.0.0.0:7000")     // HTTP
	metricsAddr := getEnv("HERMES_METRICS_ADDR", ":9090")      // Metrics
	dataDir := getEnv("HERMES_DATA_DIR", "/data")
	seedNodesStr := getEnv("HERMES_SEED_NODES", "")

	config := server.DefaultConfig(nodeID, listenAddr, dataDir)
	config.MetricsAddr = metricsAddr
	if seedNodesStr != "" {
		config.SeedNodes = strings.Split(seedNodesStr, ",")
	}

	node, err := server.NewHermesNode(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create node: %v\n", err)
		os.Exit(1)
	}

	if err := node.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start node: %v\n", err)
		os.Exit(1)
	}

	httpAPI, err := server.StartHTTPAPI(node, httpAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start HTTP API: %v\n", err)
		os.Exit(1)
	}
	defer httpAPI.Stop(context.Background())

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("Hermes node %s running.\n", nodeID)
	fmt.Printf("gRPC listening on %s\n", listenAddr)
	fmt.Printf("HTTP listening on %s\n", httpAddr)
	fmt.Println("Press Ctrl+C to stop.")
	<-sigCh

	fmt.Println("Shutting down...")
	node.Stop()
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func printUsage() {
	fmt.Println(`Hermes — A distributed database built from scratch

USAGE:
  hermes-server <command>

LEARNING DEMOS (run each phase):
  os-fundamentals    Phase 0:  OS, goroutines, syscalls, memory
  transport          Phase 1:  gRPC, protobuf, connection management
  clocks             Phase 2:  Lamport, vector clocks, HLC
  storage            Phase 3:  WAL, LSM-Tree, SSTables, Bloom filter
  raft               Phase 4:  Complete Raft consensus
  partition          Phase 5:  Sharding, consistent hashing
  membership         Phase 6:  SWIM gossip, failure detection
  txn                Phase 7:  2PC, Percolator, SSI, Saga
  consistency        Phase 8:  Linearizability, locks, CRDTs
  chaos              Phase 9:  Fault injection, Jepsen, simulation
  observability      Phase 10: Metrics, tracing, health, Kubernetes
  advanced           Phase 11: Query processing, CDC, rate limiting

SPECIAL:
  all-demos          Run all phases sequentially (full course!)
  whats-next         What we didn't cover + learning path forward

PRODUCTION:
  server             Run a real Hermes server node
                     Configure via environment variables:
                       HERMES_NODE_ID     (default: hermes-0)
                       HERMES_LISTEN_ADDR (default: 0.0.0.0:7001)
                       HERMES_HTTP_ADDR   (default: 0.0.0.0:7000)
                       HERMES_DATA_DIR    (default: /data)

EXAMPLES:
  # Learn distributed systems:
  hermes-server raft
  hermes-server chaos

  # Run full course:
  hermes-server all-demos

  # Run production node:
  HERMES_NODE_ID=hermes-0 hermes-server server`)
}
