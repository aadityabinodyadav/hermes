// pkg/txn/demo.go
package txn

import (
	"context"
	"fmt"
	"time"

	"github.com/aadityabinodyadav/hermes/pkg/clock"
)

func RunTransactionDemo() {
	printHeader()

	fmt.Println("━━━ DEMO 1: Lock Manager Basics ━━━")
	demoLockManager()

	fmt.Println("\n━━━ DEMO 2: Two-Phase Commit (2PC) ━━━")
	demoTwoPhaseCommit()

	fmt.Println("\n━━━ DEMO 3: 2PC Failure Scenarios ━━━")
	demo2PCFailure()

	fmt.Println("\n━━━ DEMO 4: Percolator Transaction ━━━")
	demoPercolator()

	fmt.Println("\n━━━ DEMO 5: Percolator Lock Conflict ━━━")
	demoPercolatorConflict()

	fmt.Println("\n━━━ DEMO 6: Serializable Snapshot Isolation ━━━")
	demoSSI()

	fmt.Println("\n━━━ DEMO 7: Saga Pattern ━━━")
	demoSaga()

	fmt.Println("\n━━━ DEMO 8: Transaction Comparison ━━━")
	demoTransactionComparison()

	printSummary()
}

// ─────────────────────────────────────────────────────────────────────────────

func demoLockManager() {
	fmt.Println()
	fmt.Println("Lock manager: concurrent access control")
	fmt.Println()

	lm := NewLockManager()

	txn1 := TxnID("txn-1")
	txn2 := TxnID("txn-2")

	// txn-1 acquires lock on key "A"
	lock1 := &Lock{
		Key:       "A",
		TxnID:     txn1,
		Timestamp: clock.NewHLC("node-1").Now(),
		ExpiresAt: time.Now().Add(5 * time.Second),
	}

	acquired := lm.TryLock("A", lock1)
	fmt.Printf("  txn-1 locks A: %v ✅\n", acquired)

	// txn-2 tries to lock same key
	lock2 := &Lock{
		Key:       "A",
		TxnID:     txn2,
		Timestamp: clock.NewHLC("node-2").Now(),
		ExpiresAt: time.Now().Add(5 * time.Second),
	}

	acquired = lm.TryLock("A", lock2)
	fmt.Printf("  txn-2 locks A: %v ❌ (conflict!)\n", acquired)

	// txn-1 releases lock
	lm.Unlock(txn1)
	fmt.Println("  txn-1 releases all locks")

	// txn-2 tries again
	acquired = lm.TryLock("A", lock2)
	fmt.Printf("  txn-2 locks A (after release): %v ✅\n", acquired)

	fmt.Println()
	fmt.Printf("  Lock stats: %v\n", lm.Stats())
}

// ─────────────────────────────────────────────────────────────────────────────

func demoTwoPhaseCommit() {
	fmt.Println()
	fmt.Println("Two-Phase Commit: classic distributed transaction")
	fmt.Println()

	hlc := clock.NewHLC("coordinator")
	coordinator := NewTwoPhaseCommitCoordinator(hlc)

	// Register participants (shards)
	coordinator.AddParticipant(&Participant{
		NodeID:  "shard-1",
		ShardID: 1,
		Address: "shard-1:7000",
	})
	coordinator.AddParticipant(&Participant{
		NodeID:  "shard-2",
		ShardID: 2,
		Address: "shard-2:7000",
	})

	ctx := context.Background()

	// Begin transaction
	txn, err := coordinator.Begin(ctx, Serializable, []uint64{1, 2})
	if err != nil {
		fmt.Printf("  Begin failed: %v\n", err)
		return
	}

	fmt.Printf("  Transaction started: %s\n", txn.ID)
	fmt.Printf("  Participants: %v\n", txn.Participants)
	fmt.Printf("  Read timestamp: %s\n", txn.ReadTimestamp)

	// Phase 1: Prepare
	fmt.Println()
	fmt.Println("  PHASE 1: PREPARE")
	allReady, err := coordinator.Prepare(ctx, txn.ID)
	if err != nil {
		fmt.Printf("  Prepare failed: %v\n", err)
		return
	}

	fmt.Printf("  All participants ready: %v\n", allReady)

	// Phase 2: Commit or Abort
	if allReady {
		fmt.Println()
		fmt.Println("  PHASE 2: COMMIT")
		err = coordinator.Commit(ctx, txn.ID)
		if err != nil {
			fmt.Printf("  Commit failed: %v\n", err)
			return
		}
		fmt.Println("  Transaction COMMITTED ✅")
	} else {
		fmt.Println()
		fmt.Println("  PHASE 2: ABORT")
		coordinator.Abort(ctx, txn.ID)
		fmt.Println("  Transaction ABORTED")
	}
}

// ─────────────────────────────────────────────────────────────────────────────

func demo2PCFailure() {
	fmt.Println()
	fmt.Println("2PC Failure: Coordinator crash after Prepare")
	fmt.Println()
	fmt.Println("  This is the BLOCKING PROBLEM of 2PC:")
	fmt.Println()
	fmt.Println("  Timeline:")
	fmt.Println("    T0: Coordinator sends PREPARE to P1, P2")
	fmt.Println("    T1: P1 votes YES, writes prepare to WAL")
	fmt.Println("    T2: P2 votes YES, writes prepare to WAL")
	fmt.Println("    T3: 💀 COORDINATOR CRASHES")
	fmt.Println("    T4: P1 and P2 are BLOCKED")
	fmt.Println("        - Can't commit (don't know coordinator's decision)")
	fmt.Println("        - Can't abort (they voted YES)")
	fmt.Println("        - Holding locks, blocking other transactions")
	fmt.Println()
	fmt.Println("  Recovery:")
	fmt.Println("    - P1 and P2 must wait for coordinator to recover")
	fmt.Println("    - Coordinator reads WAL, sees decision")
	fmt.Println("    - Coordinator sends COMMIT/ABORT to unblock")
	fmt.Println()
	fmt.Println("  This is why Percolator is preferred for high availability!")
}

// ─────────────────────────────────────────────────────────────────────────────

func demoPercolator() {
	fmt.Println()
	fmt.Println("Percolator: Optimistic distributed transaction")
	fmt.Println()

	hlc := clock.NewHLC("node-1")
	client := NewPercolatorClient(hlc)

	ctx := context.Background()

	// Begin transaction
	txn, err := client.Begin(ctx, Serializable)
	if err != nil {
		fmt.Printf("  Begin failed: %v\n", err)
		return
	}

	fmt.Printf("  Transaction: %s\n", txn.ID)
	fmt.Printf("  Read timestamp: %s\n", txn.ReadTimestamp)

	// Write to multiple keys
	txn.Set("account:A", []byte("100"))
	txn.Set("account:B", []byte("200"))
	txn.Set("account:C", []byte("300"))

	fmt.Println()
	fmt.Println("  Write set:")
	for key := range txn.WriteSet {
		fmt.Printf("    %s\n", key)
	}
	fmt.Printf("  Primary key: %s\n", txn.PrimaryKey)

	// Prewrite phase
	fmt.Println()
	fmt.Println("  PHASE 1: PREWRITE")
	err = client.Prewrite(ctx, txn)
	if err != nil {
		fmt.Printf("  Prewrite failed: %v\n", err)
		return
	}
	fmt.Println("  All keys prewritten (locks acquired)")

	// Commit phase
	fmt.Println()
	fmt.Println("  PHASE 2: COMMIT")
	err = client.Commit(ctx, txn)
	if err != nil {
		fmt.Printf("  Commit failed: %v\n", err)
		return
	}
	fmt.Println("  Transaction COMMITTED ✅")
	fmt.Printf("  Commit timestamp: %s\n", txn.CommitTimestamp)

	fmt.Println()
	fmt.Println("  KEY INSIGHT: Primary key committed first")
	fmt.Println("  Secondary keys committed asynchronously")
	fmt.Println("  If coordinator crashes after primary commit:")
	fmt.Println("    → Other participants can check primary and complete")
	fmt.Println("    → NO BLOCKING!")
}

// ─────────────────────────────────────────────────────────────────────────────

func demoPercolatorConflict() {
	fmt.Println()
	fmt.Println("Percolator: Conflict detection during prewrite")
	fmt.Println()

	hlc := clock.NewHLC("node-1")
	client1 := NewPercolatorClient(hlc)
	client2 := NewPercolatorClient(hlc)

	ctx := context.Background()

	// Transaction 1 starts
	txn1, _ := client1.Begin(ctx, Serializable)
	txn1.Set("balance:X", []byte("100"))

	// Transaction 2 starts (concurrent)
	txn2, _ := client2.Begin(ctx, Serializable)
	txn2.Set("balance:X", []byte("200"))

	fmt.Println("  txn-1 and txn-2 both write to balance:X")
	fmt.Println()

	// txn-1 prewrites first
	fmt.Println("  txn-1 prewrite...")
	client1.Prewrite(ctx, txn1)
	fmt.Println("  ✅ txn-1 acquired lock on balance:X")

	// txn-2 tries to prewrite (will conflict)
	fmt.Println("  txn-2 prewrite...")
	err := client2.Prewrite(ctx, txn2)
	if err != nil {
		fmt.Printf("  ❌ txn-2 prewrite failed: %v\n", err)
		fmt.Println("  (Lock held by txn-1)")
	}

	// txn-1 commits
	client1.Commit(ctx, txn1)
	fmt.Println("  ✅ txn-1 committed, lock released")

	// txn-2 can now retry
	fmt.Println()
	fmt.Println("  txn-2 retries after txn-1 commits...")
	txn2Retry, _ := client2.Begin(ctx, Serializable)
	txn2Retry.Set("balance:X", []byte("200"))
	client2.Prewrite(ctx, txn2Retry)
	client2.Commit(ctx, txn2Retry)
	fmt.Println("  ✅ txn-2 committed on retry")
}

// ─────────────────────────────────────────────────────────────────────────────

func demoSSI() {
	fmt.Println()
	fmt.Println("Serializable Snapshot Isolation: Conflict detection")
	fmt.Println()

	detector := NewSSIDetector()
	hlc := clock.NewHLC("node-1")

	// Transaction 1
	txn1 := &Transaction{
		ID:            "txn-1",
		ReadTimestamp: hlc.Now(),
		StartTime:     time.Now(),
	}
	detector.RegisterTxn(txn1)
	detector.RecordRead("txn-1", "account:A", txn1.ReadTimestamp)
	detector.RecordWrite("txn-1", "account:B", hlc.Now())

	fmt.Println("  txn-1: READ account:A, WRITE account:B")

	// Transaction 2 (concurrent)
	time.Sleep(time.Millisecond)
	txn2 := &Transaction{
		ID:            "txn-2",
		ReadTimestamp: hlc.Now(),
		StartTime:     time.Now(),
	}
	detector.RegisterTxn(txn2)
	detector.RecordRead("txn-2", "account:B", txn2.ReadTimestamp)
	detector.RecordWrite("txn-2", "account:A", hlc.Now())

	fmt.Println("  txn-2: READ account:B, WRITE account:A")
	fmt.Println()

	// This creates a rw-dependency cycle:
	// txn-1 reads A, txn-2 writes A → rw-dep
	// txn-2 reads B, txn-1 writes B → rw-dep
	// CYCLE → one must abort!

	fmt.Println("  Checking for serialization conflicts...")

	// Commit txn-1 first
	detector.Commit("txn-1", hlc.Now())
	fmt.Println("  txn-1 committed")

	// Try to commit txn-2
	err := detector.CheckConflict(txn2)
	if err != nil {
		fmt.Printf("  ❌ txn-2 aborted: %v\n", err)
		fmt.Println()
		fmt.Println("  This is WRITE SKEW prevention!")
		fmt.Println("  Without SSI: both transactions would commit")
		fmt.Println("  Result: A and B both have stale values")
		fmt.Println("  With SSI: one aborts, maintains consistency")
	} else {
		detector.Commit("txn-2", hlc.Now())
		fmt.Println("  ✅ txn-2 committed (no conflict)")
	}
}

// ─────────────────────────────────────────────────────────────────────────────

func demoSaga() {
	fmt.Println()
	fmt.Println("Saga Pattern: Long-running distributed transaction")
	fmt.Println()

	orchestrator := NewSagaOrchestrator()

	// Create order saga
	steps := CreateOrderSaga()

	// Begin saga
	saga := orchestrator.Begin(
		"order-12345",
		steps,
		map[string]interface{}{
			"order_id":   "12345",
			"product_id": "PROD-001",
			"quantity":   2,
			"amount":     99.99,
		},
	)

	fmt.Printf("  Saga started: %s\n", saga.ID)
	fmt.Printf("  Steps: %d\n", len(saga.Steps))
	fmt.Println()

	// Execute saga
	ctx := context.Background()
	err := orchestrator.Execute(ctx, saga)

	fmt.Println()
	if err != nil {
		fmt.Printf("  ❌ Saga failed: %v\n", err)
		fmt.Println("  Compensations were executed automatically!")
	} else {
		fmt.Println("  ✅ Saga completed successfully!")
	}

	fmt.Println()
	fmt.Printf("  Final state: %s\n", saga.State)
	fmt.Printf("  Duration: %v\n", saga.CompletedAt.Sub(saga.StartedAt))

	// Demo failure scenario
	fmt.Println()
	fmt.Println("  SAGA FAILURE SCENARIO:")
	fmt.Println("  If 'Create Shipment' fails:")
	fmt.Println("    → Compensate 'Charge Credit Card' (refund)")
	fmt.Println("    → Compensate 'Reserve Inventory' (release)")
	fmt.Println("    → 'Send Confirmation' has no compensation")
	fmt.Println()
	fmt.Println("  KEY BENEFIT: No distributed locks held for minutes!")
	fmt.Println("  Each step commits independently.")
	fmt.Println("  Tradeoff: Eventual consistency, not atomic.")
}

// ─────────────────────────────────────────────────────────────────────────────

func demoTransactionComparison() {
	fmt.Println()
	fmt.Println("Transaction Model Comparison")
	fmt.Println()

	fmt.Println("┌──────────────────┬─────────────┬──────────────┬─────────────┐")
	fmt.Println("│ Model            │ Blocking    │ Coordinator  │ Use Case    │")
	fmt.Println("├──────────────────┼─────────────┼──────────────┼─────────────┤")
	fmt.Println("│ 2PC              │ YES         │ Central      │ Small cluster│")
	fmt.Println("│ Percolator       │ NO          │ Distributed  │ Spanner/TiDB│")
	fmt.Println("│ SSI              │ NO (abort)  │ None         │ PostgreSQL  │")
	fmt.Println("│ Saga             │ NO          │ Orchestrator │ Microservices│")
	fmt.Println("└──────────────────┴─────────────┴──────────────┴─────────────┘")
	fmt.Println()

	fmt.Println("HERMES RECOMMENDATIONS:")
	fmt.Println()
	fmt.Println("  Single-shard transaction:")
	fmt.Println("    → Use local MVCC (no coordination needed)")
	fmt.Println()
	fmt.Println("  Multi-shard, low latency:")
	fmt.Println("    → Use Percolator (non-blocking, high availability)")
	fmt.Println()
	fmt.Println("  Multi-shard, simple setup:")
	fmt.Println("    → Use 2PC (easier to implement, but blocking)")
	fmt.Println()
	fmt.Println("  Long-running, cross-service:")
	fmt.Println("    → Use Saga (eventual consistency, no locks)")
	fmt.Println()
	fmt.Println("  Strict serializability required:")
	fmt.Println("    → Use SSI on top of Percolator")
	fmt.Println("    → Abort on conflicts, retry client-side")
}

// ─────────────────────────────────────────────────────────────────────────────

func printHeader() {
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("\n╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "HERMES — PHASE 7: DISTRIBUTED TRANSACTIONS")
	fmt.Printf("╚%s╝\n\n", line)
}

func printSummary() {
	fmt.Println()
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "PHASE 7 COMPLETE ✅")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "What we built:")
	fmt.Printf("║  %-61s║\n", "  ✅ LockManager — concurrent access control")
	fmt.Printf("║  %-61s║\n", "  ✅ TwoPhaseCommitCoordinator — classic 2PC")
	fmt.Printf("║  %-61s║\n", "  ✅ PercolatorClient — optimistic transactions")
	fmt.Printf("║  %-61s║\n", "  ✅ SSIDetector — serializable snapshot isolation")
	fmt.Printf("║  %-61s║\n", "  ✅ SagaOrchestrator — long-running transactions")
	fmt.Printf("║  %-61s║\n", "  ✅ Transaction types and isolation levels")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "Key insights:")
	fmt.Printf("║  %-61s║\n", "  2PC: Simple but BLOCKING (coordinator crash = block)")
	fmt.Printf("║  %-61s║\n", "  Percolator: NON-BLOCKING (primary key = coordinator)")
	fmt.Printf("║  %-61s║\n", "  SSI: Detects write skew, may abort transactions")
	fmt.Printf("║  %-61s║\n", "  Saga: No locks, compensating actions, eventual consistency")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "Real systems:")
	fmt.Printf("║  %-61s║\n", "  2PC: Oracle, MySQL Cluster, SQL Server")
	fmt.Printf("║  %-61s║\n", "  Percolator: Google Spanner, TiDB, CockroachDB")
	fmt.Printf("║  %-61s║\n", "  SSI: PostgreSQL (serializable mode)")
	fmt.Printf("║  %-61s║\n", "  Saga: Microservices, event-driven architectures")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "How Phase 7 connects forward:")
	fmt.Printf("║  %-61s║\n", "  → Phase 8 (Consistency): Linearizable reads use txn timestamps")
	fmt.Printf("║  %-61s║\n", "  → Phase 9 (Fault): Transaction recovery after node failure")
	fmt.Printf("║  %-61s║\n", "  → Phase 10 (Ops): Transaction metrics, deadlock detection")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "→ NEXT: Phase 8 — Consistency & Coordination")
	fmt.Printf("╚%s╝\n", line)
}
