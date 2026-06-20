// pkg/observability/demo.go
package observability

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"time"
)

func RunObservabilityDemo() {
	printHeader()

	fmt.Println("━━━ DEMO 1: Metrics Collection ━━━")
	demoMetrics()

	fmt.Println("\n━━━ DEMO 2: Structured Logging ━━━")
	demoStructuredLogging()

	fmt.Println("\n━━━ DEMO 3: Distributed Tracing ━━━")
	demoDistributedTracing()

	fmt.Println("\n━━━ DEMO 4: Health Checks ━━━")
	demoHealthChecks()

	fmt.Println("\n━━━ DEMO 5: Performance Profiling ━━━")
	demoPerformanceProfiling()

	fmt.Println("\n━━━ DEMO 6: RED & USE Methods ━━━")
	demoREDUSE()

	fmt.Println("\n━━━ DEMO 7: Alerting Logic ━━━")
	demoAlerting()

	fmt.Println("\n━━━ DEMO 8: Kubernetes Observability ━━━")
	demoKubernetesObservability()

	printSummary()
}

// ─────────────────────────────────────────────────────────────────────────────

func demoMetrics() {
	fmt.Println()
	fmt.Println("Simulating Hermes node metrics collection")
	fmt.Println()

	metrics := NewHermesMetrics("hermes-0")
	rng := rand.New(rand.NewSource(42))

	// Simulate a working Hermes node
	fmt.Println("Simulating 1000 operations...")

	for i := 0; i < 1000; i++ {
		// Simulate WAL writes
		walStart := time.Now()
		time.Sleep(time.Duration(rng.Intn(5)) * time.Millisecond)
		metrics.WALWriteLatency.ObserveDuration(walStart)
		metrics.WALWritesTotal.Inc()

		// Simulate fsync (less frequent, more expensive)
		if i%10 == 0 {
			fsyncStart := time.Now()
			time.Sleep(time.Duration(rng.Intn(15)+5) * time.Millisecond)
			metrics.WALFsyncLatency.ObserveDuration(fsyncStart)
			metrics.WALFsyncsTotal.Inc()
		}

		// Simulate Raft commits
		raftStart := time.Now()
		time.Sleep(time.Duration(rng.Intn(10)+5) * time.Millisecond)
		metrics.RaftCommitLatency.ObserveDuration(raftStart)
		metrics.RaftLogAppended.Inc()

		// Simulate client requests
		reqStart := time.Now()
		time.Sleep(time.Duration(rng.Intn(20)) * time.Millisecond)
		metrics.RequestDuration.ObserveDuration(reqStart)
		metrics.RequestsTotal.Inc()

		// Simulate occasional errors
		if rng.Float64() < 0.02 {
			metrics.GRPCErrors.Inc()
		}
	}

	// Set gauge values
	metrics.RaftCurrentTerm.Set(42)
	metrics.RaftIsLeader.Set(1) // we're the leader
	metrics.RaftReplicationLag.Set(3)
	metrics.ClusterSize.Set(5)
	metrics.GoroutineCount.Set(float64(runtime.NumGoroutine()))
	metrics.TxnActive.Set(12)
	metrics.PeerConnections.Set(4)
	metrics.MemTableSize.Set(32 * 1024 * 1024) // 32MB

	// Show metrics snapshot
	snap := metrics.Snapshot()

	fmt.Printf("📊 METRICS SNAPSHOT: %s\n\n", snap.NodeID)

	fmt.Printf("  RAFT:\n")
	fmt.Printf("    Leader:            %v\n", snap.RaftIsLeader)
	fmt.Printf("    Term:              %.0f\n", snap.RaftCurrentTerm)
	fmt.Printf("    Replication lag:   %.0f entries\n", snap.RaftReplicationLag)
	fmt.Printf("    Commit P50/P99:    %.1fms / %.1fms\n",
		snap.RaftCommitLatency.P50*1000, snap.RaftCommitLatency.P99*1000)
	fmt.Printf("    Heartbeats sent:   %d\n", snap.RaftHeartbeatsSent)
	fmt.Println()

	fmt.Printf("  WAL:\n")
	fmt.Printf("    Writes:            %d\n", snap.WALWritesTotal)
	fmt.Printf("    Fsyncs:            %d\n", snap.WALFsyncsTotal)
	fmt.Printf("    Write P50/P99:     %.1fms / %.1fms\n",
		snap.WALWriteLatency.P50*1000, snap.WALWriteLatency.P99*1000)
	fmt.Printf("    Fsync P50/P99:     %.1fms / %.1fms\n",
		snap.WALFsyncLatency.P50*1000, snap.WALFsyncLatency.P99*1000)
	fmt.Println()

	fmt.Printf("  REQUESTS:\n")
	fmt.Printf("    Total:             %d\n", snap.RequestsTotal)
	fmt.Printf("    P50/P95/P99:       %.1fms / %.1fms / %.1fms\n",
		snap.RequestDuration.P50*1000,
		snap.RequestDuration.P95*1000,
		snap.RequestDuration.P99*1000)
	fmt.Printf("    gRPC errors:       %d\n", snap.GRPCErrors)
	fmt.Println()

	fmt.Printf("  SYSTEM:\n")
	fmt.Printf("    Goroutines:        %.0f\n", snap.GoroutineCount)
	fmt.Printf("    Active txns:       %.0f\n", snap.TxnActive)
	fmt.Printf("    Cluster size:      %.0f\n", snap.ClusterSize)
	fmt.Printf("    MemTable:          %.1fMB\n", snap.MemTableSizeBytes/1024/1024)
	fmt.Println()

	// Show Prometheus format (excerpt)
	fmt.Println("Prometheus format (excerpt):")
	promText := snap.PrometheusFormat()
	lines := strings.Split(promText, "\n")
	for _, line := range lines[:min(15, len(lines))] {
		if line != "" {
			fmt.Printf("  %s\n", line)
		}
	}
	fmt.Println("  ...")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ─────────────────────────────────────────────────────────────────────────────

func demoStructuredLogging() {
	fmt.Println()
	fmt.Println("Structured logging: machine-parseable, human-readable")
	fmt.Println()

	raftLog := NewRaftLogger("hermes-0")
	walLog := NewWALLogger("hermes-0")
	memberLog := NewMembershipLogger("hermes-0")

	fmt.Println("Log output (JSON format):")
	fmt.Println()

	// Election events
	raftLog.LeaderElected(42, 187*time.Millisecond, 3, 5)
	raftLog.EntryCommitted(1042, 42, 8*time.Millisecond)
	raftLog.ReplicationLag("hermes-2", 1500) // triggers error level

	fmt.Println()

	// WAL events
	walLog.WriteComplete(1042, 256, 3*time.Millisecond)
	walLog.FsyncComplete(8*time.Millisecond, 10)
	walLog.FsyncComplete(120*time.Millisecond, 1) // triggers error level

	fmt.Println()

	// Membership events
	memberLog.NodeJoined("hermes-5", "10.0.1.5:7000")
	memberLog.NodeSuspected("hermes-3", 8.5)
	memberLog.NodeDead("hermes-3", 3*time.Second)

	fmt.Println()
	fmt.Println("Why structured logging:")
	fmt.Println("  grep 'leader elected'              → find all elections")
	fmt.Println("  jq 'select(.election_duration > 500)' → slow elections")
	fmt.Println("  jq 'select(.level==\"error\")'        → all errors")
	fmt.Println("  Correlation: trace_id links log to trace")
}

// ─────────────────────────────────────────────────────────────────────────────

func demoDistributedTracing() {
	fmt.Println()
	fmt.Println("Distributed tracing: follow a request through the system")
	fmt.Println()

	tracer := NewTracer("hermes-0", "hermes")
	ctx := context.Background()

	// Trace a PUT request through the system
	fmt.Println("Tracing: PUT balance:alice = 1000")
	fmt.Println()

	// Root span: gRPC server receives request
	gRPCSpan, ctx := tracer.Start(ctx, "gRPC.HermesKV.Put")
	gRPCSpan.SetAttribute("grpc.method", "Put")
	gRPCSpan.SetAttribute("key", "balance:alice")
	gRPCSpan.SetAttribute("value_size", 8)
	time.Sleep(1 * time.Millisecond)

	// Child span: routing
	routeSpan, ctx := tracer.Start(ctx, "Router.Route")
	routeSpan.SetAttribute("key", "balance:alice")
	routeSpan.SetAttribute("shard_id", 0)
	routeSpan.SetAttribute("target_node", "hermes-0")
	time.Sleep(200 * time.Microsecond)
	routeSpan.End()

	// Child span: Raft propose
	raftSpan, ctx := tracer.Start(ctx, "Raft.Propose")
	raftSpan.SetAttribute("term", 42)
	raftSpan.SetAttribute("log_index", 1043)
	time.Sleep(1 * time.Millisecond)

	// WAL write
	walSpan, ctx := tracer.Start(ctx, "WAL.Write")
	walSpan.SetAttribute("sequence", 1043)
	walSpan.SetAttribute("bytes", 128)
	walSpan.AddEvent("fsync_start", nil)
	time.Sleep(3 * time.Millisecond)
	walSpan.AddEvent("fsync_complete", map[string]interface{}{
		"duration_ms": 3,
	})
	walSpan.End()
	_ = ctx

	// Replication
	replCtx := context.Background()
	replSpan, _ := tracer.Start(replCtx, "Raft.Replicate")
	replSpan.SetAttribute("follower_count", 2)
	replSpan.SetAttribute("required_acks", 2)
	time.Sleep(7 * time.Millisecond)
	replSpan.AddEvent("quorum_reached", map[string]interface{}{
		"acks":       2,
		"elapsed_ms": 7,
	})
	replSpan.End()

	raftSpan.End()

	// Storage apply
	applyCtx := context.Background()
	applySpan, _ := tracer.Start(applyCtx, "Storage.Apply")
	applySpan.SetAttribute("key", "balance:alice")
	applySpan.SetAttribute("bytes", 64)
	time.Sleep(500 * time.Microsecond)
	applySpan.End()

	gRPCSpan.AddEvent("response_sent", map[string]interface{}{
		"status": "ok",
	})
	gRPCSpan.End()

	// Print the trace
	fmt.Printf("Trace ID: %s\n\n", gRPCSpan.TraceID)
	PrintTrace(gRPCSpan, 0)

	totalDuration := gRPCSpan.Duration
	fmt.Printf("\nTotal request duration: %v\n", totalDuration)
	fmt.Printf("\nLatency breakdown:\n")
	fmt.Printf("  Routing:      ~0.2ms (%.0f%%)\n",
		0.2/float64(totalDuration.Milliseconds())*100)
	fmt.Printf("  WAL write:    ~3ms   (%.0f%%)\n",
		3.0/float64(totalDuration.Milliseconds())*100)
	fmt.Printf("  Replication:  ~7ms   (%.0f%%)\n",
		7.0/float64(totalDuration.Milliseconds())*100)
	fmt.Printf("  Apply:        ~0.5ms (%.0f%%)\n",
		0.5/float64(totalDuration.Milliseconds())*100)
}

// ─────────────────────────────────────────────────────────────────────────────

func demoHealthChecks() {
	fmt.Println()
	fmt.Println("Health checks: Kubernetes probes for Hermes")
	fmt.Println()

	checker := NewHealthChecker("hermes-0")

	// Scenario 1: Healthy node
	fmt.Println("Scenario 1: Healthy leader node")
	checker.CheckWAL(3*time.Millisecond, 8*time.Millisecond)
	checker.CheckRaft(true, true, 3)
	checker.CheckStorage(45.0, 2)
	checker.CheckMemory(3*1024*1024*1024, 8*1024*1024*1024)

	report := checker.Report()
	printHealthReport(report)

	// Scenario 2: Degraded node (slow disk)
	fmt.Println("\nScenario 2: Degraded node (slow WAL fsync)")
	checker.CheckWAL(50*time.Millisecond, 250*time.Millisecond)
	checker.CheckRaft(false, true, 50)
	checker.CheckStorage(45.0, 2)
	checker.CheckMemory(3*1024*1024*1024, 8*1024*1024*1024)

	report2 := checker.Report()
	printHealthReport(report2)

	// Scenario 3: Unhealthy node (no leader)
	fmt.Println("\nScenario 3: Unhealthy node (no leader known)")
	checker.CheckWAL(3*time.Millisecond, 8*time.Millisecond)
	checker.CheckRaft(false, false, 0)
	checker.CheckStorage(45.0, 2)
	checker.CheckMemory(7*1024*1024*1024, 8*1024*1024*1024)

	report3 := checker.Report()
	printHealthReport(report3)

	fmt.Println()
	fmt.Println("Kubernetes probe behavior:")
	fmt.Printf("  Scenario 1 → liveness=PASS, readiness=PASS\n")
	fmt.Printf("  Scenario 2 → liveness=PASS, readiness=PASS (degraded but serving)\n")
	fmt.Printf("  Scenario 3 → liveness=PASS, readiness=FAIL (remove from load balancer)\n")
}

func printHealthReport(report HealthReport) {
	icon := "✅"
	switch report.Status {
	case HealthDegraded:
		icon = "🟡"
	case HealthUnhealthy:
		icon = "❌"
	case HealthStarting:
		icon = "🔄"
	}

	fmt.Printf("  %s Overall: %s\n", icon, report.Status)
	for _, check := range report.Checks {
		checkIcon := "  ✅"
		if check.Status == HealthDegraded {
			checkIcon = "  🟡"
		} else if check.Status == HealthUnhealthy {
			checkIcon = "  ❌"
		}
		fmt.Printf("  %s %s: %s\n", checkIcon, check.Name, check.Message)
	}
}

// ─────────────────────────────────────────────────────────────────────────────

func demoPerformanceProfiling() {
	fmt.Println()
	fmt.Println("Performance profiling: finding bottlenecks")
	fmt.Println()

	fmt.Println("Built-in Go profiling (pprof):")
	fmt.Println()
	fmt.Println("  # CPU profile: what's using CPU?")
	fmt.Println("  go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30")
	fmt.Println()
	fmt.Println("  # Memory profile: what's using RAM?")
	fmt.Println("  go tool pprof http://localhost:6060/debug/pprof/heap")
	fmt.Println()
	fmt.Println("  # Goroutine profile: what are goroutines doing?")
	fmt.Println("  go tool pprof http://localhost:6060/debug/pprof/goroutine")
	fmt.Println()
	fmt.Println("  # Block profile: what's blocking goroutines?")
	fmt.Println("  go tool pprof http://localhost:6060/debug/pprof/block")
	fmt.Println()
	fmt.Println("  # Mutex profile: what mutexes are contended?")
	fmt.Println("  go tool pprof http://localhost:6060/debug/pprof/mutex")
	fmt.Println()

	// Show runtime stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	fmt.Println("Current runtime stats:")
	fmt.Printf("  Goroutines:        %d\n", runtime.NumGoroutine())
	fmt.Printf("  GOMAXPROCS:        %d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("  Heap allocated:    %.1f MB\n",
		float64(memStats.HeapAlloc)/1024/1024)
	fmt.Printf("  Heap in use:       %.1f MB\n",
		float64(memStats.HeapInuse)/1024/1024)
	fmt.Printf("  Stack in use:      %.1f MB\n",
		float64(memStats.StackInuse)/1024/1024)
	fmt.Printf("  GC runs:           %d\n", memStats.NumGC)
	fmt.Printf("  Last GC pause:     %v\n",
		time.Duration(memStats.PauseNs[(memStats.NumGC+255)%256]))
	fmt.Printf("  Total GC pause:    %v\n",
		time.Duration(memStats.PauseTotalNs))
	fmt.Println()

	fmt.Println("Performance optimization checklist:")
	fmt.Println("  □ WAL fsync P99 < 50ms   (use NVMe SSDs)")
	fmt.Println("  □ GC pause < 10ms        (tune GOGC and GOMEMLIMIT)")
	fmt.Println("  □ Goroutine count stable  (no leaks)")
	fmt.Println("  □ Mutex contention < 5%  (check with mutex profile)")
	fmt.Println("  □ CPU < 70% on leader    (headroom for spikes)")
}

// ─────────────────────────────────────────────────────────────────────────────

func demoREDUSE() {
	fmt.Println()
	fmt.Println("RED and USE methods: systematic performance analysis")
	fmt.Println()

	metrics := NewHermesMetrics("hermes-0")
	rng := rand.New(rand.NewSource(42))

	// Simulate 60 seconds of traffic
	for i := 0; i < 600; i++ {
		metrics.RequestsTotal.Inc()
		if rng.Float64() < 0.02 {
			metrics.GRPCErrors.Inc()
		}
		metrics.RequestDuration.Observe(float64(rng.Intn(50)) / 1000.0)
	}

	snap := metrics.Snapshot()

	fmt.Println("RED Method (for request-based systems):")
	fmt.Println()

	rate := float64(snap.RequestsTotal) / 60.0
	errorRate := float64(snap.GRPCErrors) / float64(snap.RequestsTotal) * 100

	fmt.Printf("  RATE:     %.0f requests/sec\n", rate)
	fmt.Printf("  ERRORS:   %.1f%% error rate\n", errorRate)
	fmt.Printf("  DURATION: P50=%.1fms P95=%.1fms P99=%.1fms\n",
		snap.RequestDuration.P50*1000,
		snap.RequestDuration.P95*1000,
		snap.RequestDuration.P99*1000)

	fmt.Println()
	fmt.Println("  Thresholds (adjust based on SLA):")
	fmt.Printf("  RATE:     if 0 → no traffic, if very high → investigate\n")
	fmt.Printf("  ERRORS:   if > 1%% → alert (P0)\n")
	fmt.Printf("  DURATION: if P99 > 100ms → warn, > 500ms → alert\n")

	fmt.Println()
	fmt.Println("USE Method (for resources):")
	fmt.Println()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	cpuUtilization := float64(rng.Intn(70) + 20) // simulate 20-90% CPU

	fmt.Printf("  CPU:    UTILIZATION=%.0f%%  SATURATION=%s  ERRORS=0\n",
		cpuUtilization,
		func() string {
			if cpuUtilization > 80 {
				return "HIGH (request queue growing)"
			}
			return "LOW"
		}())

	fmt.Printf("  MEMORY: UTILIZATION=%.0f%%  SATURATION=%s  ERRORS=0\n",
		float64(memStats.HeapInuse)/float64(memStats.HeapSys)*100,
		func() string {
			if memStats.HeapInuse > memStats.HeapSys*8/10 {
				return "HIGH (GC running frequently)"
			}
			return "LOW"
		}())

	fmt.Printf("  DISK:   UTILIZATION=~%.0f%%  SATURATION=%s  ERRORS=0\n",
		float64(rng.Intn(60)+20),
		"LOW")

	fmt.Printf("  NETWORK:UTILIZATION=~%.0f%%  SATURATION=%s  ERRORS=0\n",
		float64(rng.Intn(30)+5),
		"LOW")
}

// ─────────────────────────────────────────────────────────────────────────────

func demoAlerting() {
	fmt.Println()
	fmt.Println("Alerting: when to wake up the on-call engineer")
	fmt.Println()

	type Alert struct {
		Severity    string
		Name        string
		Description string
		Resolution  string
	}

	alerts := []Alert{
		{
			Severity:    "CRITICAL",
			Name:        "HermesNoLeader",
			Description: "No Raft leader elected for 30+ seconds. Writes are BLOCKED.",
			Resolution: "1. Check if majority of pods are running\n" +
				"              2. Check network connectivity between pods\n" +
				"              3. Check Raft logs for election errors",
		},
		{
			Severity:    "CRITICAL",
			Name:        "HermesClusterDegraded",
			Description: "Fewer than 3 nodes alive. One more failure = NO QUORUM.",
			Resolution: "1. Identify why nodes are down (OOMkilled? CrashLoopBackOff?)\n" +
				"              2. Restore failed nodes ASAP\n" +
				"              3. Do NOT restart all nodes simultaneously",
		},
		{
			Severity:    "WARNING",
			Name:        "HermesSlowWAL",
			Description: "WAL fsync P99 > 100ms. Disk may be degraded.",
			Resolution: "1. Check disk IOPS metrics\n" +
				"              2. Check for disk pressure from other pods\n" +
				"              3. Consider WAL group commit tuning",
		},
		{
			Severity:    "WARNING",
			Name:        "HermesHighReplicationLag",
			Description: "Follower is 1000+ entries behind leader.",
			Resolution: "1. Check follower pod resources (CPU, network)\n" +
				"              2. Check for disk saturation on follower\n" +
				"              3. Consider snapshot if lag > 100k entries",
		},
		{
			Severity:    "WARNING",
			Name:        "HermesLeaderFlapping",
			Description: "Leadership changing frequently (> 6x/min).",
			Resolution: "1. Check for network instability between pods\n" +
				"              2. Check leader pod CPU (GC pauses causing missed heartbeats?)\n" +
				"              3. Increase election timeout if network is unstable",
		},
	}

	for _, alert := range alerts {
		icon := "🚨"
		if alert.Severity == "WARNING" {
			icon = "⚠️ "
		}
		fmt.Printf("%s [%s] %s\n", icon, alert.Severity, alert.Name)
		fmt.Printf("   Description: %s\n", alert.Description)
		fmt.Printf("   Resolution:  %s\n", alert.Resolution)
		fmt.Println()
	}
}

// ─────────────────────────────────────────────────────────────────────────────

func demoKubernetesObservability() {
	fmt.Println()
	fmt.Println("Kubernetes observability: running Hermes in production")
	fmt.Println()

	fmt.Println("StatefulSet pod naming:")
	fmt.Println("  hermes-0  → first pod (often initial leader seed)")
	fmt.Println("  hermes-1  → second pod")
	fmt.Println("  hermes-2  → third pod")
	fmt.Println()
	fmt.Println("  Stable DNS:")
	fmt.Println("  hermes-0.hermes.default.svc.cluster.local → hermes-0 IP")
	fmt.Println("  hermes-1.hermes.default.svc.cluster.local → hermes-1 IP")
	fmt.Println()

	fmt.Println("Rolling upgrade (zero downtime):")
	fmt.Println()
	fmt.Println("  kubectl set image statefulset/hermes hermes=hermes:v2")
	fmt.Println()
	fmt.Println("  Kubernetes will:")
	fmt.Println("  1. Update hermes-4 first (highest pod number)")
	fmt.Println("  2. Wait for hermes-4 to be Ready")
	fmt.Println("  3. Update hermes-3")
	fmt.Println("  4. ... continue in reverse order ...")
	fmt.Println("  5. Update hermes-0 last")
	fmt.Println()
	fmt.Println("  Why reverse order?")
	fmt.Println("  - hermes-0 is usually the initial leader")
	fmt.Println("  - Updating followers first is safer")
	fmt.Println("  - New version must be backward-compatible with old version")
	fmt.Println("  - (Proto backward compatibility is required!)")
	fmt.Println()

	fmt.Println("Useful kubectl commands for Hermes:")
	fmt.Println()
	fmt.Println("  # Check cluster status")
	fmt.Println("  kubectl exec hermes-0 -- hermes-cli cluster status")
	fmt.Println()
	fmt.Println("  # View Raft leader")
	fmt.Println("  kubectl exec hermes-0 -- hermes-cli raft leader")
	fmt.Println()
	fmt.Println("  # Check replication lag")
	fmt.Println("  kubectl exec hermes-0 -- hermes-cli raft followers")
	fmt.Println()
	fmt.Println("  # Trigger manual compaction")
	fmt.Println("  kubectl exec hermes-0 -- hermes-cli storage compact")
	fmt.Println()
	fmt.Println("  # Get metrics")
	fmt.Println("  kubectl port-forward hermes-0 9090:9090")
	fmt.Println("  curl localhost:9090/metrics")
	fmt.Println()
	fmt.Println("  # Get trace")
	fmt.Println("  kubectl port-forward hermes-0 16686:16686")
	fmt.Println("  open http://localhost:16686  # Jaeger UI")
}

// ─────────────────────────────────────────────────────────────────────────────

func printHeader() {
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("\n╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "HERMES — PHASE 10: OBSERVABILITY & OPERATIONS")
	fmt.Printf("╚%s╝\n\n", line)
}

func printSummary() {
	fmt.Println()
	line := "═══════════════════════════════════════════════════════════════"
	fmt.Printf("╔%s╗\n", line)
	fmt.Printf("║  %-61s║\n", "PHASE 10 COMPLETE ✅")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "What we built:")
	fmt.Printf("║  %-61s║\n", "  ✅ HermesMetrics — all Prometheus metrics")
	fmt.Printf("║  %-61s║\n", "    ✅ Counters (requests, errors, WAL writes)")
	fmt.Printf("║  %-61s║\n", "    ✅ Gauges (goroutines, memory, cluster size)")
	fmt.Printf("║  %-61s║\n", "    ✅ Histograms (latency distributions)")
	fmt.Printf("║  %-61s║\n", "  ✅ StructuredLogger — JSON logs with context")
	fmt.Printf("║  %-61s║\n", "    ✅ RaftLogger, WALLogger, MembershipLogger")
	fmt.Printf("║  %-61s║\n", "  ✅ DistributedTracer — request traces")
	fmt.Printf("║  %-61s║\n", "    ✅ Parent/child spans with context propagation")
	fmt.Printf("║  %-61s║\n", "    ✅ Attributes, events, error marking")
	fmt.Printf("║  %-61s║\n", "  ✅ HealthChecker — Kubernetes probes")
	fmt.Printf("║  %-61s║\n", "    ✅ WAL, Raft, storage, memory checks")
	fmt.Printf("║  %-61s║\n", "    ✅ Liveness/readiness/startup endpoints")
	fmt.Printf("║  %-61s║\n", "  ✅ Kubernetes manifests")
	fmt.Printf("║  %-61s║\n", "    ✅ StatefulSet with stable pod names")
	fmt.Printf("║  %-61s║\n", "    ✅ PersistentVolumeClaims")
	fmt.Printf("║  %-61s║\n", "    ✅ Prometheus scraping annotations")
	fmt.Printf("║  %-61s║\n", "    ✅ Alerting rules")
	fmt.Printf("║  %-61s║\n", "    ✅ Rolling upgrade procedure")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "The three pillars now complete:")
	fmt.Printf("║  %-61s║\n", "  Logs:    What happened? (structured JSON)")
	fmt.Printf("║  %-61s║\n", "  Metrics: How many times? (Prometheus histograms)")
	fmt.Printf("║  %-61s║\n", "  Traces:  Where did time go? (span trees)")
	fmt.Printf("║  %-61s║\n", "")
	fmt.Printf("║  %-61s║\n", "→ NEXT: Phase 11 — Advanced Topics")
	fmt.Printf("║  %-61s║\n", "  (Query Processing, CDC, Multi-Region, Rate Limiting)")
	fmt.Printf("╚%s╝\n", line)
}
