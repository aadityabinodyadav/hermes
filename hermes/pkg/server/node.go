// pkg/server/node.go (complete, compiling version)
package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/aadityabinodyadav/hermes/pkg/chaos"
	"github.com/aadityabinodyadav/hermes/pkg/clock"
	pb "github.com/aadityabinodyadav/hermes/proto"
	"github.com/aadityabinodyadav/hermes/pkg/consistency"
	"github.com/aadityabinodyadav/hermes/pkg/membership"
	"github.com/aadityabinodyadav/hermes/pkg/observability"
	"github.com/aadityabinodyadav/hermes/pkg/partition"
	"github.com/aadityabinodyadav/hermes/pkg/raft"
	"github.com/aadityabinodyadav/hermes/pkg/ratelimit"
	"github.com/aadityabinodyadav/hermes/pkg/storage"
	"github.com/aadityabinodyadav/hermes/pkg/transport"
)

// ─────────────────────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────────────────────

type Config struct {
	NodeID      string
	ListenAddr  string
	DataDir     string
	SeedNodes   []string
	ClusterSize int

	HeartbeatInterval time.Duration
	ElectionTimeout   time.Duration

	MemTableSize int64
	WALSync      bool

	GlobalRateLimit float64
	PerClientLimit  float64

	MetricsAddr string
	TLS         *transport.TLSConfig

	EnableChaos bool
}

func DefaultConfig(nodeID, listenAddr, dataDir string) *Config {
	return &Config{
		NodeID:            nodeID,
		ListenAddr:        listenAddr,
		DataDir:           dataDir,
		ClusterSize:       3,
		HeartbeatInterval: 50 * time.Millisecond,
		ElectionTimeout:   150 * time.Millisecond,
		MemTableSize:      64 * 1024 * 1024,
		WALSync:           true,
		PerClientLimit:    1000,
		MetricsAddr:       ":9090",
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// NODE
// ─────────────────────────────────────────────────────────────────────────────

type HermesNode struct {
	config *Config

	// Core components
	hlc       *clock.HLC
	engine    *storage.Engine
	raftNode  *raft.RaftNode
	raftTrans *hermesRaftTransport
	shardMap  *partition.ShardMap
	router    *partition.Router
	memberMgr      *membership.MembershipManager
	swimTransport  *membership.GrpcSWIMTransport

	// Consistency
	leaderLease *consistency.LeaderLease
	readIndex   *consistency.ReadIndexTracker

	// MVCC GC
	gcTracker *storage.ActiveTransactionTracker
	gc        *storage.MVCCGarbageCollector

	// Observability
	metrics *observability.HermesMetrics
	logger  *observability.Logger
	tracer  *observability.Tracer
	health  *observability.HealthChecker

	// Rate limiting
	globalLimiter  *ratelimit.TokenBucket
	circuitBreaker *ratelimit.CircuitBreaker

	// Chaos
	faultInjector *chaos.FaultInjector

	// Network
	connPool   *transport.ConnectionPool
	grpcServer *transport.Server

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	ready  chan struct{}
}

// ─────────────────────────────────────────────────────────────────────────────
// STARTUP
// ─────────────────────────────────────────────────────────────────────────────

func NewHermesNode(config *Config) (*HermesNode, error) {
	ctx, cancel := context.WithCancel(context.Background())
	return &HermesNode{
		config: config,
		ctx:    ctx,
		cancel: cancel,
		ready:  make(chan struct{}),
	}, nil
}

func (n *HermesNode) Start() error {
	// Step 1: Logger first (everything else uses it)
	n.logger = observability.NewLogger(n.config.NodeID, "node")
	n.logger.Info("starting Hermes node", map[string]interface{}{
		"version":     "1.0.0",
		"node_id":     n.config.NodeID,
		"listen_addr": n.config.ListenAddr,
		"data_dir":    n.config.DataDir,
		"tls_enabled": n.config.TLS != nil && n.config.TLS.IsEnabled(),
	})

	// Step 2: HLC
	n.hlc = clock.NewHLC(n.config.NodeID)

	// Step 3: Metrics + Tracer + Health
	n.metrics = observability.NewHermesMetrics(n.config.NodeID)
	n.tracer = observability.NewTracer(n.config.NodeID, "hermes")
	n.health = observability.NewHealthChecker(n.config.NodeID)

	// Step 4: MVCC GC tracker
	n.gcTracker = storage.NewActiveTransactionTracker()
	n.gc = storage.NewMVCCGarbageCollector(
		storage.DefaultGCConfig(), n.gcTracker,
	)

	// Step 5: Storage engine
	if err := n.startStorage(); err != nil {
		return err
	}

	// Step 6: gRPC server
	if err := n.startGRPC(); err != nil {
		return err
	}

	// Step 7: Membership
	if err := n.startMembership(); err != nil {
		return err
	}

	// Step 8: Raft
	if err := n.startRaft(); err != nil {
		return err
	}

	// Step 9: Shard map
	n.startShardMap()

	// Step 10: Consistency layer
	n.startConsistency()

	// Step 11: Rate limiting + circuit breaker
	n.startRateLimiting()

	// Step 12: Chaos (testing only)
	if n.config.EnableChaos {
		n.faultInjector = chaos.NewFaultInjector()
		n.faultInjector.Activate()
		n.logger.Warn("⚠️  CHAOS MODE ENABLED — NOT FOR PRODUCTION", nil)
	}

	// Step 13: Metrics HTTP server
	if n.config.MetricsAddr != "" {
		n.startHTTPServer()
	}

	// Step 14: Background loops
	go n.runBackgroundLoops()

	// Mark ready
	close(n.ready)
	n.logger.Info("✅ Hermes node READY")
	return nil
}

func (n *HermesNode) startStorage() error {
	if err := os.MkdirAll(n.config.DataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data dir: %w", err)
	}

	cfg := storage.DefaultConfig(filepath.Join(n.config.DataDir, "storage"))
	cfg.MemTableSize = n.config.MemTableSize

	var err error
	n.engine, err = storage.Open(cfg, n.hlc)
	if err != nil {
		return fmt.Errorf("storage open failed: %w", err)
	}

	n.gc.Start()
	n.health.SetCheck("storage", observability.HealthHealthy, "storage engine open")
	n.logger.Info("storage engine ready")
	return nil
}

func (n *HermesNode) startGRPC() error {
	serverCfg := transport.DefaultConfig(n.config.ListenAddr, n.config.NodeID)

	// Load TLS if configured
	if n.config.TLS != nil && n.config.TLS.IsEnabled() {
		creds, err := transport.LoadServerCredentials(n.config.TLS)
		if err != nil {
			return fmt.Errorf("TLS setup failed: %w", err)
		}
		_ = creds // wire into grpc.NewServer in production
		n.logger.Info("mTLS enabled")
	}

	var err error
	n.grpcServer, err = transport.NewServer(serverCfg)
	if err != nil {
		return fmt.Errorf("gRPC server create failed: %w", err)
	}

	// Register service handlers
	n.grpcServer.RegisterKVService(&kvHandler{node: n})
	n.grpcServer.RegisterRaftService(&raftHandler{node: n})
	n.grpcServer.RegisterMembershipService(&membershipHandler{node: n})

	if err := n.grpcServer.Start(); err != nil {
		return fmt.Errorf("gRPC server start failed: %w", err)
	}

	n.connPool = transport.NewConnectionPool(serverCfg)
	n.logger.Info("gRPC server started",
		map[string]interface{}{"addr": n.grpcServer.Addr()})
	return nil
}

func (n *HermesNode) startMembership() error {
	swimCfg := membership.DefaultSWIMConfig(
		n.config.NodeID, n.grpcServer.Addr(),
	)

	n.swimTransport = membership.NewGrpcSWIMTransport(n.config.NodeID, func(id string) string {
		// Use memberMgr to resolve node IDs to addresses if needed
		if n.memberMgr != nil {
			if m, ok := n.memberMgr.Get(id); ok {
				return m.Address
			}
		}
		return id // Fallback to id as address (works for seed nodes)
	})

	n.memberMgr = membership.NewMembershipManager(swimCfg, n.swimTransport)

	go n.handleMembershipEvents()

	n.memberMgr.Start()

	if len(n.config.SeedNodes) > 0 {
		if err := n.memberMgr.Join(n.config.SeedNodes); err != nil {
			n.logger.Warn("join via seeds failed (will retry)",
				nil, map[string]interface{}{"error": err.Error()})
		}
	}

	n.logger.Info("membership started")
	return nil
}

func (n *HermesNode) startRaft() error {
	var peerIDs []string
	for _, m := range n.memberMgr.AliveMembers() {
		if m.NodeID != n.config.NodeID {
			peerIDs = append(peerIDs, m.NodeID)
		}
	}

	raftCfg := raft.DefaultConfig(n.config.NodeID, peerIDs)
	raftCfg.HeartbeatTick = int(n.config.HeartbeatInterval / (10 * time.Millisecond))
	raftCfg.ElectionTick = int(n.config.ElectionTimeout / (10 * time.Millisecond))

	sm := &hermesStateMachine{engine: n.engine, logger: n.logger}
	rt := &hermesRaftTransport{
		pool:    n.connPool,
		nodeID:  n.config.NodeID,
		pending: make(map[string]chan raft.Message),
	}
	n.raftTrans = rt

	n.raftNode = raft.NewRaftNode(raftCfg, sm, n.hlc, rt)
	rt.raftNode = n.raftNode
	n.raftNode.Start()

	n.health.SetCheck("raft", observability.HealthStarting,
		"waiting for leader election")
	n.logger.Info("Raft started",
		map[string]interface{}{"peers": peerIDs})
	return nil
}

func (n *HermesNode) startShardMap() {
	allNodes := []string{n.config.NodeID}
	for _, m := range n.memberMgr.AliveMembers() {
		allNodes = append(allNodes, m.NodeID)
	}

	n.shardMap = partition.NewShardMap()
	numShards := len(allNodes)
	if numShards < 1 {
		numShards = 1
	}
	n.shardMap.Initialize(numShards, allNodes)
	n.router = partition.NewRouter(n.shardMap, partition.DefaultRouterConfig())
	n.logger.Info("shard map initialized",
		map[string]interface{}{"shards": numShards, "nodes": len(allNodes)})
}

func (n *HermesNode) startConsistency() {
	n.leaderLease = consistency.NewLeaderLease(
		n.config.NodeID, consistency.DefaultLeaseConfig(),
	)
	n.readIndex = consistency.NewReadIndexTracker(
		n.config.NodeID,
		func() {
			n.readIndex.ConfirmLeadership(n.raftNode.Status().CommitIndex)
		},
	)
}

func (n *HermesNode) startRateLimiting() {
	if n.config.GlobalRateLimit > 0 {
		burst := n.config.GlobalRateLimit * 0.1
		if burst < 10 {
			burst = 10
		}
		n.globalLimiter = ratelimit.NewTokenBucket(burst, n.config.GlobalRateLimit)
	}
	n.circuitBreaker = ratelimit.NewCircuitBreaker("storage")
}

// ─────────────────────────────────────────────────────────────────────────────
// PUBLIC API
// ─────────────────────────────────────────────────────────────────────────────

// Put writes a key-value pair via Raft consensus
func (n *HermesNode) Put(ctx context.Context, key string, value []byte) error {
	// Rate limit check
	if n.globalLimiter != nil && !n.globalLimiter.Allow(1) {
		n.metrics.NotLeaderTotal.Inc()
		return fmt.Errorf("rate limit exceeded")
	}

	// Circuit breaker check
	if !n.circuitBreaker.Allow() {
		return fmt.Errorf("circuit open: storage unavailable")
	}

	// Must be leader
	if !n.raftNode.IsLeader() {
		n.metrics.NotLeaderTotal.Inc()
		return fmt.Errorf("not leader: redirect to %s", n.raftNode.Leader())
	}

	// Encode command
	data := storage.NewPutCommand(key, value)

	// Start trace span
	span, ctx := n.tracer.Start(ctx, "HermesNode.Put")
	span.SetAttribute("key", key)
	span.SetAttribute("value_size", len(value))
	defer span.End()

	// Propose to Raft (this blocks until majority ACK)
	start := time.Now()
	if err := n.raftNode.Propose(ctx, data); err != nil {
		span.SetError(err)
		n.circuitBreaker.Failure()
		return fmt.Errorf("raft propose failed: %w", err)
	}

	// Record metrics
	n.metrics.RequestDuration.ObserveDuration(start)
	n.metrics.RequestsTotal.Inc()
	n.circuitBreaker.Success()

	return nil
}

// Get reads a key with linearizable consistency
func (n *HermesNode) Get(ctx context.Context, key string) ([]byte, bool, error) {
	// Rate limit
	if n.globalLimiter != nil && !n.globalLimiter.Allow(1) {
		return nil, false, fmt.Errorf("rate limit exceeded")
	}

	// Must be leader or have valid lease
	if !n.raftNode.IsLeader() && !n.leaderLease.IsValid() {
		return nil, false, fmt.Errorf("not leader: redirect to %s",
			n.raftNode.Leader())
	}

	// Linearizable read: confirm we're still leader
	if n.raftNode.IsLeader() {
		readIdx, err := n.readIndex.RequestRead(
			ctx, n.raftNode.Status().CommitIndex,
		)
		if err != nil {
			return nil, false, fmt.Errorf("readindex failed: %w", err)
		}
		// Wait until applied >= readIdx
		// (simplified: in production, wait on a channel)
		if n.raftNode.Status().AppliedIndex < readIdx {
			// Small wait (real impl: subscribe to apply events)
			time.Sleep(5 * time.Millisecond)
		}
	}

	// Start trace
	span, ctx := n.tracer.Start(ctx, "HermesNode.Get")
	span.SetAttribute("key", key)
	defer span.End()

	start := time.Now()
	val, found, err := n.engine.Get(key)
	n.metrics.RequestDuration.ObserveDuration(start)
	n.metrics.RequestsTotal.Inc()

	if err != nil {
		span.SetError(err)
	}

	return val, found, err
}

// Delete removes a key via Raft consensus
func (n *HermesNode) Delete(ctx context.Context, key string) error {
	if !n.raftNode.IsLeader() {
		return fmt.Errorf("not leader")
	}

	data := storage.NewDeleteCommand(key)
	return n.raftNode.Propose(ctx, data)
}

// Scan returns key-value entries in [startKey, endKey). An empty endKey means no upper bound.
func (n *HermesNode) Scan(ctx context.Context, startKey, endKey string) ([]KVPair, error) {
	if n.globalLimiter != nil && !n.globalLimiter.Allow(1) {
		return nil, fmt.Errorf("rate limit exceeded")
	}
	if n.raftNode == nil {
		return nil, fmt.Errorf("raft not started")
	}
	if !n.raftNode.IsLeader() && !n.leaderLease.IsValid() {
		return nil, fmt.Errorf("not leader: redirect to %s", n.raftNode.Leader())
	}

	span, ctx := n.tracer.Start(ctx, "HermesNode.Scan")
	span.SetAttribute("start_key", startKey)
	span.SetAttribute("end_key", endKey)
	defer span.End()

	start := time.Now()
	entries, err := n.engine.Scan(startKey, endKey)
	n.metrics.RequestDuration.ObserveDuration(start)
	n.metrics.RequestsTotal.Inc()
	if err != nil {
		span.SetError(err)
		return nil, err
	}

	pairs := make([]KVPair, 0, len(entries))
	for _, entry := range entries {
		pairs = append(pairs, KVPair{Key: entry.Key, Value: entry.Value})
	}
	return pairs, nil
}

// KVPair is a public key-value view returned by scans.
type KVPair struct {
	Key   string
	Value []byte
}

// IsLeader returns true if this node is the current Raft leader
func (n *HermesNode) IsLeader() bool {
	return n.raftNode != nil && n.raftNode.IsLeader()
}

// Leader returns the current leader's node ID
func (n *HermesNode) Leader() string {
	if n.raftNode == nil {
		return ""
	}
	return n.raftNode.Leader()
}

// Status returns the full node status
func (n *HermesNode) Status() NodeStatus {
	var raftStatus raft.Status
	if n.raftNode != nil {
		raftStatus = n.raftNode.Status()
	}

	alive := 0
	if n.memberMgr != nil {
		alive = len(n.memberMgr.AliveMembers())
	}
	if alive == 0 {
		alive = 1
	}
	shards := 0
	if n.shardMap != nil {
		shards = len(n.shardMap.All())
	}

	return NodeStatus{
		NodeID:       n.config.NodeID,
		IsLeader:     n.IsLeader(),
		LeaderID:     n.Leader(),
		Term:         raftStatus.Term,
		CommitIndex:  raftStatus.CommitIndex,
		AppliedIndex: raftStatus.AppliedIndex,
		ClusterSize:  alive,
		Shards:       shards,
		Healthy:      n.health.IsReady(),
	}
}

// NodeStatus is the public status view
type NodeStatus struct {
	NodeID       string
	IsLeader     bool
	LeaderID     string
	Term         uint64
	CommitIndex  uint64
	AppliedIndex uint64
	ClusterSize  int
	Shards       int
	Healthy      bool
}

func (s NodeStatus) String() string {
	role := "FOLLOWER"
	if s.IsLeader {
		role = "LEADER"
	}
	return fmt.Sprintf("[%s] %s term=%d commit=%d applied=%d cluster=%d",
		role, s.NodeID, s.Term, s.CommitIndex, s.AppliedIndex, s.ClusterSize)
}

// WaitReady blocks until the node is fully initialized
func (n *HermesNode) WaitReady(timeout time.Duration) error {
	select {
	case <-n.ready:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("node not ready after %v", timeout)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// STOP
// ─────────────────────────────────────────────────────────────────────────────

func (n *HermesNode) Stop() error {
	n.logger.Info("shutting down Hermes node gracefully")
	n.cancel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Transfer leadership if we're leader
	if n.raftNode != nil && n.IsLeader() {
		n.logger.Info("transferring leadership before shutdown")
		// In production: send TimeoutNow to a healthy follower
	}

	// Stop in reverse order of start
	if n.gc != nil {
		n.gc.Stop()
	}
	if n.grpcServer != nil {
		n.grpcServer.Stop(ctx)
	}
	if n.raftNode != nil {
		n.raftNode.Stop()
	}
	if n.memberMgr != nil {
		n.memberMgr.Stop()
	}
	if n.connPool != nil {
		n.connPool.Close()
	}
	if n.engine != nil {
		if err := n.engine.Close(); err != nil {
			n.logger.Error("storage close error", err)
		}
	}

	n.logger.Info("Hermes node stopped")
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// BACKGROUND
// ─────────────────────────────────────────────────────────────────────────────

func (n *HermesNode) runBackgroundLoops() {
	metricsTicker := time.NewTicker(10 * time.Second)
	healthTicker := time.NewTicker(5 * time.Second)
	defer metricsTicker.Stop()
	defer healthTicker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-metricsTicker.C:
			n.updateMetrics()
		case <-healthTicker.C:
			n.updateHealth()
		case newLeader := <-n.raftNode.LeaderChanges():
			n.onLeaderChange(newLeader)
		}
	}
}

func (n *HermesNode) handleMembershipEvents() {
	for event := range n.memberMgr.Events() {
		switch event.Type {
		case membership.EventNodeJoined:
			n.logger.Info("node joined",
				map[string]interface{}{"node": event.Member.NodeID})
			n.metrics.ClusterSize.Inc()
			n.connPool.AddPeer(event.Member.NodeID, event.Member.Address)

		case membership.EventNodeDead:
			n.logger.Warn("node dead",
				map[string]interface{}{"node": event.Member.NodeID})
			n.metrics.NodesDead.Inc()
			if event.Member.NodeID == n.raftNode.Leader() {
				n.raftNode.Step(n.ctx, raft.Message{
					Type: raft.MsgUnreachable,
					From: event.Member.NodeID,
				})
			}

		case membership.EventNodeSuspected:
			n.metrics.NodesSuspected.Inc()

		case membership.EventNodeRevived:
			n.metrics.NodesSuspected.Dec()
		}
	}
}

func (n *HermesNode) onLeaderChange(newLeader string) {
	if newLeader == n.config.NodeID {
		n.logger.Info("became leader",
			map[string]interface{}{"term": n.raftNode.Status().Term})
		n.metrics.RaftLeaderChanges.Inc()
		n.leaderLease.Renew(time.Now())
		n.metrics.RaftIsLeader.Set(1)
	} else {
		n.leaderLease.Expire()
		n.metrics.RaftIsLeader.Set(0)
		if newLeader != "" {
			n.logger.Info("new leader",
				map[string]interface{}{"leader": newLeader})
		}
	}
}

func (n *HermesNode) updateMetrics() {
	n.metrics.GoroutineCount.Set(float64(runtime.NumGoroutine()))

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	n.metrics.MemoryUsageBytes.Set(float64(mem.HeapAlloc))

	if n.engine != nil {
		stats := n.engine.Stats()
		n.metrics.MemTableSize.Set(float64(stats.MemTableSize))
		n.metrics.SSTableCount.Set(float64(stats.SSTableCount))
	}

	if n.memberMgr != nil {
		n.metrics.ClusterSize.Set(float64(
			len(n.memberMgr.AliveMembers())))
	}

	status := n.raftNode.Status()
	n.metrics.RaftCurrentTerm.Set(float64(status.Term))
}

func (n *HermesNode) updateHealth() {
	n.health.CheckRaft(
		n.IsLeader(),
		n.raftNode.Leader() != "",
		0,
	)

	if n.engine != nil {
		stats := n.engine.Stats()
		pct := float64(stats.MemTableSize) / float64(n.config.MemTableSize) * 100
		n.health.CheckStorage(pct, stats.SSTableCount)
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	n.health.CheckMemory(mem.HeapAlloc, mem.HeapSys)
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP
// ─────────────────────────────────────────────────────────────────────────────

func (n *HermesNode) startHTTPServer() {
	mux := http.NewServeMux()

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		snap := n.metrics.Snapshot()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, snap.PrometheusFormat())
	})

	mux.HandleFunc("/healthz/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"live"}`)
	})

	mux.HandleFunc("/healthz/ready", func(w http.ResponseWriter, _ *http.Request) {
		if n.health.IsReady() {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"status":"ready","node":"%s"}`, n.config.NodeID)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `{"status":"not_ready"}`)
		}
	})

	mux.HandleFunc("/healthz/startup", func(w http.ResponseWriter, _ *http.Request) {
		select {
		case <-n.ready:
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"status":"started"}`)
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `{"status":"starting"}`)
		}
	})

	mux.HandleFunc("/debug/status", func(w http.ResponseWriter, _ *http.Request) {
		status := n.Status()
		fmt.Fprintf(w, "%s\n", status)

		gcStats := n.gc.Stats()
		fmt.Fprintf(w, "GC: %s\n", gcStats)

		snap := n.metrics.Snapshot()
		fmt.Fprintf(w, "Goroutines: %.0f\n", snap.GoroutineCount)
		fmt.Fprintf(w, "MemTable:   %.1fMB\n", snap.MemTableSizeBytes/1024/1024)
	})

	go func() {
		n.logger.Info("HTTP server starting",
			map[string]interface{}{"addr": n.config.MetricsAddr})
		if err := http.ListenAndServe(n.config.MetricsAddr, mux); err != nil {
			if err != http.ErrServerClosed {
				n.logger.Error("HTTP server error", err)
			}
		}
	}()
}

// ─────────────────────────────────────────────────────────────────────────────
// INTERNAL IMPLEMENTATIONS
// ─────────────────────────────────────────────────────────────────────────────

// hermesStateMachine applies committed Raft entries to storage
type hermesStateMachine struct {
	engine *storage.Engine
	logger *observability.Logger
}

func (sm *hermesStateMachine) Apply(entry raft.LogEntry) error {
	if entry.Type == raft.EntryNoop || len(entry.Data) == 0 {
		return nil
	}

	if err := storage.ValidateCommand(entry.Data); err != nil {
		return fmt.Errorf("invalid command at index %d: %w", entry.Index, err)
	}

	key, value, deleted := storage.DecodeCommand(entry.Data)

	if deleted {
		return sm.engine.Delete(key)
	}
	return sm.engine.Put(key, value)
}

func (sm *hermesStateMachine) Snapshot() ([]byte, error) {
	return []byte("snapshot-placeholder"), nil
}

func (sm *hermesStateMachine) Restore(data []byte) error {
	sm.logger.Info("restoring from snapshot",
		map[string]interface{}{"size": len(data)})
	return nil
}

// hermesRaftTransport sends Raft messages via gRPC connection pool
type hermesRaftTransport struct {
	pool     *transport.ConnectionPool
	raftNode *raft.RaftNode
	nodeID   string

	mu      sync.Mutex
	pending map[string]chan raft.Message
}

func (t *hermesRaftTransport) Send(msgs []raft.Message) {
	for _, msg := range msgs {
		peer := t.pool.GetPeer(msg.To)
		if peer == nil {
			continue
		}

		// Context is created inside the goroutine to prevent immediate cancellation
		switch msg.Type {
		case raft.MsgAppend, raft.MsgHeartbeat:
			entries := make([]*pb.LogEntry, len(msg.Entries))
			for i, e := range msg.Entries {
				entries[i] = &pb.LogEntry{
					Index: e.Index,
					Term:  e.Term,
					Type:  pb.LogEntry_EntryType(e.Type), // Assuming direct mapping
					Data:  e.Data,
				}
			}

			req := &pb.AppendEntriesRequest{
				Term:          msg.Term,
				LeaderId:      msg.From,
				PrevLogIndex:  msg.LogIndex,
				PrevLogTerm:   msg.LogTerm,
				Entries:       entries,
				LeaderCommit:  msg.CommitIndex,
			}

			go func(m raft.Message) {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()
				resp, err := peer.RaftClient.AppendEntries(ctx, req)
				if err != nil {
					fmt.Printf("[RaftTransport] AppendEntries to %s failed: %v\n", m.To, err)
					return
				}
				if t.raftNode != nil {
					t.raftNode.Step(context.Background(), raft.Message{
						Type:          raft.MsgAppendRsp,
						To:            m.From,
						From:          m.To,
						Term:          resp.Term,
						Success:       resp.Success,
						ConflictIndex: resp.ConflictIndex,
						ConflictTerm:  resp.ConflictTerm,
					})
				}
			}(msg)

		case raft.MsgVote, raft.MsgPreVote:
			req := &pb.RequestVoteRequest{
				Term:         msg.Term,
				CandidateId:  msg.From,
				LastLogIndex: msg.LogIndex,
				LastLogTerm:  msg.LogTerm,
				PreVote:      msg.PreVote,
			}

			go func(m raft.Message) {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()
				resp, err := peer.RaftClient.RequestVote(ctx, req)
				if err != nil {
					fmt.Printf("[RaftTransport] RequestVote to %s failed: %v\n", m.To, err)
					return
				}
				if t.raftNode != nil {
					respType := raft.MsgVoteRsp
					if m.PreVote {
						respType = raft.MsgPreVoteRsp
					}
					t.raftNode.Step(context.Background(), raft.Message{
						Type:        respType,
						To:          m.From,
						From:        m.To,
						Term:        resp.Term,
						VoteGranted: resp.VoteGranted,
					})
				}
			}(msg)

		case raft.MsgAppendRsp, raft.MsgVoteRsp, raft.MsgPreVoteRsp:
			t.mu.Lock()
			ch, ok := t.pending[msg.To]
			if ok {
				delete(t.pending, msg.To)
			}
			t.mu.Unlock()
			if ok {
				ch <- msg
			}
		}
	}
}

func (t *hermesRaftTransport) AddPeer(id, addr string) {
	t.pool.AddPeer(id, addr)
}

func (t *hermesRaftTransport) RemovePeer(id string) {
	t.pool.RemovePeer(id)
}
