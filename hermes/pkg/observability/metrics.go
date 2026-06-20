// pkg/observability/metrics.go
package observability

// Metrics defines all Hermes Prometheus metrics
//
// Naming convention: hermes_<subsystem>_<name>_<unit>
//   hermes_raft_leader_changes_total
//   hermes_wal_fsync_duration_seconds
//   hermes_storage_memtable_size_bytes
//
// Labels: always include node_id for per-node breakdown
//   {node_id="hermes-0", shard_id="0"}

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// METRIC TYPES (simplified implementation, mirrors Prometheus data model)
// In production: use github.com/prometheus/client_golang
// ─────────────────────────────────────────────────────────────────────────────

// Counter is a monotonically increasing metric
type Counter struct {
	mu     sync.Mutex
	name   string
	help   string
	value  int64
	labels map[string]string
}

func NewCounter(name, help string) *Counter {
	return &Counter{name: name, help: help}
}

func (c *Counter) Inc() {
	atomic.AddInt64(&c.value, 1)
}

func (c *Counter) Add(n int64) {
	atomic.AddInt64(&c.value, n)
}

func (c *Counter) Value() int64 {
	return atomic.LoadInt64(&c.value)
}

// Gauge is a value that goes up and down
type Gauge struct {
	mu    sync.Mutex
	name  string
	help  string
	value int64 // stored as int64, represents float64 bits
}

func NewGauge(name, help string) *Gauge {
	return &Gauge{name: name, help: help}
}

func (g *Gauge) Set(v float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value = int64(math.Float64bits(v))
}

func (g *Gauge) Inc() { g.Add(1) }
func (g *Gauge) Dec() { g.Add(-1) }

func (g *Gauge) Add(v float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	current := math.Float64frombits(uint64(g.value))
	g.value = int64(math.Float64bits(current + v))
}

func (g *Gauge) Value() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return math.Float64frombits(uint64(g.value))
}

// Histogram tracks distribution of values
type Histogram struct {
	mu      sync.Mutex
	name    string
	help    string
	buckets []float64 // bucket upper bounds
	counts  []int64   // count per bucket
	sum     float64   // sum of all values
	count   int64     // total count
}

func NewHistogram(name, help string, buckets []float64) *Histogram {
	return &Histogram{
		name:    name,
		help:    help,
		buckets: buckets,
		counts:  make([]int64, len(buckets)+1), // +1 for +Inf bucket
	}
}

func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.sum += v
	h.count++

	for i, b := range h.buckets {
		if v <= b {
			h.counts[i]++
		}
	}
	h.counts[len(h.buckets)]++ // +Inf bucket always increments
}

func (h *Histogram) Percentile(p float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.count == 0 {
		return 0
	}

	target := int64(float64(h.count) * p)
	cumulative := int64(0)

	for i, count := range h.counts {
		cumulative += count
		if cumulative >= target {
			if i < len(h.buckets) {
				return h.buckets[i]
			}
			return math.Inf(1)
		}
	}

	return 0
}

// ObserveDuration records duration in seconds
func (h *Histogram) ObserveDuration(start time.Time) {
	h.Observe(time.Since(start).Seconds())
}

// Summary of histogram
type HistogramSummary struct {
	P50   float64
	P95   float64
	P99   float64
	Count int64
	Sum   float64
}

func (h *Histogram) Summary() HistogramSummary {
	return HistogramSummary{
		P50:   h.Percentile(0.50),
		P95:   h.Percentile(0.95),
		P99:   h.Percentile(0.99),
		Count: h.count,
		Sum:   h.sum,
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// HERMES METRICS REGISTRY
// ─────────────────────────────────────────────────────────────────────────────

// Default latency buckets for Hermes (in seconds)
// Chosen to give good resolution in the 1ms-100ms range
var DefaultLatencyBuckets = []float64{
	0.0001, // 100μs
	0.0005, // 500μs
	0.001,  // 1ms
	0.005,  // 5ms
	0.010,  // 10ms
	0.025,  // 25ms
	0.050,  // 50ms
	0.100,  // 100ms
	0.250,  // 250ms
	0.500,  // 500ms
	1.000,  // 1s
	5.000,  // 5s
}

// HermesMetrics holds all metrics for one Hermes node
type HermesMetrics struct {
	NodeID string

	// ── Raft Metrics ──────────────────────────────────────────────────────

	// Total leader elections (should be low in stable cluster)
	RaftLeaderChanges *Counter

	// Current Raft term
	RaftCurrentTerm *Gauge

	// Whether this node is the leader
	RaftIsLeader *Gauge

	// How many log entries followers are behind
	RaftReplicationLag *Gauge

	// Latency from propose to commit
	RaftCommitLatency *Histogram

	// Heartbeats sent to followers
	RaftHeartbeatsSent *Counter

	// Raft log entries appended
	RaftLogAppended *Counter

	// ── WAL Metrics ──────────────────────────────────────────────────────

	// WAL write latency (includes fsync time!)
	WALWriteLatency *Histogram

	// fsync latency (the expensive part)
	WALFsyncLatency *Histogram

	// WAL writes per second
	WALWritesTotal *Counter

	// WAL bytes written
	WALBytesTotal *Counter

	// WAL fsync calls
	WALFsyncsTotal *Counter

	// ── Storage Metrics ──────────────────────────────────────────────────

	// MemTable size in bytes
	MemTableSize *Gauge

	// Number of SSTables per level
	SSTableCount *Gauge

	// Storage read latency
	StorageReadLatency *Histogram

	// Storage write latency
	StorageWriteLatency *Histogram

	// Bloom filter hits (key found without disk read)
	BloomFilterHits *Counter

	// Bloom filter misses (needed disk read)
	BloomFilterMisses *Counter

	// Compaction bytes processed
	CompactionBytesTotal *Counter

	// ── Request Metrics ──────────────────────────────────────────────────

	// Total requests by method and status
	RequestsTotal *Counter

	// Request duration by method
	RequestDuration *Histogram

	// Requests that were NOT_LEADER (client needs to retry)
	NotLeaderTotal *Counter

	// ── Transaction Metrics ──────────────────────────────────────────────

	// Total transactions
	TxnTotal *Counter

	// Transaction conflicts (Percolator prewrite failures)
	TxnConflicts *Counter

	// Transaction duration
	TxnDuration *Histogram

	// Active transactions right now
	TxnActive *Gauge

	// ── Network Metrics ──────────────────────────────────────────────────

	// Bytes sent to peers
	NetworkBytesSent *Counter

	// Bytes received from peers
	NetworkBytesReceived *Counter

	// gRPC errors by code
	GRPCErrors *Counter

	// Active connections to peers
	PeerConnections *Gauge

	// ── Cluster Metrics ──────────────────────────────────────────────────

	// Current cluster size (alive nodes)
	ClusterSize *Gauge

	// Nodes currently suspected by SWIM
	NodesSuspected *Gauge

	// Nodes currently dead
	NodesDead *Gauge

	// ── System Metrics ──────────────────────────────────────────────────

	// Goroutine count (monitor for leaks!)
	GoroutineCount *Gauge

	// GC pause duration
	GCPauseDuration *Histogram

	// Memory used
	MemoryUsageBytes *Gauge

	// Open file descriptors
	FDCount *Gauge
}

// NewHermesMetrics creates all metrics for a Hermes node
func NewHermesMetrics(nodeID string) *HermesMetrics {
	return &HermesMetrics{
		NodeID: nodeID,

		// Raft
		RaftLeaderChanges:  NewCounter("hermes_raft_leader_changes_total", "Total leader changes"),
		RaftCurrentTerm:    NewGauge("hermes_raft_current_term", "Current Raft term"),
		RaftIsLeader:       NewGauge("hermes_raft_is_leader", "1 if this node is leader"),
		RaftReplicationLag: NewGauge("hermes_raft_replication_lag_entries", "Follower replication lag"),
		RaftCommitLatency:  NewHistogram("hermes_raft_commit_duration_seconds", "Raft commit latency", DefaultLatencyBuckets),
		RaftHeartbeatsSent: NewCounter("hermes_raft_heartbeats_sent_total", "Heartbeats sent"),
		RaftLogAppended:    NewCounter("hermes_raft_log_appended_total", "Log entries appended"),

		// WAL
		WALWriteLatency: NewHistogram("hermes_wal_write_duration_seconds", "WAL write latency", DefaultLatencyBuckets),
		WALFsyncLatency: NewHistogram("hermes_wal_fsync_duration_seconds", "WAL fsync latency", DefaultLatencyBuckets),
		WALWritesTotal:  NewCounter("hermes_wal_writes_total", "WAL writes"),
		WALBytesTotal:   NewCounter("hermes_wal_bytes_total", "WAL bytes written"),
		WALFsyncsTotal:  NewCounter("hermes_wal_fsyncs_total", "WAL fsync calls"),

		// Storage
		MemTableSize:         NewGauge("hermes_storage_memtable_size_bytes", "MemTable size"),
		SSTableCount:         NewGauge("hermes_storage_sstable_count", "SSTable count"),
		StorageReadLatency:   NewHistogram("hermes_storage_read_duration_seconds", "Storage read latency", DefaultLatencyBuckets),
		StorageWriteLatency:  NewHistogram("hermes_storage_write_duration_seconds", "Storage write latency", DefaultLatencyBuckets),
		BloomFilterHits:      NewCounter("hermes_storage_bloom_hits_total", "Bloom filter hits"),
		BloomFilterMisses:    NewCounter("hermes_storage_bloom_misses_total", "Bloom filter misses"),
		CompactionBytesTotal: NewCounter("hermes_storage_compaction_bytes_total", "Bytes compacted"),

		// Requests
		RequestsTotal:   NewCounter("hermes_requests_total", "Total requests"),
		RequestDuration: NewHistogram("hermes_request_duration_seconds", "Request latency", DefaultLatencyBuckets),
		NotLeaderTotal:  NewCounter("hermes_not_leader_total", "NOT_LEADER responses"),

		// Transactions
		TxnTotal:     NewCounter("hermes_txn_total", "Total transactions"),
		TxnConflicts: NewCounter("hermes_txn_conflicts_total", "Transaction conflicts"),
		TxnDuration:  NewHistogram("hermes_txn_duration_seconds", "Transaction duration", DefaultLatencyBuckets),
		TxnActive:    NewGauge("hermes_txn_active", "Active transactions"),

		// Network
		NetworkBytesSent:     NewCounter("hermes_network_bytes_sent_total", "Bytes sent"),
		NetworkBytesReceived: NewCounter("hermes_network_bytes_received_total", "Bytes received"),
		GRPCErrors:           NewCounter("hermes_grpc_errors_total", "gRPC errors"),
		PeerConnections:      NewGauge("hermes_peer_connections", "Peer connections"),

		// Cluster
		ClusterSize:    NewGauge("hermes_cluster_size", "Cluster size"),
		NodesSuspected: NewGauge("hermes_nodes_suspected", "Suspected nodes"),
		NodesDead:      NewGauge("hermes_nodes_dead", "Dead nodes"),

		// System
		GoroutineCount:   NewGauge("hermes_goroutines", "Goroutine count"),
		GCPauseDuration:  NewHistogram("hermes_gc_pause_duration_seconds", "GC pause duration", DefaultLatencyBuckets),
		MemoryUsageBytes: NewGauge("hermes_memory_usage_bytes", "Memory usage"),
		FDCount:          NewGauge("hermes_file_descriptors", "File descriptors"),
	}
}

// Snapshot returns a point-in-time view of all metrics
func (m *HermesMetrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		NodeID:    m.NodeID,
		Timestamp: time.Now(),

		// Raft
		RaftLeaderChanges:  m.RaftLeaderChanges.Value(),
		RaftCurrentTerm:    m.RaftCurrentTerm.Value(),
		RaftIsLeader:       m.RaftIsLeader.Value() > 0,
		RaftReplicationLag: m.RaftReplicationLag.Value(),
		RaftCommitLatency:  m.RaftCommitLatency.Summary(),
		RaftHeartbeatsSent: m.RaftHeartbeatsSent.Value(),

		// WAL
		WALWriteLatency: m.WALWriteLatency.Summary(),
		WALFsyncLatency: m.WALFsyncLatency.Summary(),
		WALWritesTotal:  m.WALWritesTotal.Value(),
		WALFsyncsTotal:  m.WALFsyncsTotal.Value(),

		// Storage
		MemTableSizeBytes: m.MemTableSize.Value(),
		SSTableCount:      m.SSTableCount.Value(),
		BloomFilterHits:   m.BloomFilterHits.Value(),
		BloomFilterMisses: m.BloomFilterMisses.Value(),

		// Requests
		RequestsTotal:   m.RequestsTotal.Value(),
		RequestDuration: m.RequestDuration.Summary(),
		NotLeaderTotal:  m.NotLeaderTotal.Value(),

		// Transactions
		TxnTotal:     m.TxnTotal.Value(),
		TxnConflicts: m.TxnConflicts.Value(),
		TxnDuration:  m.TxnDuration.Summary(),
		TxnActive:    m.TxnActive.Value(),

		// Network
		NetworkBytesSent:     m.NetworkBytesSent.Value(),
		NetworkBytesReceived: m.NetworkBytesReceived.Value(),
		GRPCErrors:           m.GRPCErrors.Value(),
		PeerConnections:      m.PeerConnections.Value(),

		// Cluster
		ClusterSize:    m.ClusterSize.Value(),
		NodesSuspected: m.NodesSuspected.Value(),
		NodesDead:      m.NodesDead.Value(),

		// System
		GoroutineCount:   m.GoroutineCount.Value(),
		MemoryUsageBytes: m.MemoryUsageBytes.Value(),
		FDCount:          m.FDCount.Value(),
	}
}

// MetricsSnapshot is a point-in-time view of all metrics
type MetricsSnapshot struct {
	NodeID    string
	Timestamp time.Time

	// Raft
	RaftLeaderChanges  int64
	RaftCurrentTerm    float64
	RaftIsLeader       bool
	RaftReplicationLag float64
	RaftCommitLatency  HistogramSummary
	RaftHeartbeatsSent int64

	// WAL
	WALWriteLatency HistogramSummary
	WALFsyncLatency HistogramSummary
	WALWritesTotal  int64
	WALFsyncsTotal  int64

	// Storage
	MemTableSizeBytes float64
	SSTableCount      float64
	BloomFilterHits   int64
	BloomFilterMisses int64

	// Requests
	RequestsTotal   int64
	RequestDuration HistogramSummary
	NotLeaderTotal  int64

	// Transactions
	TxnTotal     int64
	TxnConflicts int64
	TxnDuration  HistogramSummary
	TxnActive    float64

	// Network
	NetworkBytesSent     int64
	NetworkBytesReceived int64
	GRPCErrors           int64
	PeerConnections      float64

	// Cluster
	ClusterSize    float64
	NodesSuspected float64
	NodesDead      float64

	// System
	GoroutineCount   float64
	MemoryUsageBytes float64
	FDCount          float64
}

// PrometheusFormat returns Prometheus text format for scraping
func (s MetricsSnapshot) PrometheusFormat() string {
	labels := fmt.Sprintf(`node_id="%s"`, s.NodeID)
	out := ""

	metric := func(name, help, mtype string, value float64) {
		out += fmt.Sprintf("# HELP %s %s\n", name, help)
		out += fmt.Sprintf("# TYPE %s %s\n", name, mtype)
		out += fmt.Sprintf("%s{%s} %g\n", name, labels, value)
	}

	histogramMetric := func(name, help string, summary HistogramSummary) {
		out += fmt.Sprintf("# HELP %s %s\n", name, help)
		out += fmt.Sprintf("# TYPE %s histogram\n", name)
		out += fmt.Sprintf("%s_p50{%s} %g\n", name, labels, summary.P50)
		out += fmt.Sprintf("%s_p95{%s} %g\n", name, labels, summary.P95)
		out += fmt.Sprintf("%s_p99{%s} %g\n", name, labels, summary.P99)
		out += fmt.Sprintf("%s_count{%s} %d\n", name, labels, summary.Count)
		out += fmt.Sprintf("%s_sum{%s} %g\n", name, labels, summary.Sum)
	}

	// Raft
	metric("hermes_raft_leader_changes_total", "Leader changes", "counter",
		float64(s.RaftLeaderChanges))
	metric("hermes_raft_current_term", "Current term", "gauge", s.RaftCurrentTerm)
	isLeader := float64(0)
	if s.RaftIsLeader {
		isLeader = 1
	}
	metric("hermes_raft_is_leader", "Is leader", "gauge", isLeader)
	histogramMetric("hermes_raft_commit_duration_seconds",
		"Raft commit latency", s.RaftCommitLatency)

	// WAL
	histogramMetric("hermes_wal_write_duration_seconds",
		"WAL write latency", s.WALWriteLatency)
	histogramMetric("hermes_wal_fsync_duration_seconds",
		"WAL fsync latency", s.WALFsyncLatency)

	// Requests
	histogramMetric("hermes_request_duration_seconds",
		"Request latency", s.RequestDuration)
	metric("hermes_requests_total", "Total requests", "counter",
		float64(s.RequestsTotal))

	// Transactions
	metric("hermes_txn_total", "Total transactions", "counter",
		float64(s.TxnTotal))
	metric("hermes_txn_conflicts_total", "Transaction conflicts", "counter",
		float64(s.TxnConflicts))

	// System
	metric("hermes_goroutines", "Goroutine count", "gauge", s.GoroutineCount)
	metric("hermes_memory_usage_bytes", "Memory usage", "gauge", s.MemoryUsageBytes)

	return out
}
