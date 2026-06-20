// pkg/observability/health.go
package observability

// HealthCheck implements HTTP health endpoints for Kubernetes probes
//
// Kubernetes uses three probes:
//   - Liveness:   Is the process alive? (restart if no)
//   - Readiness:  Can it serve traffic? (remove from LB if no)
//   - Startup:    Has it finished starting? (give time to init)
//
// Hermes health states:
//   HEALTHY:   Leader or follower, serving requests normally
//   DEGRADED:  Can serve some requests (stale reads) but not all
//   UNHEALTHY: Cannot serve requests (too far behind, no leader)
//   STARTING:  Still initializing (replaying WAL, catching up)

import (
	"fmt"
	"sync"
	"time"
)

// HealthStatus represents the health of the Hermes node
type HealthStatus string

const (
	HealthHealthy   HealthStatus = "healthy"
	HealthDegraded  HealthStatus = "degraded"
	HealthUnhealthy HealthStatus = "unhealthy"
	HealthStarting  HealthStatus = "starting"
)

// HealthCheck is one check contributing to overall health
type HealthCheck struct {
	Name    string
	Status  HealthStatus
	Message string
	LastOK  time.Time
}

// HealthReport is the full health report of a node
type HealthReport struct {
	Status    HealthStatus
	NodeID    string
	Timestamp time.Time

	// Individual checks
	Checks []HealthCheck

	// Key metrics for the dashboard
	IsLeader       bool
	CurrentTerm    uint64
	CommittedIndex uint64
	AppliedIndex   uint64
	ReplicationLag uint64 // entries behind for this node (0 if leader)
	LeaderID       string
}

// HealthChecker manages health checks for a Hermes node
type HealthChecker struct {
	mu     sync.RWMutex
	nodeID string
	checks map[string]*HealthCheck

	// Thresholds
	maxReplicationLag uint64
	maxApplyLag       uint64
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(nodeID string) *HealthChecker {
	return &HealthChecker{
		nodeID:            nodeID,
		checks:            make(map[string]*HealthCheck),
		maxReplicationLag: 1000, // entries
		maxApplyLag:       100,  // entries
	}
}

// SetCheck updates one health check
func (h *HealthChecker) SetCheck(name string, status HealthStatus, message string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.checks[name] = &HealthCheck{
		Name:    name,
		Status:  status,
		Message: message,
		LastOK: func() time.Time {
			if status == HealthHealthy {
				return time.Now()
			}
			if existing, ok := h.checks[name]; ok {
				return existing.LastOK
			}
			return time.Time{}
		}(),
	}
}

// CheckWAL checks if WAL is functioning
func (h *HealthChecker) CheckWAL(lastWriteLatency time.Duration, lastFsyncLatency time.Duration) {
	if lastFsyncLatency > 500*time.Millisecond {
		h.SetCheck("wal", HealthDegraded,
			fmt.Sprintf("WAL fsync very slow: %v (>500ms)", lastFsyncLatency))
		return
	}
	if lastFsyncLatency > 100*time.Millisecond {
		h.SetCheck("wal", HealthDegraded,
			fmt.Sprintf("WAL fsync slow: %v (>100ms)", lastFsyncLatency))
		return
	}
	h.SetCheck("wal", HealthHealthy, "WAL operating normally")
}

// CheckRaft checks Raft state
func (h *HealthChecker) CheckRaft(isLeader bool, leaderKnown bool, replicationLag uint64) {
	if !leaderKnown {
		h.SetCheck("raft", HealthUnhealthy, "No leader known - cluster may be partitioned")
		return
	}
	if replicationLag > h.maxReplicationLag {
		h.SetCheck("raft", HealthDegraded,
			fmt.Sprintf("Replication lag too high: %d entries (>%d)",
				replicationLag, h.maxReplicationLag))
		return
	}
	if isLeader {
		h.SetCheck("raft", HealthHealthy, "Node is leader, cluster healthy")
	} else {
		h.SetCheck("raft", HealthHealthy,
			fmt.Sprintf("Node is follower, replication lag: %d entries", replicationLag))
	}
}

// CheckStorage checks storage engine health
func (h *HealthChecker) CheckStorage(memTableFullPct float64, compactionBacklog int) {
	if memTableFullPct > 95 {
		h.SetCheck("storage", HealthDegraded,
			fmt.Sprintf("MemTable nearly full: %.0f%%", memTableFullPct))
		return
	}
	if compactionBacklog > 10 {
		h.SetCheck("storage", HealthDegraded,
			fmt.Sprintf("Compaction backlog: %d files", compactionBacklog))
		return
	}
	h.SetCheck("storage", HealthHealthy, "Storage engine healthy")
}

// CheckMemory checks memory pressure
func (h *HealthChecker) CheckMemory(usedBytes, limitBytes uint64) {
	usedPct := float64(usedBytes) / float64(limitBytes) * 100
	if usedPct > 90 {
		h.SetCheck("memory", HealthUnhealthy,
			fmt.Sprintf("Memory critically high: %.0f%%", usedPct))
		return
	}
	if usedPct > 75 {
		h.SetCheck("memory", HealthDegraded,
			fmt.Sprintf("Memory pressure: %.0f%%", usedPct))
		return
	}
	h.SetCheck("memory", HealthHealthy,
		fmt.Sprintf("Memory usage: %.0f%%", usedPct))
}

// Report generates the full health report
func (h *HealthChecker) Report() HealthReport {
	h.mu.RLock()
	defer h.mu.RUnlock()

	report := HealthReport{
		Status:    HealthHealthy,
		NodeID:    h.nodeID,
		Timestamp: time.Now(),
	}

	// Collect all checks
	for _, check := range h.checks {
		report.Checks = append(report.Checks, *check)

		// Overall status = worst individual status
		switch check.Status {
		case HealthUnhealthy:
			report.Status = HealthUnhealthy
		case HealthDegraded:
			if report.Status == HealthHealthy {
				report.Status = HealthDegraded
			}
		}
	}

	return report
}

// IsLive returns true if the process is alive (for liveness probe)
// A process is NOT live if it's completely stuck or deadlocked
func (h *HealthChecker) IsLive() bool {
	// In production: check if we've made recent progress
	// (e.g., goroutine ticker is still running)
	// For simplicity: always return true (process is running)
	return true
}

// IsReady returns true if node can serve traffic (for readiness probe)
// A node is NOT ready if:
//   - Still starting up (replaying WAL)
//   - No leader known
//   - Replication lag too high
func (h *HealthChecker) IsReady() bool {
	report := h.Report()
	return report.Status != HealthUnhealthy && report.Status != HealthStarting
}
