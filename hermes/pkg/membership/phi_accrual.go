// pkg/membership/phi_accrual.go
package membership

// PhiAccrualDetector implements the Phi Accrual Failure Detector
// Originally proposed by Hayashibara et al. (2004)
// Used by: Apache Cassandra, Akka
//
// Key insight: instead of binary alive/dead,
// output a continuous suspicion value φ that grows over time
// without receiving heartbeats.
//
// The caller decides what φ threshold to use based on
// their false positive tolerance.
//
//   φ = 1  → 10% chance it's a false positive
//   φ = 5  → 0.1% chance it's a false positive
//   φ = 8  → 0.003% chance it's a false positive (Cassandra default)
//   φ = 10 → 0.0001% chance it's a false positive

import (
	"math"
	"sync"
	"time"
)

const (
	// windowSize is how many heartbeat intervals to track
	// More = better statistical model, more memory
	windowSize = 200

	// minSamples before we start making decisions
	// Too few samples = unreliable statistics
	minSamples = 10

	// defaultMean is assumed heartbeat interval before we have data
	defaultMeanMs = 500.0 // 500ms

	// scalingFactor converts from normal distribution CDF to phi
	// Makes phi=1 correspond to ~10% false positive rate
	scalingFactor = 1 / math.Ln10
)

// HeartbeatWindow is a sliding window of heartbeat inter-arrival times
type HeartbeatWindow struct {
	mu            sync.Mutex
	intervals     []float64 // inter-arrival times in milliseconds
	writeIdx      int       // next write position (circular buffer)
	count         int       // how many valid samples
	lastHeartbeat time.Time
}

// newHeartbeatWindow creates a new heartbeat tracking window
func newHeartbeatWindow() *HeartbeatWindow {
	return &HeartbeatWindow{
		intervals: make([]float64, windowSize),
	}
}

// Heartbeat records a new heartbeat arrival
// Call this every time you receive a message from the peer
func (w *HeartbeatWindow) Heartbeat(now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.lastHeartbeat.IsZero() {
		// Record inter-arrival interval
		interval := now.Sub(w.lastHeartbeat).Seconds() * 1000 // convert to ms
		w.intervals[w.writeIdx] = interval
		w.writeIdx = (w.writeIdx + 1) % windowSize
		if w.count < windowSize {
			w.count++
		}
	}

	w.lastHeartbeat = now
}

// Phi returns the current suspicion level for this heartbeat source
// Higher φ = more suspicious
// Returns 0 if no data yet (benefit of the doubt)
func (w *HeartbeatWindow) Phi(now time.Time) float64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.lastHeartbeat.IsZero() {
		return 0 // never seen this node, can't judge
	}

	// Time since last heartbeat
	timeSinceLast := now.Sub(w.lastHeartbeat).Seconds() * 1000 // ms

	if timeSinceLast <= 0 {
		return 0
	}

	// Calculate statistics from window
	mean, variance := w.statistics()

	if mean <= 0 {
		mean = defaultMeanMs
	}

	// P(inter-arrival > t) using normal distribution
	// P_later = 1 - Φ((t - μ) / σ)  [Φ = normal CDF]
	// But we use exponential distribution approximation
	// (heartbeat intervals are roughly exponential)
	//
	// For exponential distribution with mean μ:
	// P(X > t) = e^(-t/μ)
	// φ = -log₁₀(P(X > t)) = t/(μ × ln(10))
	//
	// With variance adjustment for better accuracy:
	sigma := math.Sqrt(variance)
	if sigma < 1 {
		sigma = 1 // minimum sigma to prevent degenerate cases
	}

	// Normal distribution CDF approximation
	y := (timeSinceLast - mean) / sigma
	pLater := phi_cdf(y)

	if pLater <= 0 {
		return math.Inf(1) // infinite suspicion (definitely dead)
	}

	return -math.Log10(pLater)
}

// statistics calculates mean and variance of the heartbeat window
// MUST be called with lock held
func (w *HeartbeatWindow) statistics() (mean, variance float64) {
	if w.count == 0 {
		return defaultMeanMs, defaultMeanMs * defaultMeanMs
	}

	// Calculate mean
	sum := 0.0
	for i := 0; i < w.count; i++ {
		sum += w.intervals[i]
	}
	mean = sum / float64(w.count)

	if w.count < minSamples {
		// Not enough samples — use conservative estimate
		// Assume high variance to avoid false positives
		return mean, mean * mean
	}

	// Calculate variance
	varSum := 0.0
	for i := 0; i < w.count; i++ {
		diff := w.intervals[i] - mean
		varSum += diff * diff
	}
	variance = varSum / float64(w.count)

	return mean, variance
}

// phi_cdf approximates the upper tail probability of the normal distribution
// P(X > y) for standard normal X
// Uses the complementary error function (erfc) approximation
func phi_cdf(y float64) float64 {
	return math.Erfc(y/math.Sqrt2) / 2
}

// ─────────────────────────────────────────────────────────────────────────────

// PhiAccrualDetector manages phi accrual for multiple peers
type PhiAccrualDetector struct {
	mu        sync.RWMutex
	peers     map[string]*HeartbeatWindow
	threshold float64 // φ above this → suspect

	// Callbacks
	onSuspect func(nodeID string, phi float64)
	onAlive   func(nodeID string, phi float64)
}

// NewPhiAccrualDetector creates a new detector
func NewPhiAccrualDetector(threshold float64) *PhiAccrualDetector {
	return &PhiAccrualDetector{
		peers:     make(map[string]*HeartbeatWindow),
		threshold: threshold,
	}
}

// OnSuspect sets the callback for when a node becomes suspected
func (d *PhiAccrualDetector) OnSuspect(fn func(nodeID string, phi float64)) {
	d.onSuspect = fn
}

// OnAlive sets the callback for when a suspected node comes back
func (d *PhiAccrualDetector) OnAlive(fn func(nodeID string, phi float64)) {
	d.onAlive = fn
}

// Heartbeat records a heartbeat from a peer
func (d *PhiAccrualDetector) Heartbeat(nodeID string, now time.Time) {
	d.mu.Lock()
	window, exists := d.peers[nodeID]
	if !exists {
		window = newHeartbeatWindow()
		d.peers[nodeID] = window
	}
	d.mu.Unlock()

	prevPhi := window.Phi(now)
	window.Heartbeat(now)
	newPhi := window.Phi(now)

	// Transition from suspected to alive
	if prevPhi >= d.threshold && newPhi < d.threshold {
		if d.onAlive != nil {
			d.onAlive(nodeID, newPhi)
		}
	}
}

// Phi returns current suspicion level for a node
func (d *PhiAccrualDetector) Phi(nodeID string) float64 {
	d.mu.RLock()
	window, exists := d.peers[nodeID]
	d.mu.RUnlock()

	if !exists {
		return 0
	}

	return window.Phi(time.Now())
}

// Check evaluates all peers and triggers callbacks for state changes
// Call this periodically (e.g., every 100ms)
func (d *PhiAccrualDetector) Check() map[string]float64 {
	d.mu.RLock()
	peers := make(map[string]*HeartbeatWindow, len(d.peers))
	for k, v := range d.peers {
		peers[k] = v
	}
	d.mu.RUnlock()

	now := time.Now()
	result := make(map[string]float64, len(peers))

	for nodeID, window := range peers {
		phi := window.Phi(now)
		result[nodeID] = phi

		if phi >= d.threshold && d.onSuspect != nil {
			d.onSuspect(nodeID, phi)
		}
	}

	return result
}

// RemovePeer removes tracking for a peer
func (d *PhiAccrualDetector) RemovePeer(nodeID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.peers, nodeID)
}

// AddPeer starts tracking a new peer
func (d *PhiAccrualDetector) AddPeer(nodeID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.peers[nodeID]; !exists {
		d.peers[nodeID] = newHeartbeatWindow()
	}
}
