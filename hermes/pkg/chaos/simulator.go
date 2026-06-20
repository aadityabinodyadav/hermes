// pkg/chaos/simulator.go
package chaos

// DeterministicSimulator implements deterministic simulation testing
//
// Inspired by FoundationDB's simulation framework
//
// Key properties:
//   1. SINGLE GOROUTINE: all events happen in one goroutine
//   2. FAKE TIME: time.Now() returns simulator's clock
//   3. FAKE NETWORK: messages go through simulator
//   4. REPRODUCIBLE: same seed → same execution
//   5. FAST: no real I/O, no real sleeping

import (
	"container/heap"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// SimTime represents simulated time (nanoseconds since epoch)
type SimTime int64

// SimEvent is something that happens at a specific simulated time
type SimEvent struct {
	// When this event should fire
	At SimTime

	// What to do (callback)
	Action func()

	// Description for debugging
	Description string

	// Priority for tie-breaking (lower = higher priority)
	Priority int

	// Index in the heap
	index int
}

// eventHeap implements heap.Interface for SimEvents
type eventHeap []*SimEvent

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].At != h[j].At {
		return h[i].At < h[j].At
	}
	return h[i].Priority < h[j].Priority
}
func (h eventHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *eventHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*SimEvent)
	item.index = n
	*h = append(*h, item)
}
func (h *eventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}

// SimulatedMessage is a network message in the simulator
type SimulatedMessage struct {
	From    string
	To      string
	Content interface{}
	Delay   SimTime
}

// DeterministicSimulator controls simulated execution
type DeterministicSimulator struct {
	mu sync.Mutex

	// Random source (seeded for determinism)
	rng  *rand.Rand
	seed int64

	// Simulated clock
	currentTime SimTime

	// Event queue
	events eventHeap

	// Network simulator
	network *SimulatedNetwork

	// Crashed nodes
	crashed map[string]bool

	// Statistics
	eventsProcessed int64
	maxTime         SimTime

	// Scenario tracking
	scenarios []TestScenario
}

// SimulatedNetwork handles message delivery in simulation
type SimulatedNetwork struct {
	sim        *DeterministicSimulator
	partitions map[string]map[string]bool // from → to → partitioned
	dropRate   float64
	minDelay   SimTime
	maxDelay   SimTime

	// Message handlers per node
	handlers map[string]func(msg SimulatedMessage)
}

// NewDeterministicSimulator creates a simulator with a specific seed
func NewDeterministicSimulator(seed int64) *DeterministicSimulator {
	sim := &DeterministicSimulator{
		rng:     rand.New(rand.NewSource(seed)),
		seed:    seed,
		crashed: make(map[string]bool),
	}

	sim.network = &SimulatedNetwork{
		sim:        sim,
		partitions: make(map[string]map[string]bool),
		handlers:   make(map[string]func(SimulatedMessage)),
	}

	heap.Init(&sim.events)

	fmt.Printf("[Sim] Created with seed=%d\n", seed)
	return sim
}

// Now returns the current simulated time
func (s *DeterministicSimulator) Now() time.Time {
	return time.Unix(0, int64(s.currentTime))
}

// Schedule adds an event at a specific simulated time
func (s *DeterministicSimulator) Schedule(at SimTime, description string, action func()) {
	event := &SimEvent{
		At:          at,
		Action:      action,
		Description: description,
	}
	heap.Push(&s.events, event)
}

// After schedules an event relative to current time
func (s *DeterministicSimulator) After(delay time.Duration, description string, action func()) {
	at := s.currentTime + SimTime(delay.Nanoseconds())
	s.Schedule(at, description, action)
}

// Run executes the simulation until maxTime or no more events
func (s *DeterministicSimulator) Run(maxTime time.Duration) {
	s.maxTime = SimTime(maxTime.Nanoseconds())

	fmt.Printf("[Sim] Running until T+%v\n", maxTime)

	for s.events.Len() > 0 {
		event := heap.Pop(&s.events).(*SimEvent)

		// Stop if past max time
		if event.At > s.maxTime {
			break
		}

		// Advance clock to event time
		s.currentTime = event.At
		s.eventsProcessed++

		// Execute event
		event.Action()
	}

	fmt.Printf("[Sim] Finished: processed %d events, simulated %v\n",
		s.eventsProcessed, time.Duration(s.currentTime))
}

// ─────────────────────────────────────────────────────────────────────────────
// NETWORK SIMULATION
// ─────────────────────────────────────────────────────────────────────────────

// RegisterNode registers a node's message handler
func (n *SimulatedNetwork) RegisterNode(nodeID string, handler func(SimulatedMessage)) {
	n.handlers[nodeID] = handler
}

// Send delivers a message with simulated delay and potential failure
func (n *SimulatedNetwork) Send(from, to string, content interface{}) {
	// Check partition
	if n.IsPartitioned(from, to) {
		return // dropped silently
	}

	// Check drop rate
	if n.dropRate > 0 && n.sim.rng.Float64() < n.dropRate {
		return // dropped
	}

	// Calculate delivery delay
	delay := n.minDelay
	if n.maxDelay > n.minDelay {
		delay += SimTime(n.sim.rng.Int63n(int64(n.maxDelay - n.minDelay)))
	}

	deliveryTime := n.sim.currentTime + delay
	msg := SimulatedMessage{
		From:    from,
		To:      to,
		Content: content,
	}

	// Schedule delivery
	n.sim.Schedule(deliveryTime,
		fmt.Sprintf("deliver %s→%s", from, to),
		func() {
			if n.sim.crashed[to] {
				return // destination crashed, drop
			}
			if handler, exists := n.handlers[to]; exists {
				handler(msg)
			}
		})
}

// Partition creates a network partition
func (n *SimulatedNetwork) Partition(from, to string) {
	if n.partitions[from] == nil {
		n.partitions[from] = make(map[string]bool)
	}
	n.partitions[from][to] = true
}

// Heal removes a partition
func (n *SimulatedNetwork) Heal(from, to string) {
	if n.partitions[from] != nil {
		delete(n.partitions[from], to)
	}
}

// IsPartitioned checks if a link is partitioned
func (n *SimulatedNetwork) IsPartitioned(from, to string) bool {
	return n.partitions[from][to]
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST SCENARIO FRAMEWORK
// ─────────────────────────────────────────────────────────────────────────────

// TestScenario describes one chaos test scenario
type TestScenario struct {
	Name        string
	Description string
	Seed        int64
	Duration    time.Duration
	Setup       func(sim *DeterministicSimulator)
	Verify      func(sim *DeterministicSimulator) []string // returns violations
}

// TestResult is the result of running a scenario
type TestResult struct {
	Scenario   string
	Passed     bool
	Duration   time.Duration
	Violations []string
	Events     int64
}

// ScenarioRunner runs test scenarios
type ScenarioRunner struct {
	scenarios []TestScenario
	results   []TestResult
}

// NewScenarioRunner creates a new runner
func NewScenarioRunner() *ScenarioRunner {
	return &ScenarioRunner{}
}

// Add adds a scenario to the runner
func (r *ScenarioRunner) Add(scenario TestScenario) {
	r.scenarios = append(r.scenarios, scenario)
}

// RunAll runs all registered scenarios
func (r *ScenarioRunner) RunAll() []TestResult {
	for _, scenario := range r.scenarios {
		result := r.runScenario(scenario)
		r.results = append(r.results, result)
	}
	return r.results
}

// runScenario executes one test scenario
func (r *ScenarioRunner) runScenario(scenario TestScenario) TestResult {
	start := time.Now()

	fmt.Printf("\n📋 Running scenario: %s\n", scenario.Name)
	fmt.Printf("   %s\n", scenario.Description)
	fmt.Println()

	sim := NewDeterministicSimulator(scenario.Seed)

	// Setup the scenario
	scenario.Setup(sim)

	// Run the simulation
	sim.Run(scenario.Duration)

	// Verify properties
	violations := scenario.Verify(sim)

	elapsed := time.Since(start)
	passed := len(violations) == 0

	if passed {
		fmt.Printf("   ✅ PASSED (%v, %d events)\n",
			elapsed, sim.eventsProcessed)
	} else {
		fmt.Printf("   ❌ FAILED (%v, %d events)\n",
			elapsed, sim.eventsProcessed)
		for _, v := range violations {
			fmt.Printf("      Violation: %s\n", v)
		}
	}

	return TestResult{
		Scenario:   scenario.Name,
		Passed:     passed,
		Duration:   elapsed,
		Violations: violations,
		Events:     sim.eventsProcessed,
	}
}

// Summary prints a summary of all test results
func (r *ScenarioRunner) Summary() {
	passed := 0
	failed := 0

	for _, result := range r.results {
		if result.Passed {
			passed++
		} else {
			failed++
		}
	}

	fmt.Println()
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("SCENARIO RESULTS: %d passed, %d failed\n", passed, failed)
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	for _, result := range r.results {
		icon := "✅"
		if !result.Passed {
			icon = "❌"
		}
		fmt.Printf("%s %-40s (%d events, %v)\n",
			icon, result.Scenario, result.Events, result.Duration)
		for _, v := range result.Violations {
			fmt.Printf("     ⚠️  %s\n", v)
		}
	}
}
