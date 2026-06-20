package transport



import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type NetworkRule struct {
	DropRate float64

	MinDelay time.Duration
	MaxDelay time.Duration

	DuplicateRate float64

	Partition bool

	CorruptRate float64
}

type SimulatorStats struct {
	mu          sync.Mutex
	Sent        int64
	Dropped     int64
	Delayed     int64
	Duplicated  int64
	Corrupted   int64
	Partitioned int64
}

func (s *SimulatorStats) Record(action string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Sent++
	switch action {
	case "drop":
		s.Dropped++
	case "delay":
		s.Delayed++
	case "duplicate":
		s.Duplicated++
	case "corrupt":
		s.Corrupted++
	case "partition":
		s.Partitioned++
	}
}

func (s *SimulatorStats) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fmt.Sprintf(
		"Sent=%d Dropped=%d Delayed=%d Duplicated=%d Partitioned=%d",
		s.Sent, s.Dropped, s.Delayed, s.Duplicated, s.Partitioned,
	)
}

type NetworkSimulator struct {
	mu    sync.RWMutex
	rules map[string]map[string]*NetworkRule
	stats *SimulatorStats
	rng   *rand.Rand
}

func NewNetworkSimulator() *NetworkSimulator {
	return &NetworkSimulator{
		rules: make(map[string]map[string]*NetworkRule),
		stats: &SimulatorStats{},
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *NetworkSimulator) SetRule(from, to string, rule NetworkRule) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.rules[from] == nil {
		s.rules[from] = make(map[string]*NetworkRule)
	}
	s.rules[from][to] = &rule

	if rule.Partition {
		fmt.Printf("🔴 PARTITION: %s → %s (all messages dropped)\n", from, to)
	} else {
		fmt.Printf("⚙️  RULE SET: %s → %s (drop=%.0f%%, delay=%v-%v)\n",
			from, to, rule.DropRate*100, rule.MinDelay, rule.MaxDelay)
	}
}

func (s *NetworkSimulator) Partition(groupA, groupB []string) {
	fmt.Printf("\n🔴🔴🔴 NETWORK PARTITION 🔴🔴🔴\n")
	fmt.Printf("Group A: %v\n", groupA)
	fmt.Printf("Group B: %v\n", groupB)
	fmt.Printf("All communication between groups is blocked\n\n")

	for _, a := range groupA {
		for _, b := range groupB {
			s.SetRule(a, b, NetworkRule{Partition: true})
			s.SetRule(b, a, NetworkRule{Partition: true})
		}
	}
}

func (s *NetworkSimulator) HealPartition(groupA, groupB []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fmt.Printf("\n🟢🟢🟢 PARTITION HEALED 🟢🟢🟢\n")
	fmt.Printf("Group A: %v\n", groupA)
	fmt.Printf("Group B: %v\n", groupB)
	fmt.Printf("Communication restored\n\n")

	for _, a := range groupA {
		for _, b := range groupB {
			delete(s.rules[a], b)
			delete(s.rules[b], a)
		}
	}
}

func (s *NetworkSimulator) SetPacketLoss(from, to string, dropRate float64) {
	s.SetRule(from, to, NetworkRule{DropRate: dropRate})
}

func (s *NetworkSimulator) SetLatency(from, to string, min, max time.Duration) {
	s.SetRule(from, to, NetworkRule{MinDelay: min, MaxDelay: max})
}

func (s *NetworkSimulator) ClearAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules = make(map[string]map[string]*NetworkRule)
	fmt.Println("🟢 Network restored to perfect conditions")
}

func (s *NetworkSimulator) ShouldDeliver(from, to string) bool {
	s.mu.RLock()
	rule := s.getRule(from, to)
	s.mu.RUnlock()

	if rule == nil {
		// No rule = perfect network
		s.stats.Record("ok")
		return true
	}

	if rule.Partition {
		s.stats.Record("partition")
		return false
	}

	if rule.DropRate > 0 && s.rng.Float64() < rule.DropRate {
		s.stats.Record("drop")
		return false
	}

	if rule.MinDelay > 0 || rule.MaxDelay > 0 {
		delay := rule.MinDelay
		if rule.MaxDelay > rule.MinDelay {
			delay += time.Duration(s.rng.Int63n(int64(rule.MaxDelay - rule.MinDelay)))
		}
		time.Sleep(delay)
		s.stats.Record("delay")
	}

	s.stats.Record("ok")
	return true
}

func (s *NetworkSimulator) getRule(from, to string) *NetworkRule {
	if fromRules, ok := s.rules[from]; ok {
		if rule, ok := fromRules[to]; ok {
			return rule
		}
	}
	return nil
}

func (s *NetworkSimulator) Stats() string {
	return s.stats.String()
}

func SimulatorInterceptor(sim *NetworkSimulator, fromNodeID string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		toNodeID := cc.Target()

		if !sim.ShouldDeliver(fromNodeID, toNodeID) {
			return status.Errorf(codes.Unavailable,
				"simulated network failure: %s → %s", fromNodeID, toNodeID)
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func ScenarioSlowNetwork(sim *NetworkSimulator, nodes []string) {
	fmt.Println("📡 SCENARIO: Slow Network (high latency)")
	for _, from := range nodes {
		for _, to := range nodes {
			if from != to {
				sim.SetRule(from, to, NetworkRule{
					MinDelay: 100 * time.Millisecond,
					MaxDelay: 300 * time.Millisecond,
				})
			}
		}
	}
}
func ScenarioFlakyNetwork(sim *NetworkSimulator, nodes []string, dropRate float64) {
	fmt.Printf("📡 SCENARIO: Flaky Network (%.0f%% packet loss)\n", dropRate*100)
	for _, from := range nodes {
		for _, to := range nodes {
			if from != to {
				sim.SetRule(from, to, NetworkRule{
					DropRate: dropRate,
				})
			}
		}
	}
}

func ScenarioAsymmetricPartition(sim *NetworkSimulator, from, to string) {
	fmt.Printf("📡 SCENARIO: Asymmetric Partition %s→%s broken (but %s→%s works)\n",
		to, from, from, to)

	sim.SetRule(to, from, NetworkRule{Partition: true})
}
