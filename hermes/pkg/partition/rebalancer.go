// pkg/partition/rebalancer.go
package partition

// Rebalancer monitors shard health and triggers splits/merges/moves
//
// WHY REBALANCING IS HARD:
//   Moving data while serving traffic is like changing a tire while driving.
//   You need to:
//     1. Copy data to new shard (could take minutes for large shards)
//     2. Continue serving reads/writes to OLD location
//     3. Atomically switch to NEW location
//     4. Old location stops serving
//
//   If anything goes wrong: don't lose data, don't serve stale data
//
// SPLIT TRIGGERS:
//   - Shard size > maxShardSize (default 256MB)
//   - Shard request rate > maxShardRPS (default 10K/sec)
//   - Manual override from operator
//
// MERGE TRIGGERS:
//   - Shard size < minShardSize (default 10MB)
//   - Adjacent shard also small
//   - No recent splits (avoid thrash)
//
// MOVE TRIGGERS:
//   - Node is overloaded (too many shards)
//   - Node has too little data (storage imbalance)
//   - Node join/leave (rebalance to new set of nodes)

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ShardStats contains runtime statistics for a shard
type ShardStats struct {
	ShardID    ShardID
	SizeBytes  int64
	RPS        float64 // requests per second
	LeaderNode string
	LastSplit  time.Time
	LastMerge  time.Time
}

// RebalancerConfig configures the rebalancer
type RebalancerConfig struct {
	// MaxShardSize triggers split when exceeded
	MaxShardSize int64

	// MinShardSize triggers merge when below
	MinShardSize int64

	// MaxShardRPS triggers split when exceeded
	MaxShardRPS float64

	// CheckInterval how often to evaluate shard health
	CheckInterval time.Duration

	// SplitCooldown prevents rapid split-merge oscillation
	SplitCooldown time.Duration
}

// DefaultRebalancerConfig returns sensible defaults
func DefaultRebalancerConfig() RebalancerConfig {
	return RebalancerConfig{
		MaxShardSize:  256 * 1024 * 1024, // 256MB
		MinShardSize:  10 * 1024 * 1024,  // 10MB
		MaxShardRPS:   10000,
		CheckInterval: 30 * time.Second,
		SplitCooldown: 5 * time.Minute,
	}
}

// RebalanceAction describes what the rebalancer wants to do
type RebalanceAction struct {
	Type       RebalanceActionType
	ShardID    ShardID
	SplitKey   string  // for SPLIT
	TargetNode string  // for MOVE
	MergePeer  ShardID // for MERGE
	Reason     string
}

type RebalanceActionType uint8

const (
	ActionSplit RebalanceActionType = 0
	ActionMerge RebalanceActionType = 1
	ActionMove  RebalanceActionType = 2
)

func (a RebalanceActionType) String() string {
	switch a {
	case ActionSplit:
		return "SPLIT"
	case ActionMerge:
		return "MERGE"
	case ActionMove:
		return "MOVE"
	}
	return "UNKNOWN"
}

// Rebalancer monitors and rebalances shards
type Rebalancer struct {
	config   RebalancerConfig
	shardMap *ShardMap

	mu         sync.Mutex
	shardStats map[ShardID]*ShardStats

	// Pending actions queue
	pendingActions []RebalanceAction

	// Running state
	ctx    context.Context
	cancel context.CancelFunc
}

// NewRebalancer creates a new rebalancer
func NewRebalancer(shardMap *ShardMap, config RebalancerConfig) *Rebalancer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Rebalancer{
		config:     config,
		shardMap:   shardMap,
		shardStats: make(map[ShardID]*ShardStats),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start begins background rebalancing
func (r *Rebalancer) Start() {
	go r.run()
}

// Stop stops the rebalancer
func (r *Rebalancer) Stop() {
	r.cancel()
}

// UpdateStats updates statistics for a shard
// Called by the storage engine periodically
func (r *Rebalancer) UpdateStats(stats ShardStats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shardStats[stats.ShardID] = &stats
}

// run is the background rebalancing loop
func (r *Rebalancer) run() {
	ticker := time.NewTicker(r.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.evaluate()
		}
	}
}

// evaluate checks all shards and generates rebalancing actions
func (r *Rebalancer) evaluate() {
	r.mu.Lock()
	defer r.mu.Unlock()

	allShards := r.shardMap.All()
	var actions []RebalanceAction

	for _, shard := range allShards {
		stats, ok := r.shardStats[shard.ShardID]
		if !ok {
			continue
		}

		// Check if shard should be SPLIT
		if r.shouldSplit(shard, stats) {
			splitKey := r.findSplitKey(shard, stats)
			if splitKey != "" {
				actions = append(actions, RebalanceAction{
					Type:     ActionSplit,
					ShardID:  shard.ShardID,
					SplitKey: splitKey,
					Reason: fmt.Sprintf("size=%dMB > max=%dMB OR rps=%.0f > max=%.0f",
						stats.SizeBytes/1024/1024,
						r.config.MaxShardSize/1024/1024,
						stats.RPS, r.config.MaxShardRPS),
				})
			}
		}
	}

	// Check for MERGE opportunities
	for i := 0; i < len(allShards)-1; i++ {
		s1 := allShards[i]
		s2 := allShards[i+1]

		stats1 := r.shardStats[s1.ShardID]
		stats2 := r.shardStats[s2.ShardID]

		if stats1 != nil && stats2 != nil &&
			r.shouldMerge(s1, stats1) && r.shouldMerge(s2, stats2) {
			// Check total size after merge
			totalSize := stats1.SizeBytes + stats2.SizeBytes
			if totalSize < r.config.MaxShardSize {
				actions = append(actions, RebalanceAction{
					Type:      ActionMerge,
					ShardID:   s1.ShardID,
					MergePeer: s2.ShardID,
					Reason: fmt.Sprintf("both shards small: %dMB + %dMB",
						stats1.SizeBytes/1024/1024,
						stats2.SizeBytes/1024/1024),
				})
			}
		}
	}

	if len(actions) > 0 {
		fmt.Printf("Rebalancer: found %d actions:\n", len(actions))
		for _, a := range actions {
			fmt.Printf("  %s shard=%d: %s\n", a.Type, a.ShardID, a.Reason)
		}
		r.pendingActions = append(r.pendingActions, actions...)
	}
}

func (r *Rebalancer) shouldSplit(shard *ShardDescriptor, stats *ShardStats) bool {
	if stats == nil {
		return false
	}

	// Respect cooldown period
	if time.Since(stats.LastSplit) < r.config.SplitCooldown {
		return false
	}

	return stats.SizeBytes > r.config.MaxShardSize ||
		stats.RPS > r.config.MaxShardRPS
}

func (r *Rebalancer) shouldMerge(shard *ShardDescriptor, stats *ShardStats) bool {
	if stats == nil {
		return false
	}
	return stats.SizeBytes < r.config.MinShardSize
}

// findSplitKey finds the best key to split a shard at
// In production: use actual key frequency distribution
// Here: use the midpoint of the key range
func (r *Rebalancer) findSplitKey(shard *ShardDescriptor, stats *ShardStats) string {
	// Simple: midpoint of start and end keys
	if shard.EndKey == "" {
		// Unbounded end: can't easily compute midpoint
		return ""
	}

	// Find approximate midpoint string
	// (This is simplified; production uses actual data distribution)
	if len(shard.StartKey) == 0 && len(shard.EndKey) > 0 {
		return string([]byte{shard.EndKey[0] / 2})
	}

	if len(shard.StartKey) > 0 && len(shard.EndKey) > 0 {
		// Midpoint between first characters
		mid := (int(shard.StartKey[0]) + int(shard.EndKey[0])) / 2
		return string([]byte{byte(mid)})
	}

	return ""
}

// PendingActions returns and clears pending rebalance actions
func (r *Rebalancer) PendingActions() []RebalanceAction {
	r.mu.Lock()
	defer r.mu.Unlock()
	actions := r.pendingActions
	r.pendingActions = nil
	return actions
}
