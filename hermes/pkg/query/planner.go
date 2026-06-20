// pkg/query/planner.go
package query

// QueryPlanner breaks a query into a distributed execution plan
//
// OPERATOR PUSH-DOWN:
//   The key optimization in distributed query processing.
//   Instead of pulling all data to one node:
//   Push the computation to where the data lives.
//
//   Bad:  Shard → [all data] → Coordinator → filter → result
//   Good: Shard → [filter locally] → Coordinator → merge

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// QUERY TYPES
// ─────────────────────────────────────────────────────────────────────────────

// Query represents a distributed query
type Query struct {
	// Type of query
	Type QueryType

	// KeyPrefix: scan all keys with this prefix
	KeyPrefix string

	// KeyRange: scan keys in [StartKey, EndKey)
	StartKey string
	EndKey   string

	// Filter: only return entries matching this
	Filter func(key string, value []byte) bool

	// Aggregation to apply
	Aggregation AggregationType

	// Limit on results
	Limit int

	// Timeout
	Timeout time.Duration
}

type QueryType uint8

const (
	QueryScan      QueryType = 0 // Scan a key range
	QueryAggregate QueryType = 1 // Aggregate (COUNT, SUM, AVG, etc.)
	QueryTopK      QueryType = 2 // Get top K results
	QueryDistinct  QueryType = 3 // Get distinct values
)

type AggregationType uint8

const (
	AggNone  AggregationType = 0
	AggCount AggregationType = 1
	AggSum   AggregationType = 2
	AggAvg   AggregationType = 3
	AggMin   AggregationType = 4
	AggMax   AggregationType = 5
)

func (a AggregationType) String() string {
	switch a {
	case AggCount:
		return "COUNT"
	case AggSum:
		return "SUM"
	case AggAvg:
		return "AVG"
	case AggMin:
		return "MIN"
	case AggMax:
		return "MAX"
	}
	return "NONE"
}

// ─────────────────────────────────────────────────────────────────────────────
// EXECUTION PLAN
// ─────────────────────────────────────────────────────────────────────────────

// ShardQuery is the sub-query sent to one shard
type ShardQuery struct {
	ShardID  uint64
	NodeID   string // which node to send to
	StartKey string
	EndKey   string
	Filter   func(key string, value []byte) bool
	LocalAgg AggregationType // aggregation to do locally
	Limit    int
}

// ExecutionPlan describes how to execute a distributed query
type ExecutionPlan struct {
	Query      *Query
	ShardPlans []*ShardQuery   // one per affected shard
	MergeOp    AggregationType // how to merge shard results
	FinalLimit int
}

// ─────────────────────────────────────────────────────────────────────────────
// QUERY PLANNER
// ─────────────────────────────────────────────────────────────────────────────

// ShardInfo describes one shard for planning purposes
type ShardInfo struct {
	ShardID  uint64
	StartKey string
	EndKey   string
	LeaderID string
}

// QueryPlanner creates execution plans for distributed queries
type QueryPlanner struct {
	shards []ShardInfo
}

// NewQueryPlanner creates a planner with shard layout
func NewQueryPlanner(shards []ShardInfo) *QueryPlanner {
	return &QueryPlanner{shards: shards}
}

// Plan creates an execution plan for a query
func (p *QueryPlanner) Plan(q *Query) *ExecutionPlan {
	plan := &ExecutionPlan{
		Query:      q,
		MergeOp:    q.Aggregation,
		FinalLimit: q.Limit,
	}

	// Find affected shards (shard pruning)
	affected := p.pruneShards(q)

	// Create per-shard sub-queries with push-down
	for _, shard := range affected {
		shardQuery := &ShardQuery{
			ShardID:  shard.ShardID,
			NodeID:   shard.LeaderID,
			StartKey: maxStr(q.StartKey, shard.StartKey),
			EndKey:   minStr(q.EndKey, shard.EndKey),
			Filter:   q.Filter,
			Limit:    q.Limit, // push limit down too
		}

		// Push aggregation down to shard level
		// The shard can compute partial results locally
		switch q.Aggregation {
		case AggCount:
			shardQuery.LocalAgg = AggCount // count locally
		case AggSum:
			shardQuery.LocalAgg = AggSum // sum locally
		case AggAvg:
			// Can't push AVG directly — push COUNT and SUM instead
			// Coordinator computes AVG from partial results
			shardQuery.LocalAgg = AggSum // we'll also need count
		case AggMin:
			shardQuery.LocalAgg = AggMin // min of local values
		case AggMax:
			shardQuery.LocalAgg = AggMax // max of local values
		}

		plan.ShardPlans = append(plan.ShardPlans, shardQuery)
	}

	return plan
}

// pruneShards returns only shards that overlap with the query's key range
// This is a critical optimization: don't scan shards that can't have results
func (p *QueryPlanner) pruneShards(q *Query) []ShardInfo {
	var result []ShardInfo

	for _, shard := range p.shards {
		// Check if shard overlaps with query range
		if q.StartKey != "" && shard.EndKey != "" && q.StartKey >= shard.EndKey {
			continue // query starts after shard ends
		}
		if q.EndKey != "" && shard.StartKey != "" && q.EndKey <= shard.StartKey {
			continue // query ends before shard starts
		}
		if q.KeyPrefix != "" {
			// For prefix queries: shard must have keys that could start with prefix
			if shard.EndKey != "" && q.KeyPrefix > shard.EndKey {
				continue
			}
			if shard.StartKey != "" && q.KeyPrefix < shard.StartKey &&
				!strings.HasPrefix(shard.StartKey, q.KeyPrefix) {
				// Shard starts after prefix range
				if !strings.HasPrefix(q.KeyPrefix, shard.StartKey[:min(len(q.KeyPrefix), len(shard.StartKey))]) {
					continue
				}
			}
		}
		result = append(result, shard)
	}

	return result
}

func maxStr(a, b string) string {
	if a > b {
		return a
	}
	return b
}

func minStr(a, b string) string {
	if a == "" {
		return b
	}
	if b == "" {
		return a
	}
	if a < b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ─────────────────────────────────────────────────────────────────────────────
// QUERY EXECUTOR
// ─────────────────────────────────────────────────────────────────────────────

// ShardResult is the result from one shard
type ShardResult struct {
	ShardID uint64
	Entries []KeyValue
	Count   int64
	Sum     float64
	Min     float64
	Max     float64
	Err     error
}

// KeyValue is one key-value pair in a result
type KeyValue struct {
	Key   string
	Value []byte
}

// QueryResult is the final merged result
type QueryResult struct {
	Entries []KeyValue
	Count   int64
	Sum     float64
	Avg     float64
	Min     float64
	Max     float64

	// Metadata
	ShardsQueried int
	TotalTime     time.Duration
	PartialResult bool // true if some shards failed
}

// ScatterGather executes a query across multiple shards
// This is the SCATTER-GATHER pattern
type ScatterGather struct {
	planner  *QueryPlanner
	executor ShardExecutor
}

// ShardExecutor executes a sub-query on one shard
// In production: makes a gRPC call to the shard leader
type ShardExecutor interface {
	ExecuteShard(shardQuery *ShardQuery) (*ShardResult, error)
}

// NewScatterGather creates a scatter-gather executor
func NewScatterGather(planner *QueryPlanner, executor ShardExecutor) *ScatterGather {
	return &ScatterGather{
		planner:  planner,
		executor: executor,
	}
}

// Execute runs a query across all relevant shards
func (sg *ScatterGather) Execute(q *Query) (*QueryResult, error) {
	start := time.Now()

	// Create execution plan
	plan := sg.planner.Plan(q)

	if len(plan.ShardPlans) == 0 {
		return &QueryResult{}, nil
	}

	// SCATTER: execute on all shards in parallel
	resultCh := make(chan *ShardResult, len(plan.ShardPlans))

	fmt.Printf("[Query] Scattering to %d shards\n", len(plan.ShardPlans))

	for _, shardPlan := range plan.ShardPlans {
		go func(sp *ShardQuery) {
			result, err := sg.executor.ExecuteShard(sp)
			if err != nil {
				result = &ShardResult{ShardID: sp.ShardID, Err: err}
			}
			resultCh <- result
		}(shardPlan)
	}

	// Collect all shard results
	shardResults := make([]*ShardResult, 0, len(plan.ShardPlans))

	timeout := q.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	for i := 0; i < len(plan.ShardPlans); i++ {
		select {
		case result := <-resultCh:
			shardResults = append(shardResults, result)
		case <-time.After(timeout):
			return nil, fmt.Errorf("query timeout after %v", timeout)
		}
	}

	// GATHER: merge results
	final := sg.merge(plan, shardResults)
	final.TotalTime = time.Since(start)
	final.ShardsQueried = len(plan.ShardPlans)

	fmt.Printf("[Query] Gathered from %d shards in %v\n",
		len(plan.ShardPlans), final.TotalTime)

	return final, nil
}

// merge combines shard results into the final result
func (sg *ScatterGather) merge(plan *ExecutionPlan, results []*ShardResult) *QueryResult {
	final := &QueryResult{}

	// Check for partial failures
	for _, r := range results {
		if r.Err != nil {
			final.PartialResult = true
			fmt.Printf("[Query] Shard %d failed: %v\n", r.ShardID, r.Err)
		}
	}

	switch plan.MergeOp {
	case AggCount:
		// Sum up counts from all shards
		for _, r := range results {
			if r.Err == nil {
				final.Count += r.Count
			}
		}

	case AggSum:
		// Sum up sums from all shards
		for _, r := range results {
			if r.Err == nil {
				final.Sum += r.Sum
			}
		}

	case AggAvg:
		// Sum all sums and counts, then divide
		var totalCount int64
		var totalSum float64
		for _, r := range results {
			if r.Err == nil {
				totalCount += r.Count
				totalSum += r.Sum
			}
		}
		if totalCount > 0 {
			final.Avg = totalSum / float64(totalCount)
			final.Count = totalCount
			final.Sum = totalSum
		}

	case AggMin:
		// Take global minimum
		final.Min = 0
		first := true
		for _, r := range results {
			if r.Err == nil {
				if first || r.Min < final.Min {
					final.Min = r.Min
					first = false
				}
			}
		}

	case AggMax:
		// Take global maximum
		for _, r := range results {
			if r.Err == nil {
				if r.Max > final.Max {
					final.Max = r.Max
				}
			}
		}

	default:
		// Scan: merge and sort all entries
		var allEntries []KeyValue
		for _, r := range results {
			if r.Err == nil {
				allEntries = append(allEntries, r.Entries...)
			}
		}

		// Sort by key (results from different shards may interleave)
		sort.Slice(allEntries, func(i, j int) bool {
			return allEntries[i].Key < allEntries[j].Key
		})

		// Apply limit
		if plan.FinalLimit > 0 && len(allEntries) > plan.FinalLimit {
			allEntries = allEntries[:plan.FinalLimit]
		}

		final.Entries = allEntries
		final.Count = int64(len(allEntries))
	}

	return final
}
