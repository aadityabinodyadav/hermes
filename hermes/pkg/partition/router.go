// pkg/partition/router.go
package partition

// Router routes client requests to the correct shard/node
//
// The Router is the entry point for ALL client requests.
// It uses the ShardMap to determine which node to talk to.
//
// Routing algorithm:
//   1. Extract key from request
//   2. ShardMap.Lookup(key) → ShardDescriptor
//   3. ShardDescriptor.Leader → target node
//   4. Send request to target node
//   5. If NOT_LEADER response: update leader cache, retry
//
// The "retry on NOT_LEADER" handles:
//   - Stale leader information in ShardMap
//   - Leader failover between our request and the ShardMap update
//   - The brief period when a new leader is elected but ShardMap not updated

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// RoutedRequest is a request with routing information attached
type RoutedRequest struct {
	Key        string
	Shard      *ShardDescriptor
	TargetNode string
	Attempt    int
}

// RouterConfig configures the router
type RouterConfig struct {
	// MaxRetries before giving up
	MaxRetries int

	// RetryDelay between retries
	RetryDelay time.Duration

	// StaleShardMapTTL: how long before we refresh shard map
	StaleShardMapTTL time.Duration
}

// DefaultRouterConfig returns sensible defaults
func DefaultRouterConfig() RouterConfig {
	return RouterConfig{
		MaxRetries:       3,
		RetryDelay:       10 * time.Millisecond,
		StaleShardMapTTL: 30 * time.Second,
	}
}

// Router routes requests to the correct shard
type Router struct {
	config   RouterConfig
	shardMap *ShardMap

	// Leader cache: shardID → nodeID
	// Updated on NOT_LEADER responses
	leaderCache   map[ShardID]string
	leaderCacheMu sync.RWMutex

	// Stats
	totalRequests   int64
	cacheHits       int64
	leaderRedirects int64
	errors          int64

	// Shard map version we last observed
	// If version changed, invalidate leader cache
	lastMapVersion uint64
}

// NewRouter creates a new request router
func NewRouter(shardMap *ShardMap, config RouterConfig) *Router {
	r := &Router{
		config:      config,
		shardMap:    shardMap,
		leaderCache: make(map[ShardID]string),
	}

	// Invalidate leader cache when shard map changes
	shardMap.OnChange(func(newMap *ShardMap) {
		r.leaderCacheMu.Lock()
		defer r.leaderCacheMu.Unlock()
		// Clear all cached leaders — shards may have moved
		r.leaderCache = make(map[ShardID]string)
		fmt.Printf("Router: shard map changed (v%d), cleared leader cache\n",
			newMap.Version())
	})

	return r
}

// Route resolves a key to a RoutedRequest with target node
// Implements the "find leader" logic with leader hint caching
func (r *Router) Route(key string) (*RoutedRequest, error) {
	atomic.AddInt64(&r.totalRequests, 1)

	// Look up which shard owns this key
	shard, err := r.shardMap.Lookup(key)
	if err != nil {
		atomic.AddInt64(&r.errors, 1)
		return nil, fmt.Errorf("router: failed to find shard for key %q: %w",
			key, err)
	}

	// Find the leader for this shard
	// Priority: our cached leader > shard map's leader > any replica
	targetNode := r.resolveLeader(shard)

	return &RoutedRequest{
		Key:        key,
		Shard:      shard,
		TargetNode: targetNode,
		Attempt:    0,
	}, nil
}

// RouteRange resolves a range scan [start, end) to multiple RoutedRequests
// A range scan may span multiple shards!
func (r *Router) RouteRange(startKey, endKey string) ([]*RoutedRequest, error) {
	// Find all shards in range
	shards, err := r.shardMap.LookupRange(startKey, endKey)
	if err != nil {
		return nil, fmt.Errorf("router: failed to find shards for range: %w", err)
	}

	requests := make([]*RoutedRequest, 0, len(shards))
	for _, shard := range shards {
		// Clip the key range to this shard's boundaries
		reqStart := startKey
		if shard.StartKey > reqStart {
			reqStart = shard.StartKey
		}

		reqEnd := endKey
		if endKey == "" || (shard.EndKey != "" && shard.EndKey < endKey) {
			reqEnd = shard.EndKey
		}

		targetNode := r.resolveLeader(shard)

		requests = append(requests, &RoutedRequest{
			Key:        reqStart, // use start as primary key for routing
			Shard:      shard,
			TargetNode: targetNode,
			Attempt:    0,
		})

		_ = reqEnd // used when actually sending the request
	}

	return requests, nil
}

// HandleNotLeader processes a NOT_LEADER response from a node
// Updates the leader cache with the hint from the response
// Returns the new target node to retry with
func (r *Router) HandleNotLeader(req *RoutedRequest, leaderHint string) (*RoutedRequest, error) {
	atomic.AddInt64(&r.leaderRedirects, 1)

	if req.Attempt >= r.config.MaxRetries {
		return nil, fmt.Errorf("router: max retries (%d) exceeded for key %q",
			r.config.MaxRetries, req.Key)
	}

	// Update leader cache with hint
	if leaderHint != "" {
		r.leaderCacheMu.Lock()
		r.leaderCache[req.Shard.ShardID] = leaderHint
		r.leaderCacheMu.Unlock()

		// Also update the shard map
		r.shardMap.UpdateLeader(req.Shard.ShardID, leaderHint)

		fmt.Printf("Router: shard %d leader updated to %s (via redirect)\n",
			req.Shard.ShardID, leaderHint)
	}

	// Retry with new target
	return &RoutedRequest{
		Key:        req.Key,
		Shard:      req.Shard,
		TargetNode: leaderHint,
		Attempt:    req.Attempt + 1,
	}, nil
}

// resolveLeader finds the best node to send a request to for a shard
// Priority:
//  1. Our cached leader (fastest, usually correct)
//  2. Shard map's recorded leader
//  3. First replica (fallback)
func (r *Router) resolveLeader(shard *ShardDescriptor) string {
	// Check our local cache first
	r.leaderCacheMu.RLock()
	cached, ok := r.leaderCache[shard.ShardID]
	r.leaderCacheMu.RUnlock()

	if ok {
		atomic.AddInt64(&r.cacheHits, 1)
		return cached
	}

	// Use shard map's leader info
	if shard.Leader != "" {
		return shard.Leader
	}

	// Fallback: pick any replica
	if len(shard.Replicas) > 0 {
		return shard.Replicas[0].NodeID
	}

	return ""
}

// Stats returns router statistics
func (r *Router) Stats() RouterStats {
	return RouterStats{
		TotalRequests:   atomic.LoadInt64(&r.totalRequests),
		CacheHits:       atomic.LoadInt64(&r.cacheHits),
		LeaderRedirects: atomic.LoadInt64(&r.leaderRedirects),
		Errors:          atomic.LoadInt64(&r.errors),
	}
}

// RouterStats contains router performance metrics
type RouterStats struct {
	TotalRequests   int64
	CacheHits       int64
	LeaderRedirects int64
	Errors          int64
}

func (s RouterStats) String() string {
	hitRate := float64(0)
	if s.TotalRequests > 0 {
		hitRate = float64(s.CacheHits) / float64(s.TotalRequests) * 100
	}
	return fmt.Sprintf(
		"Router{requests=%d, cacheHits=%.1f%%, redirects=%d, errors=%d}",
		s.TotalRequests, hitRate, s.LeaderRedirects, s.Errors,
	)
}
