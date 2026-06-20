// pkg/multiregion/topology.go
package multiregion

// MultiRegion manages geo-distributed Hermes deployments
//
// Key concepts:
//   Region: a geographic location (us-east, eu-west, ap-sydney)
//   Cluster: a Raft group within a region
//   Learner: a non-voting replica in another region
//
// Write path: always to the region owning the data
// Read path: from nearest region (may be stale if not local)

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// Region represents a geographic region
type Region struct {
	ID       string
	Name     string
	Location string // e.g., "us-east-1"

	// Nodes in this region
	Nodes []string

	// Latency to other regions (approximate)
	Latency map[string]time.Duration // regionID → RTT
}

// RegionTopology manages the multi-region configuration
type RegionTopology struct {
	mu      sync.RWMutex
	regions map[string]*Region // regionID → Region
	localID string             // this node's region
}

// NewRegionTopology creates a topology
func NewRegionTopology(localRegionID string) *RegionTopology {
	return &RegionTopology{
		regions: make(map[string]*Region),
		localID: localRegionID,
	}
}

// AddRegion registers a region
func (t *RegionTopology) AddRegion(region *Region) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.regions[region.ID] = region
	fmt.Printf("[MultiRegion] Registered region %s (%s)\n",
		region.ID, region.Location)
}

// NearestRegion returns the region with lowest latency to local
func (t *RegionTopology) NearestRegion() *Region {
	t.mu.RLock()
	defer t.mu.RUnlock()

	localRegion, exists := t.regions[t.localID]
	if !exists {
		return nil
	}

	var nearest *Region
	minLatency := time.Duration(math.MaxInt64)

	for _, region := range t.regions {
		if region.ID == t.localID {
			continue
		}

		latency, ok := localRegion.Latency[region.ID]
		if !ok {
			continue
		}

		if latency < minLatency {
			minLatency = latency
			nearest = region
		}
	}

	return nearest
}

// AllRegions returns all regions sorted by latency from local
func (t *RegionTopology) AllRegionsByLatency() []*Region {
	t.mu.RLock()
	defer t.mu.RUnlock()

	localRegion := t.regions[t.localID]
	if localRegion == nil {
		return nil
	}

	type regionWithLatency struct {
		region  *Region
		latency time.Duration
	}

	var sorted []regionWithLatency
	for _, region := range t.regions {
		latency := time.Duration(0)
		if region.ID != t.localID {
			if l, ok := localRegion.Latency[region.ID]; ok {
				latency = l
			}
		}
		sorted = append(sorted, regionWithLatency{region, latency})
	}

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].latency < sorted[j].latency
	})

	result := make([]*Region, len(sorted))
	for i, r := range sorted {
		result[i] = r.region
	}
	return result
}

// FollowerReadRouter routes reads to the nearest available replica
type FollowerReadRouter struct {
	topology     *RegionTopology
	maxStaleness time.Duration // max acceptable staleness
}

// NewFollowerReadRouter creates a router for follower reads
func NewFollowerReadRouter(topology *RegionTopology, maxStaleness time.Duration) *FollowerReadRouter {
	return &FollowerReadRouter{
		topology:     topology,
		maxStaleness: maxStaleness,
	}
}

// RouteRead finds the best region to serve a read from
// Returns: (regionID, estimated_latency, estimated_staleness)
func (r *FollowerReadRouter) RouteRead(
	requiredConsistency string,
) (regionID string, latency, staleness time.Duration) {

	switch requiredConsistency {
	case "strong":
		// Must go to leader's region
		// No follower reads allowed
		return "global-leader", 100 * time.Millisecond, 0

	case "bounded":
		// Can use local replica if within staleness bound
		regions := r.topology.AllRegionsByLatency()
		for _, region := range regions {
			// In production: check actual replication lag of this region
			estimatedStaleness := estimateReplicationLag(region.ID)
			if estimatedStaleness <= r.maxStaleness {
				return region.ID, 1 * time.Millisecond, estimatedStaleness
			}
		}
		// All replicas too stale: go to leader
		return "global-leader", 100 * time.Millisecond, 0

	default: // eventual
		// Use local region (may be stale)
		return r.topology.localID, 1 * time.Millisecond, 200 * time.Millisecond
	}
}

// estimateReplicationLag estimates how stale a region's data is
// In production: query the actual replication lag from metrics
func estimateReplicationLag(regionID string) time.Duration {
	// Placeholder: in production, this queries the replica's
	// committed index and compares to leader's committed index
	return 50 * time.Millisecond
}
