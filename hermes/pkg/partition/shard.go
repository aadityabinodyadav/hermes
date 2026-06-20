package partition



import (
	"fmt"
	"sort"
	"sync"
)

type ShardID uint64

type RaftGroupID uint64

type ShardDescriptor struct {
	ShardID ShardID

	StartKey string

	EndKey    string
	RaftGroup RaftGroupID

	Replicas []ReplicaDescriptor

	Leader string

	Version uint64
}

type ReplicaDescriptor struct {
	NodeID  string
	Address string // host:port for gRPC
	Role    ReplicaRole
}

type ReplicaRole uint8

const (
	ReplicaVoter    ReplicaRole = 0 // participates in voting
	ReplicaLearner  ReplicaRole = 1 // receives log but doesn't vote
	ReplicaOutgoing ReplicaRole = 2 // being removed
)

func (s *ShardDescriptor) ContainsKey(key string) bool {
	if s.StartKey != "" && key < s.StartKey {
		return false
	}
	if s.EndKey != "" && key >= s.EndKey {
		return false
	}
	return true
}

func (s *ShardDescriptor) String() string {
	start := s.StartKey
	if start == "" {
		start = "(-∞"
	} else {
		start = "[" + start
	}

	end := s.EndKey
	if end == "" {
		end = "+∞)"
	} else {
		end = end + ")"
	}

	return fmt.Sprintf("Shard{id=%d, range=%s..%s, group=%d, leader=%s}",
		s.ShardID, start, end, s.RaftGroup, s.Leader)
}

type ShardMap struct {
	mu sync.RWMutex

	shards []*ShardDescriptor

	shardsByID map[ShardID]*ShardDescriptor

	version uint64

	onChange []func(newMap *ShardMap)
}

func NewShardMap() *ShardMap {
	return &ShardMap{
		shardsByID: make(map[ShardID]*ShardDescriptor),
	}
}

func (sm *ShardMap) Initialize(numShards int, nodes []string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	splitPoints := computeSplitPoints(numShards)

	sm.shards = make([]*ShardDescriptor, 0, numShards)
	sm.shardsByID = make(map[ShardID]*ShardDescriptor)

	for i := 0; i < numShards; i++ {
		startKey := ""
		if i > 0 {
			startKey = splitPoints[i-1]
		}

		endKey := ""
		if i < len(splitPoints) {
			endKey = splitPoints[i]
		}

		replicas := assignReplicas(nodes, i, 3) // 3-way replication

		shard := &ShardDescriptor{
			ShardID:   ShardID(i),
			StartKey:  startKey,
			EndKey:    endKey,
			RaftGroup: RaftGroupID(i),
			Replicas:  replicas,
			Version:   1,
		}

		sm.shards = append(sm.shards, shard)
		sm.shardsByID[shard.ShardID] = shard
	}

	sm.version++
	fmt.Printf("ShardMap initialized: %d shards across %d nodes\n",
		numShards, len(nodes))
	for _, s := range sm.shards {
		fmt.Printf("  %s\n", s)
	}
}

func (sm *ShardMap) Lookup(key string) (*ShardDescriptor, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if len(sm.shards) == 0 {
		return nil, fmt.Errorf("shardmap: no shards configured")
	}

	idx := sort.Search(len(sm.shards), func(i int) bool {
		return sm.shards[i].StartKey > key
	}) - 1

	if idx < 0 {
		idx = 0
	}

	shard := sm.shards[idx]
	if !shard.ContainsKey(key) {
		return nil, fmt.Errorf("shardmap: no shard for key %q", key)
	}

	return shard, nil
}

func (sm *ShardMap) LookupRange(startKey, endKey string) ([]*ShardDescriptor, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if len(sm.shards) == 0 {
		return nil, fmt.Errorf("shardmap: no shards configured")
	}

	startIdx := sort.Search(len(sm.shards), func(i int) bool {
		return sm.shards[i].StartKey > startKey
	}) - 1

	if startIdx < 0 {
		startIdx = 0
	}

	var result []*ShardDescriptor
	for i := startIdx; i < len(sm.shards); i++ {
		shard := sm.shards[i]
		if endKey != "" && shard.StartKey >= endKey {
			break
		}
		result = append(result, shard)
	}

	return result, nil
}

func (sm *ShardMap) UpdateLeader(shardID ShardID, leaderNodeID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	shard, ok := sm.shardsByID[shardID]
	if !ok {
		return
	}

	shard.Leader = leaderNodeID
	shard.Version++
	sm.version++
}

func (sm *ShardMap) Split(shardID ShardID, splitKey string, newGroupID RaftGroupID, newReplicas []ReplicaDescriptor) (*ShardDescriptor, *ShardDescriptor, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	original, ok := sm.shardsByID[shardID]
	if !ok {
		return nil, nil, fmt.Errorf("shardmap: shard %d not found", shardID)
	}

	if !original.ContainsKey(splitKey) {
		return nil, nil, fmt.Errorf("shardmap: split key %q not in shard %s",
			splitKey, original)
	}

	newShardID := ShardID(len(sm.shards))

	newShard := &ShardDescriptor{
		ShardID:   newShardID,
		StartKey:  splitKey,
		EndKey:    original.EndKey,
		RaftGroup: newGroupID,
		Replicas:  newReplicas,
		Version:   1,
	}

	original.EndKey = splitKey
	original.Version++

	sm.shards = append(sm.shards, newShard)
	sort.Slice(sm.shards, func(i, j int) bool {
		return sm.shards[i].StartKey < sm.shards[j].StartKey
	})
	sm.shardsByID[newShardID] = newShard
	sm.version++

	fmt.Printf("ShardMap: split shard %d at key=%q\n", shardID, splitKey)
	fmt.Printf("  Original: %s\n", original)
	fmt.Printf("  New:      %s\n", newShard)

	for _, cb := range sm.onChange {
		go cb(sm)
	}

	return original, newShard, nil
}

func (sm *ShardMap) Merge(shard1ID, shard2ID ShardID) (*ShardDescriptor, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	s1, ok1 := sm.shardsByID[shard1ID]
	s2, ok2 := sm.shardsByID[shard2ID]

	if !ok1 || !ok2 {
		return nil, fmt.Errorf("shardmap: shard not found")
	}

	if s1.EndKey != s2.StartKey {
		return nil, fmt.Errorf("shardmap: shards %d and %d are not adjacent",
			shard1ID, shard2ID)
	}

	s1.EndKey = s2.EndKey
	s1.Version++
	sm.version++

	delete(sm.shardsByID, shard2ID)
	newShards := make([]*ShardDescriptor, 0, len(sm.shards)-1)
	for _, s := range sm.shards {
		if s.ShardID != shard2ID {
			newShards = append(newShards, s)
		}
	}
	sm.shards = newShards

	fmt.Printf("ShardMap: merged shards %d and %d → %s\n",
		shard1ID, shard2ID, s1)

	return s1, nil
}

func (sm *ShardMap) Version() uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.version
}

func (sm *ShardMap) All() []*ShardDescriptor {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	result := make([]*ShardDescriptor, len(sm.shards))
	copy(result, sm.shards)
	return result
}

func (sm *ShardMap) OnChange(fn func(*ShardMap)) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.onChange = append(sm.onChange, fn)
}

func computeSplitPoints(n int) []string {
	if n <= 1 {
		return nil
	}

	points := make([]string, n-1)
	for i := 0; i < n-1; i++ {
		r := 33 + (93*(i+1))/n // '!' to '~'
		points[i] = string(rune(r))
	}
	return points
}

func assignReplicas(nodes []string, shardIdx, replicationFactor int) []ReplicaDescriptor {
	n := len(nodes)
	if replicationFactor > n {
		replicationFactor = n
	}

	replicas := make([]ReplicaDescriptor, replicationFactor)
	for i := 0; i < replicationFactor; i++ {
		nodeIdx := (shardIdx + i) % n
		replicas[i] = ReplicaDescriptor{
			NodeID:  nodes[nodeIdx],
			Address: fmt.Sprintf("%s:7000", nodes[nodeIdx]),
			Role:    ReplicaVoter,
		}
	}
	return replicas
}
