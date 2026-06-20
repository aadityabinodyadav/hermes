// pkg/membership/manager.go
package membership

// MembershipManager is the top-level coordinator for all membership logic
// It integrates:
//   - SWIM failure detection and dissemination
//   - PhiAccrual failure detection
//   - MemberList (the cluster state)
//   - Callbacks to other Hermes subsystems

import (
	"fmt"
	"sync"
	"time"
)

// ClusterEvent represents a significant cluster membership change
type ClusterEvent struct {
	Type   ClusterEventType
	Member *Member
	At     time.Time
}

type ClusterEventType uint8

const (
	EventNodeJoined    ClusterEventType = 0
	EventNodeLeft      ClusterEventType = 1
	EventNodeSuspected ClusterEventType = 2
	EventNodeDead      ClusterEventType = 3
	EventNodeRevived   ClusterEventType = 4
)

func (e ClusterEventType) String() string {
	switch e {
	case EventNodeJoined:
		return "NODE_JOINED"
	case EventNodeLeft:
		return "NODE_LEFT"
	case EventNodeSuspected:
		return "NODE_SUSPECTED"
	case EventNodeDead:
		return "NODE_DEAD"
	case EventNodeRevived:
		return "NODE_REVIVED"
	}
	return "UNKNOWN"
}

// MembershipManager coordinates cluster membership
type MembershipManager struct {
	config SWIMConfig
	nodeID string

	memberList *MemberList
	swim       *SWIM

	// Event channel for external subscribers
	// (e.g., Raft needs to know when a node dies to trigger re-election)
	events chan ClusterEvent

	mu      sync.RWMutex
	started bool
}

// NewMembershipManager creates a new membership manager
func NewMembershipManager(
	config SWIMConfig,
	transport Transport,
) *MembershipManager {
	ml := NewMemberList(config.NodeID)
	swim := NewSWIM(config, ml, transport)

	m := &MembershipManager{
		config:     config,
		nodeID:     config.NodeID,
		memberList: ml,
		swim:       swim,
		events:     make(chan ClusterEvent, 256),
	}

	// Wire up callbacks
	ml.OnJoin(func(member *Member) {
		fmt.Printf("[%s] 🟢 NODE JOINED: %s\n", config.NodeID, member.NodeID)
		m.emit(ClusterEvent{
			Type:   EventNodeJoined,
			Member: member,
			At:     time.Now(),
		})
	})

	ml.OnLeave(func(member *Member) {
		fmt.Printf("[%s] 👋 NODE LEFT: %s\n", config.NodeID, member.NodeID)
		m.emit(ClusterEvent{
			Type:   EventNodeLeft,
			Member: member,
			At:     time.Now(),
		})
	})

	ml.OnSuspect(func(member *Member) {
		fmt.Printf("[%s] 🟡 NODE SUSPECTED: %s\n", config.NodeID, member.NodeID)
		m.emit(ClusterEvent{
			Type:   EventNodeSuspected,
			Member: member,
			At:     time.Now(),
		})
	})

	ml.OnDead(func(member *Member) {
		fmt.Printf("[%s] 💀 NODE DEAD: %s\n", config.NodeID, member.NodeID)
		m.emit(ClusterEvent{
			Type:   EventNodeDead,
			Member: member,
			At:     time.Now(),
		})
	})

	return m
}

// Start begins membership management
func (m *MembershipManager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		return
	}
	m.started = true
	m.swim.Start()
}

// Stop stops membership management
func (m *MembershipManager) Stop() {
	m.swim.Stop()
	close(m.events)
}

// Join joins an existing cluster via seed nodes
func (m *MembershipManager) Join(seedNodes []string) error {
	return m.swim.JoinCluster(seedNodes)
}

// Events returns the channel for cluster events
// Subscribe to this to react to membership changes
func (m *MembershipManager) Events() <-chan ClusterEvent {
	return m.events
}

// Members returns the current cluster state
func (m *MembershipManager) Members() []*Member {
	return m.memberList.All()
}

// AliveMembers returns only alive members
func (m *MembershipManager) AliveMembers() []*Member {
	return m.memberList.Alive()
}

// IsAlive returns true if a node is considered alive
func (m *MembershipManager) IsAlive(nodeID string) bool {
	member, exists := m.memberList.Get(nodeID)
	if !exists {
		return false
	}
	return member.State == StateAlive
}

// Get returns the member information by node ID
func (m *MembershipManager) Get(nodeID string) (*Member, bool) {
	return m.memberList.Get(nodeID)
}

// RecentUpdates returns the recent gossip updates
func (m *MembershipManager) RecentUpdates(n int) []GossipUpdate {
	return m.memberList.RecentUpdates(n)
}

// emit sends a cluster event to subscribers
func (m *MembershipManager) emit(event ClusterEvent) {
	select {
	case m.events <- event:
	default:
		// Event channel full — drop (subscriber is slow)
		// In production: log this as a warning
	}
}
