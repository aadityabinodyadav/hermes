// pkg/membership/memberlist.go
package membership

// MemberList maintains the cluster's view of all nodes
//
// Each node maintains its OWN copy of the member list.
// Copies may temporarily diverge during network issues.
// Gossip protocol converges them within O(log N) rounds.
//
// INCARNATION NUMBERS:
//   Each node has an incarnation counter.
//   When a node is suspected/declared dead, it increments
//   its incarnation and broadcasts "I'm alive!"
//   This "refutation" proves the node is still running.
//
//   Node A suspects Node B (incarnation=3)
//   Node B receives suspicion, increments to incarnation=4
//   Node B broadcasts: "B is ALIVE, incarnation=4"
//   All nodes update: B=ALIVE(4) overrides B=SUSPECTED(3)
//
//   Rule: higher incarnation ALWAYS wins
//   Rule: for same incarnation, ALIVE beats SUSPECTED beats DEAD

import (
	"fmt"
	"sync"
	"time"
)

// MemberState is the health state of a cluster member
type MemberState uint8

const (
	StateAlive     MemberState = 0 // node is healthy and reachable
	StateSuspected MemberState = 1 // node might be dead (under investigation)
	StateDead      MemberState = 2 // node is confirmed dead
	StateLeft      MemberState = 3 // node gracefully left the cluster
)

func (s MemberState) String() string {
	switch s {
	case StateAlive:
		return "ALIVE"
	case StateSuspected:
		return "SUSPECTED"
	case StateDead:
		return "DEAD"
	case StateLeft:
		return "LEFT"
	}
	return "UNKNOWN"
}

// Member represents one node's membership record
type Member struct {
	// NodeID is the unique node identifier (hostname:port)
	NodeID string

	// Address is the gRPC address for direct communication
	Address string

	// State is the current health state
	State MemberState

	// Incarnation is the node's self-declared version
	// Higher incarnation overrides lower for same node
	Incarnation uint64

	// LastSeen is when we last received a message from this node
	LastSeen time.Time

	// Meta is application-defined metadata (region, rack, version, etc.)
	Meta map[string]string
}

// Supersedes returns true if this member record supersedes other
// Used to resolve conflicts during gossip merge
func (m *Member) Supersedes(other *Member) bool {
	if m.NodeID != other.NodeID {
		return false
	}

	// Higher incarnation always wins
	if m.Incarnation > other.Incarnation {
		return true
	}
	if m.Incarnation < other.Incarnation {
		return false
	}

	// Same incarnation: state priority
	// DEAD > SUSPECTED > ALIVE (more severe state wins)
	// Exception: node can refute by sending ALIVE with higher incarnation
	statePriority := map[MemberState]int{
		StateAlive:     0,
		StateSuspected: 1,
		StateDead:      2,
		StateLeft:      3,
	}

	return statePriority[m.State] > statePriority[other.State]
}

// GossipUpdate is a compact representation for gossip propagation
type GossipUpdate struct {
	NodeID      string
	State       MemberState
	Incarnation uint64
	Address     string
}

// ─────────────────────────────────────────────────────────────────────────────

// MemberList manages the full cluster membership view
type MemberList struct {
	mu      sync.RWMutex
	localID string

	// members maps nodeID → Member
	members map[string]*Member

	// Callbacks
	onJoin    []func(*Member)
	onLeave   []func(*Member)
	onSuspect []func(*Member)
	onDead    []func(*Member)

	// GC period: how long to keep dead nodes before removing
	deadGCPeriod time.Duration

	// Recent updates queue for gossip piggybacking
	recentUpdates []GossipUpdate
	updatesMu     sync.Mutex
}

// NewMemberList creates a new member list
func NewMemberList(localID string) *MemberList {
	ml := &MemberList{
		localID:      localID,
		members:      make(map[string]*Member),
		deadGCPeriod: 5 * time.Minute,
	}

	// Add ourselves as the first member
	ml.members[localID] = &Member{
		NodeID:      localID,
		State:       StateAlive,
		Incarnation: 1,
		LastSeen:    time.Now(),
	}

	return ml
}

// OnJoin registers a callback for node joins
func (ml *MemberList) OnJoin(fn func(*Member))    { ml.onJoin = append(ml.onJoin, fn) }
func (ml *MemberList) OnLeave(fn func(*Member))   { ml.onLeave = append(ml.onLeave, fn) }
func (ml *MemberList) OnSuspect(fn func(*Member)) { ml.onSuspect = append(ml.onSuspect, fn) }
func (ml *MemberList) OnDead(fn func(*Member))    { ml.onDead = append(ml.onDead, fn) }

// ApplyUpdate applies a gossip update, merging with existing state
// Returns true if the update changed anything (new information)
func (ml *MemberList) ApplyUpdate(update GossipUpdate) bool {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	existing, exists := ml.members[update.NodeID]

	newMember := &Member{
		NodeID:      update.NodeID,
		State:       update.State,
		Incarnation: update.Incarnation,
		Address:     update.Address,
		LastSeen:    time.Now(),
	}

	if !exists {
		// New node we haven't seen before
		ml.members[update.NodeID] = newMember
		ml.queueUpdate(update)
		go ml.notifyJoin(newMember)
		return true
	}

	// Only apply if new info supersedes existing
	if !newMember.Supersedes(existing) {
		return false // stale update, ignore
	}

	prevState := existing.State
	*existing = *newMember

	// Trigger appropriate callbacks
	if prevState != newMember.State {
		ml.queueUpdate(update)
		switch newMember.State {
		case StateSuspected:
			go ml.notifySuspect(newMember)
		case StateDead:
			go ml.notifyDead(newMember)
		case StateLeft:
			go ml.notifyLeave(newMember)
		case StateAlive:
			if prevState == StateSuspected || prevState == StateDead {
				go ml.notifyJoin(newMember) // "back from dead"
			}
		}
	}

	return true
}

// SuspectNode marks a node as suspected
// Called when direct ping fails
func (ml *MemberList) SuspectNode(nodeID string) {
	ml.mu.Lock()
	member, exists := ml.members[nodeID]
	if !exists || member.State != StateAlive {
		ml.mu.Unlock()
		return
	}

	member.State = StateSuspected
	ml.mu.Unlock()

	ml.ApplyUpdate(GossipUpdate{
		NodeID:      nodeID,
		State:       StateSuspected,
		Incarnation: member.Incarnation,
		Address:     member.Address,
	})

	fmt.Printf("[%s] SUSPECT: %s\n", ml.localID, nodeID)
}

// ConfirmDead marks a node as dead
// Called after suspicion timeout with no refutation
func (ml *MemberList) ConfirmDead(nodeID string) {
	ml.mu.Lock()
	member, exists := ml.members[nodeID]
	if !exists || member.State == StateDead {
		ml.mu.Unlock()
		return
	}

	member.State = StateDead
	incr := member.Incarnation
	ml.mu.Unlock()

	ml.ApplyUpdate(GossipUpdate{
		NodeID:      nodeID,
		State:       StateDead,
		Incarnation: incr,
	})

	fmt.Printf("[%s] DEAD: %s\n", ml.localID, nodeID)
}

// RefuteSuspicion is called when WE are suspected
// Increment incarnation and broadcast aliveness
func (ml *MemberList) RefuteSuspicion() GossipUpdate {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	self := ml.members[ml.localID]
	self.Incarnation++
	self.State = StateAlive

	fmt.Printf("[%s] REFUTING suspicion (incarnation=%d)\n",
		ml.localID, self.Incarnation)

	return GossipUpdate{
		NodeID:      ml.localID,
		State:       StateAlive,
		Incarnation: self.Incarnation,
		Address:     self.Address,
	}
}

// Alive returns all alive members
func (ml *MemberList) Alive() []*Member {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	var result []*Member
	for _, m := range ml.members {
		if m.State == StateAlive {
			result = append(result, m)
		}
	}
	return result
}

// All returns all members (including dead, for gossip)
func (ml *MemberList) All() []*Member {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	result := make([]*Member, 0, len(ml.members))
	for _, m := range ml.members {
		result = append(result, m)
	}
	return result
}

// Get returns a specific member
func (ml *MemberList) Get(nodeID string) (*Member, bool) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	m, ok := ml.members[nodeID]
	return m, ok
}

// GC removes dead nodes that have been dead long enough
// Called periodically to prevent the member list from growing forever
func (ml *MemberList) GC() int {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	removed := 0
	now := time.Now()

	for nodeID, member := range ml.members {
		if nodeID == ml.localID {
			continue // never remove ourselves
		}
		if member.State == StateDead &&
			now.Sub(member.LastSeen) > ml.deadGCPeriod {
			delete(ml.members, nodeID)
			removed++
		}
	}

	return removed
}

// RecentUpdates returns the most recent membership changes
// Used to piggyback on outgoing SWIM messages (gossip!)
func (ml *MemberList) RecentUpdates(max int) []GossipUpdate {
	ml.updatesMu.Lock()
	defer ml.updatesMu.Unlock()

	n := len(ml.recentUpdates)
	if n > max {
		n = max
	}

	result := make([]GossipUpdate, n)
	copy(result, ml.recentUpdates[:n])
	return result
}

// queueUpdate adds an update to the recent updates queue
// MUST be called with updatesMu held or ml.mu held
func (ml *MemberList) queueUpdate(update GossipUpdate) {
	ml.updatesMu.Lock()
	defer ml.updatesMu.Unlock()

	// Keep last 64 updates for gossip piggybacking
	ml.recentUpdates = append(ml.recentUpdates, update)
	if len(ml.recentUpdates) > 64 {
		ml.recentUpdates = ml.recentUpdates[1:]
	}
}

// Notification helpers
func (ml *MemberList) notifyJoin(m *Member) {
	for _, fn := range ml.onJoin {
		fn(m)
	}
}
func (ml *MemberList) notifyLeave(m *Member) {
	for _, fn := range ml.onLeave {
		fn(m)
	}
}
func (ml *MemberList) notifySuspect(m *Member) {
	for _, fn := range ml.onSuspect {
		fn(m)
	}
}
func (ml *MemberList) notifyDead(m *Member) {
	for _, fn := range ml.onDead {
		fn(m)
	}
}

// String returns a human-readable cluster state
func (ml *MemberList) String() string {
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	result := fmt.Sprintf("MemberList (%d members):\n", len(ml.members))
	for _, m := range ml.members {
		icon := "✅"
		switch m.State {
		case StateSuspected:
			icon = "🟡"
		case StateDead:
			icon = "💀"
		case StateLeft:
			icon = "👋"
		}
		result += fmt.Sprintf("  %s %-12s state=%-9s incarnation=%d\n",
			icon, m.NodeID, m.State, m.Incarnation)
	}
	return result
}
