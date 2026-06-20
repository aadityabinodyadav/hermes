// pkg/consistency/distlock.go
package consistency

// DistributedLock implements distributed mutual exclusion
// with fencing tokens (Martin Kleppmann's approach)
//
// The problem with simple distributed locks:
//
//   1. Client A acquires lock (token=1)
//   2. Client A pauses (GC, slow network, etc.)
//   3. Lock expires (TTL elapsed)
//   4. Client B acquires lock (token=2)
//   5. Client B uses the resource, writes data
//   6. Client A resumes, thinks it still holds the lock
//   7. Client A also writes data ← CORRUPTION!
//
// Fencing token solution:
//   - Each lock grant includes a monotonically increasing token
//   - Client must present token when using the resource
//   - Resource rejects operations with stale tokens
//
//   1. Client A acquires lock, gets token=1
//   2. Client A pauses
//   3. Lock expires
//   4. Client B acquires lock, gets token=2
//   5. Client B writes with token=2 (accepted)
//   6. Client A resumes, tries to write with token=1
//   7. Resource sees token=1 < seen_token=2 → REJECT
//   → No corruption!

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// FencingToken is a monotonically increasing lock identifier
type FencingToken uint64

// LockGrant is returned when a lock is successfully acquired
type LockGrant struct {
	// Token is the fencing token for this lock grant
	Token FencingToken

	// ExpiresAt is when this lock expires automatically
	ExpiresAt time.Time

	// LockName is the name of the lock that was acquired
	LockName string

	// Holder is the node/client holding this lock
	Holder string
}

// IsValid returns true if the grant hasn't expired
func (g *LockGrant) IsValid() bool {
	return time.Now().Before(g.ExpiresAt)
}

// DistributedLockService manages distributed locks
// In production: backed by Raft for consensus
// Here: in-memory simulation for demo
type DistributedLockService struct {
	mu sync.Mutex

	// locks maps lock name → current grant
	locks map[string]*LockGrant

	// nextToken is the next fencing token to issue
	nextToken uint64

	// defaultTTL is how long locks live
	defaultTTL time.Duration

	// Raft integration: all lock operations go through Raft
	// (in our demo we simulate this)
	raftLog []string
}

// NewDistributedLockService creates a new lock service
func NewDistributedLockService() *DistributedLockService {
	svc := &DistributedLockService{
		locks:      make(map[string]*LockGrant),
		defaultTTL: 30 * time.Second,
		nextToken:  1,
	}

	// Background goroutine to expire locks
	go svc.expireLoop()

	return svc
}

// Acquire tries to acquire a named lock
// Returns the lock grant if successful, error if lock is held
func (s *DistributedLockService) Acquire(
	ctx context.Context,
	lockName string,
	holder string,
	ttl time.Duration,
) (*LockGrant, error) {
	if ttl <= 0 {
		ttl = s.defaultTTL
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if lock is currently held by someone else
	existing, exists := s.locks[lockName]
	if exists && existing.IsValid() {
		return nil, fmt.Errorf("lock %q is held by %s (expires in %v)",
			lockName, existing.Holder, time.Until(existing.ExpiresAt))
	}

	// Issue new grant with fresh fencing token
	token := FencingToken(atomic.AddUint64(&s.nextToken, 1))

	grant := &LockGrant{
		Token:     token,
		ExpiresAt: time.Now().Add(ttl),
		LockName:  lockName,
		Holder:    holder,
	}

	s.locks[lockName] = grant

	// In production: this goes through Raft
	// s.raftPropose(LockAcquireCommand{lockName, token, holder, ttl})
	s.raftLog = append(s.raftLog, fmt.Sprintf(
		"LOCK_ACQUIRE: name=%s holder=%s token=%d ttl=%v",
		lockName, holder, token, ttl))

	fmt.Printf("[LockService] ACQUIRED: %s by %s (token=%d, expires=%v)\n",
		lockName, holder, token, ttl)

	return grant, nil
}

// Release releases a named lock
// Must present the original fencing token
func (s *DistributedLockService) Release(
	ctx context.Context,
	lockName string,
	token FencingToken,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.locks[lockName]
	if !exists {
		return fmt.Errorf("lock %q not found", lockName)
	}

	if existing.Token != token {
		return fmt.Errorf("lock %q: token mismatch (have %d, grant was %d) - possible stale lock",
			lockName, token, existing.Token)
	}

	delete(s.locks, lockName)

	s.raftLog = append(s.raftLog, fmt.Sprintf(
		"LOCK_RELEASE: name=%s token=%d", lockName, token))

	fmt.Printf("[LockService] RELEASED: %s (token=%d)\n", lockName, token)

	return nil
}

// Renew extends a lock's TTL (heartbeat mechanism)
// Lock holder must renew before expiry to keep the lock
func (s *DistributedLockService) Renew(
	ctx context.Context,
	lockName string,
	token FencingToken,
	ttl time.Duration,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.locks[lockName]
	if !exists {
		return fmt.Errorf("lock %q not found - has it expired?", lockName)
	}

	if existing.Token != token {
		return fmt.Errorf("lock %q: cannot renew - stale token", lockName)
	}

	if !existing.IsValid() {
		return fmt.Errorf("lock %q has already expired", lockName)
	}

	existing.ExpiresAt = time.Now().Add(ttl)
	return nil
}

// VerifyToken checks if a fencing token is still valid for resource access
// The RESOURCE calls this to prevent stale lock holders from corrupting state
func (s *DistributedLockService) VerifyToken(lockName string, token FencingToken) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.locks[lockName]
	if !exists {
		return fmt.Errorf("lock %q: no current holder - operation rejected", lockName)
	}

	if existing.Token > token {
		return fmt.Errorf("lock %q: stale token %d (current holder has token %d) - REJECTED",
			lockName, token, existing.Token)
	}

	if !existing.IsValid() {
		return fmt.Errorf("lock %q: expired token - REJECTED", lockName)
	}

	return nil
}

// expireLoop removes expired locks
func (s *DistributedLockService) expireLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		for name, grant := range s.locks {
			if now.After(grant.ExpiresAt) {
				fmt.Printf("[LockService] EXPIRED: %s (held by %s, token=%d)\n",
					name, grant.Holder, grant.Token)
				delete(s.locks, name)
			}
		}
		s.mu.Unlock()
	}
}

// Status returns current lock status
func (s *DistributedLockService) Status() map[string]*LockGrant {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make(map[string]*LockGrant)
	for k, v := range s.locks {
		grantCopy := *v
		result[k] = &grantCopy
	}
	return result
}
