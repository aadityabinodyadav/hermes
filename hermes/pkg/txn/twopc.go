// pkg/txn/twopc.go
package txn

// TwoPhaseCommit implements the classic 2PC protocol
//
// ROLES:
//   Coordinator: orchestrates the transaction
//   Participant: executes part of the transaction on one shard
//
// STATES:
//   INIT → PREPARING → PREPARED → COMMITTING → COMMITTED
//                              ↘ ABORTING → ABORTED
//
// PERSISTENCE:
//   Coordinator must persist:
//     - PREPARE record before sending PREPARE to participants
//     - COMMIT/ABORT record before sending to participants
//
//   Participants must persist:
//     - PREPARE record before voting COMMIT
//     - COMMIT/ABORT record before applying/discarding

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// TxnState represents the state of a 2PC transaction
type TxnState uint8

const (
	TxnInit       TxnState = 0
	TxnPreparing  TxnState = 1
	TxnPrepared   TxnState = 2
	TxnCommitting TxnState = 3
	TxnCommitted  TxnState = 4
	TxnAborting   TxnState = 5
	TxnAborted    TxnState = 6
)

// TxnActive is an alias for TxnInit (used by TwoPhaseCommitCoordinator)
const TxnActive = TxnInit

func (s TxnState) String() string {
	switch s {
	case TxnInit:
		return "INIT"
	case TxnPreparing:
		return "PREPARING"
	case TxnPrepared:
		return "PREPARED"
	case TxnCommitting:
		return "COMMITTING"
	case TxnCommitted:
		return "COMMITTED"
	case TxnAborting:
		return "ABORTING"
	case TxnAborted:
		return "ABORTED"
	}
	return "UNKNOWN"
}

// Vote represents a participant's vote in 2PC
type Vote uint8

const (
	VoteCommit Vote = 0
	VoteAbort  Vote = 1
)

// Participant represents a shard/node participating in 2PC
type Participant struct {
	NodeID  string
	ShardID uint64
	Address string
	Voted   bool
	Vote    Vote
	Acked   bool
}

// TwoPCTransaction represents a single 2PC transaction
type TwoPCTransaction struct {
	TxnID        uint64
	State        TxnState
	StartTime    time.Time
	Participants []*Participant

	mu sync.Mutex
}

// TwoPCCoordinator coordinates distributed transactions
type TwoPCCoordinator struct {
	mu sync.Mutex

	// nodeID is this coordinator's ID
	nodeID string

	// transactions tracks all active transactions
	transactions map[uint64]*TwoPCTransaction

	// nextTxnID for generating unique transaction IDs
	nextTxnID uint64

	// TSO for timestamps
	tso *TimestampOracle

	// Callbacks for persistence
	persistPrepare  func(txn *TwoPCTransaction) error
	persistDecision func(txn *TwoPCTransaction, commit bool) error

	// Timeout configuration
	prepareTimeout time.Duration
	commitTimeout  time.Duration
}

// NewTwoPCCoordinator creates a new 2PC coordinator
func NewTwoPCCoordinator(nodeID string, tso *TimestampOracle) *TwoPCCoordinator {
	return &TwoPCCoordinator{
		nodeID:         nodeID,
		transactions:   make(map[uint64]*TwoPCTransaction),
		nextTxnID:      1,
		tso:            tso,
		prepareTimeout: 5 * time.Second,
		commitTimeout:  10 * time.Second,
	}
}

// Begin starts a new distributed transaction
// Returns a transaction handle for the caller
func (c *TwoPCCoordinator) Begin(ctx context.Context, participants []Participant) (*TwoPCTransaction, error) {
	c.mu.Lock()
	txnID := c.nextTxnID
	c.nextTxnID++

	txn := &TwoPCTransaction{
		TxnID:        txnID,
		State:        TxnInit,
		StartTime:    time.Now(),
		Participants: make([]*Participant, len(participants)),
	}
	for i, p := range participants {
		participantCopy := p
		txn.Participants[i] = &participantCopy
	}

	c.transactions[txnID] = txn
	c.mu.Unlock()

	fmt.Printf("[2PC] Transaction %d started with %d participants\n",
		txnID, len(txn.Participants))

	return txn, nil
}

// Prepare executes Phase 1 of 2PC
// Returns true if all participants voted to commit
func (c *TwoPCCoordinator) Prepare(ctx context.Context, txn *TwoPCTransaction) (bool, error) {
	txn.mu.Lock()
	if txn.State != TxnInit {
		txn.mu.Unlock()
		return false, fmt.Errorf("2PC: transaction %d not in INIT state", txn.TxnID)
	}
	txn.State = TxnPreparing
	txn.mu.Unlock()

	// Persist PREPARE record BEFORE sending to participants
	// This is critical for recovery!
	if c.persistPrepare != nil {
		if err := c.persistPrepare(txn); err != nil {
			txn.mu.Lock()
			txn.State = TxnAborting
			txn.mu.Unlock()
			return false, fmt.Errorf("2PC: failed to persist prepare: %w", err)
		}
	}

	fmt.Printf("[2PC] Transaction %d: sending PREPARE to %d participants\n",
		txn.TxnID, len(txn.Participants))

	// Send PREPARE to all participants concurrently
	voteCh := make(chan *ParticipantVote, len(txn.Participants))
	for _, p := range txn.Participants {
		go c.sendPrepare(ctx, txn, p, voteCh)
	}

	// Collect all votes
	votes := make([]*ParticipantVote, 0, len(txn.Participants))
	for i := 0; i < len(txn.Participants); i++ {
		select {
		case vote := <-voteCh:
			votes = append(votes, vote)
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(c.prepareTimeout):
			return false, fmt.Errorf("2PC: prepare timeout for transaction %d", txn.TxnID)
		}
	}

	// Check if all voted commit
	allCommit := true
	for _, vote := range votes {
		if vote.Vote == VoteAbort {
			allCommit = false
			fmt.Printf("[2PC] Participant %s voted ABORT: %s\n",
				vote.Participant.NodeID, vote.Reason)
		}
		vote.Participant.Voted = true
		vote.Participant.Vote = vote.Vote
	}

	txn.mu.Lock()
	txn.State = TxnPrepared
	txn.mu.Unlock()

	fmt.Printf("[2PC] Transaction %d: all participants PREPARED (allCommit=%v)\n",
		txn.TxnID, allCommit)

	return allCommit, nil
}

// ParticipantVote is a participant's response to PREPARE
type ParticipantVote struct {
	Participant *Participant
	Vote        Vote
	Reason      string
}

// sendPrepare sends PREPARE to one participant and collects vote
func (c *TwoPCCoordinator) sendPrepare(
	ctx context.Context,
	txn *TwoPCTransaction,
	p *Participant,
	voteCh chan<- *ParticipantVote,
) {
	// In production: send gRPC to participant
	// Participant executes:
	//   1. Acquire locks on keys
	//   2. Check for conflicts
	//   3. Write PREPARE record to local WAL
	//   4. Return VOTE_COMMIT or VOTE_ABORT

	// Simulate participant processing
	time.Sleep(10 * time.Millisecond)

	// Simulate occasional abort (conflict detection)
	vote := VoteCommit
	reason := ""

	// For demo: randomly abort some transactions
	if txn.TxnID%7 == 0 {
		vote = VoteAbort
		reason = "key locked by another transaction"
	}

	voteCh <- &ParticipantVote{
		Participant: p,
		Vote:        vote,
		Reason:      reason,
	}
}

// Commit executes Phase 2 of 2PC (commit path)
func (c *TwoPCCoordinator) Commit(ctx context.Context, txn *TwoPCTransaction) error {
	txn.mu.Lock()
	if txn.State != TxnPrepared {
		txn.mu.Unlock()
		return fmt.Errorf("2PC: transaction %d not in PREPARED state", txn.TxnID)
	}
	txn.State = TxnCommitting
	txn.mu.Unlock()

	// Persist COMMIT decision BEFORE sending
	if c.persistDecision != nil {
		if err := c.persistDecision(txn, true); err != nil {
			return fmt.Errorf("2PC: failed to persist commit: %w", err)
		}
	}

	fmt.Printf("[2PC] Transaction %d: sending COMMIT to participants\n", txn.TxnID)

	// Send COMMIT to all participants
	ackCh := make(chan *Participant, len(txn.Participants))
	for _, p := range txn.Participants {
		go c.sendCommit(ctx, txn, p, ackCh)
	}

	// Wait for all ACKs
	for i := 0; i < len(txn.Participants); i++ {
		select {
		case <-ackCh:
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.commitTimeout):
			// In production: background retry, not failure
			// Participant will eventually commit when coordinator recovers
		}
	}

	txn.mu.Lock()
	txn.State = TxnCommitted
	txn.mu.Unlock()

	fmt.Printf("[2PC] Transaction %d: COMMITTED ✅\n", txn.TxnID)
	return nil
}

// Abort executes Phase 2 of 2PC (abort path)
func (c *TwoPCCoordinator) Abort(ctx context.Context, txn *TwoPCTransaction) error {
	txn.mu.Lock()
	if txn.State == TxnCommitted || txn.State == TxnAborted {
		txn.mu.Unlock()
		return fmt.Errorf("2PC: transaction %d already terminal", txn.TxnID)
	}
	txn.State = TxnAborting
	txn.mu.Unlock()

	// Persist ABORT decision
	if c.persistDecision != nil {
		if err := c.persistDecision(txn, false); err != nil {
			return fmt.Errorf("2PC: failed to persist abort: %w", err)
		}
	}

	fmt.Printf("[2PC] Transaction %d: sending ABORT to participants\n", txn.TxnID)

	// Send ABORT to all participants
	for _, p := range txn.Participants {
		c.sendAbort(ctx, txn, p)
	}

	txn.mu.Lock()
	txn.State = TxnAborted
	txn.mu.Unlock()

	fmt.Printf("[2PC] Transaction %d: ABORTED ❌\n", txn.TxnID)
	return nil
}

// sendCommit sends COMMIT to one participant
func (c *TwoPCCoordinator) sendCommit(
	ctx context.Context,
	txn *TwoPCTransaction,
	p *Participant,
	ackCh chan<- *Participant,
) {
	// In production: gRPC to participant
	// Participant:
	//   1. Write COMMIT to WAL
	//   2. Apply changes to storage
	//   3. Release locks
	//   4. Send ACK

	time.Sleep(5 * time.Millisecond)
	p.Acked = true
	ackCh <- p
}

// sendAbort sends ABORT to one participant
func (c *TwoPCCoordinator) sendAbort(
	ctx context.Context,
	txn *TwoPCTransaction,
	p *Participant,
) {
	// In production: gRPC to participant
	// Participant:
	//   1. Write ABORT to WAL
	//   2. Discard changes
	//   3. Release locks

	time.Sleep(5 * time.Millisecond)
	fmt.Printf("[2PC] Participant %s: transaction %d ABORTED\n",
		p.NodeID, txn.TxnID)
}

// GetState returns the current state of a transaction
func (c *TwoPCCoordinator) GetState(txnID uint64) (TxnState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	txn, exists := c.transactions[txnID]
	if !exists {
		return TxnAborted, fmt.Errorf("2PC: transaction %d not found", txnID)
	}

	return txn.State, nil
}

// Recovery: on coordinator restart, recover in-doubt transactions
func (c *TwoPCCoordinator) RecoverInDoubt() ([]uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var inDoubt []uint64
	for txnID, txn := range c.transactions {
		if txn.State == TxnPrepared || txn.State == TxnCommitting {
			inDoubt = append(inDoubt, txnID)
		}
	}

	if len(inDoubt) > 0 {
		fmt.Printf("[2PC] Recovery: found %d in-doubt transactions: %v\n",
			len(inDoubt), inDoubt)
	}

	return inDoubt, nil
}
