// pkg/txn/saga.go
package txn

// Saga implements the Saga pattern for long-running transactions
//
// USE CASE: Transactions that span multiple services and take
// minutes or hours to complete. Traditional 2PC is too slow
// and holds locks too long.
//
// EXAMPLE: E-commerce order
//   1. Reserve inventory (compensate: release inventory)
//   2. Charge credit card (compensate: refund)
//   3. Create shipment (compensate: cancel shipment)
//   4. Send confirmation email (no compensate needed)
//
// If step 3 fails:
//   → Execute compensations in REVERSE order:
//     2. Refund credit card
//     1. Release inventory
//
// TWO IMPLEMENTATION STYLES:
//
//   1. CHOREOGRAPHY: Each step triggers the next via events
//      Pros: Decoupled, no central coordinator
//      Cons: Hard to track overall state, debugging difficult
//
//   2. ORCHESTRATION: Central coordinator manages the flow
//      Pros: Clear state, easier debugging, retry logic
//      Cons: Single point of failure (mitigate with replication)
//
// We implement ORCHESTRATION style.

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// SagaStep is one step in a saga
type SagaStep struct {
	// Name is a human-readable name for this step
	Name string

	// Execute performs the forward action
	Execute func(ctx context.Context, sagaData map[string]interface{}) error

	// Compensate undoes the forward action
	// Called if a later step fails
	Compensate func(ctx context.Context, sagaData map[string]interface{}) error

	// RetryCount is how many times to retry on failure
	RetryCount int

	// RetryDelay is delay between retries
	RetryDelay time.Duration
}

// SagaOrchestrator manages saga execution
type SagaOrchestrator struct {
	mu sync.Mutex

	// sagas tracks all active sagas
	sagas map[string]*SagaExecution

	// Default retry settings
	defaultRetryCount int
	defaultRetryDelay time.Duration
}

// SagaExecution tracks the execution state of one saga
type SagaExecution struct {
	ID          string
	Steps       []SagaStep
	CurrentStep int
	State       SagaState
	Data        map[string]interface{}
	StartedAt   time.Time
	CompletedAt time.Time
	Error       error
}

type SagaState uint8

const (
	SagaPending      SagaState = 0
	SagaRunning      SagaState = 1
	SagaCompleted    SagaState = 2
	SagaFailed       SagaState = 3
	SagaCompensating SagaState = 4
)

func (s SagaState) String() string {
	switch s {
	case SagaPending:
		return "PENDING"
	case SagaRunning:
		return "RUNNING"
	case SagaCompleted:
		return "COMPLETED"
	case SagaFailed:
		return "FAILED"
	case SagaCompensating:
		return "COMPENSATING"
	}
	return "UNKNOWN"
}

// NewSagaOrchestrator creates a new saga orchestrator
func NewSagaOrchestrator() *SagaOrchestrator {
	return &SagaOrchestrator{
		sagas:             make(map[string]*SagaExecution),
		defaultRetryCount: 3,
		defaultRetryDelay: 100 * time.Millisecond,
	}
}

// Begin starts a new saga with the given steps
func (o *SagaOrchestrator) Begin(
	sagaID string,
	steps []SagaStep,
	initialData map[string]interface{},
) *SagaExecution {
	o.mu.Lock()
	defer o.mu.Unlock()

	execution := &SagaExecution{
		ID:          sagaID,
		Steps:       steps,
		CurrentStep: 0,
		State:       SagaPending,
		Data:        initialData,
		StartedAt:   time.Now(),
	}

	o.sagas[sagaID] = execution

	return execution
}

// Execute runs the saga to completion (blocking)
// For non-blocking, use ExecuteAsync
func (o *SagaOrchestrator) Execute(
	ctx context.Context,
	execution *SagaExecution,
) error {
	o.mu.Lock()
	execution.State = SagaRunning
	o.mu.Unlock()

	// Execute each step in order
	for i := 0; i < len(execution.Steps); i++ {
		execution.CurrentStep = i
		step := execution.Steps[i]

		err := o.executeStepWithRetry(ctx, execution, step)
		if err != nil {
			// Step failed — compensate all previous steps
			o.mu.Lock()
			execution.State = SagaCompensating
			execution.Error = err
			o.mu.Unlock()

			compensateErr := o.compensate(execution, i-1)
			if compensateErr != nil {
				// Compensation failed — this is serious!
				// In production: alert operators, log for manual recovery
				fmt.Printf("Saga %s: compensation failed: %v\n",
					execution.ID, compensateErr)
			}

			o.mu.Lock()
			execution.State = SagaFailed
			execution.CompletedAt = time.Now()
			o.mu.Unlock()

			return fmt.Errorf("saga: step %d (%s) failed: %w",
				i, step.Name, err)
		}
	}

	o.mu.Lock()
	execution.State = SagaCompleted
	execution.CompletedAt = time.Now()
	o.mu.Unlock()

	return nil
}

// executeStepWithRetry executes a step with retry logic
func (o *SagaOrchestrator) executeStepWithRetry(
	ctx context.Context,
	execution *SagaExecution,
	step SagaStep,
) error {
	retryCount := step.RetryCount
	if retryCount == 0 {
		retryCount = o.defaultRetryCount
	}

	retryDelay := step.RetryDelay
	if retryDelay == 0 {
		retryDelay = o.defaultRetryDelay
	}

	var lastErr error
	for attempt := 0; attempt <= retryCount; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(retryDelay):
			case <-ctx.Done():
				return ctx.Err()
			}
			// Exponential backoff
			retryDelay *= 2
		}

		lastErr = step.Execute(ctx, execution.Data)
		if lastErr == nil {
			return nil
		}

		fmt.Printf("Saga %s step %s: attempt %d failed: %v\n",
			execution.ID, step.Name, attempt+1, lastErr)
	}

	return lastErr
}

// compensate executes compensation for steps in reverse order
func (o *SagaOrchestrator) compensate(
	execution *SagaExecution,
	upToStep int,
) error {
	for i := upToStep; i >= 0; i-- {
		step := execution.Steps[i]

		if step.Compensate == nil {
			// No compensation defined — log warning
			fmt.Printf("Saga %s: no compensation for step %s\n",
				execution.ID, step.Name)
			continue
		}

		err := step.Compensate(context.Background(), execution.Data)
		if err != nil {
			// Compensation failed — this is serious
			// In production: this requires manual intervention
			return fmt.Errorf("compensation for step %s failed: %w",
				step.Name, err)
		}

		fmt.Printf("Saga %s: compensated step %s\n",
			execution.ID, step.Name)
	}

	return nil
}

// ExecuteAsync starts saga execution in background
func (o *SagaOrchestrator) ExecuteAsync(
	ctx context.Context,
	execution *SagaExecution,
) <-chan error {
	resultCh := make(chan error, 1)

	go func() {
		err := o.Execute(ctx, execution)
		resultCh <- err
	}()

	return resultCh
}

// GetStatus returns the current status of a saga
func (o *SagaOrchestrator) GetStatus(sagaID string) (*SagaExecution, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	execution, exists := o.sagas[sagaID]
	return execution, exists
}

// ─────────────────────────────────────────────────────────────────────────────
// SAGA EXAMPLE: E-COMMERCE ORDER
// ─────────────────────────────────────────────────────────────────────────────

// CreateOrderSaga creates a sample e-commerce order saga
func CreateOrderSaga() []SagaStep {
	return []SagaStep{
		{
			Name: "Reserve Inventory",
			Execute: func(ctx context.Context, data map[string]interface{}) error {
				// Call inventory service
				// data["order_id"], data["product_id"], data["quantity"]
				fmt.Println("  [Saga] Reserving inventory...")
				time.Sleep(50 * time.Millisecond)
				// Simulate success
				data["inventory_reserved"] = true
				return nil
			},
			Compensate: func(ctx context.Context, data map[string]interface{}) error {
				fmt.Println("  [Saga] Releasing inventory...")
				time.Sleep(50 * time.Millisecond)
				return nil
			},
			RetryCount: 2,
		},
		{
			Name: "Charge Credit Card",
			Execute: func(ctx context.Context, data map[string]interface{}) error {
				fmt.Println("  [Saga] Charging credit card...")
				time.Sleep(50 * time.Millisecond)
				data["payment_charged"] = true
				return nil
			},
			Compensate: func(ctx context.Context, data map[string]interface{}) error {
				fmt.Println("  [Saga] Refunding payment...")
				time.Sleep(50 * time.Millisecond)
				return nil
			},
		},
		{
			Name: "Create Shipment",
			Execute: func(ctx context.Context, data map[string]interface{}) error {
				fmt.Println("  [Saga] Creating shipment...")
				time.Sleep(50 * time.Millisecond)
				data["shipment_created"] = true
				return nil
			},
			Compensate: func(ctx context.Context, data map[string]interface{}) error {
				fmt.Println("  [Saga] Canceling shipment...")
				time.Sleep(50 * time.Millisecond)
				return nil
			},
		},
		{
			Name: "Send Confirmation",
			Execute: func(ctx context.Context, data map[string]interface{}) error {
				fmt.Println("  [Saga] Sending confirmation email...")
				time.Sleep(50 * time.Millisecond)
				return nil
			},
			// No compensation needed for sending email
			Compensate: nil,
		},
	}
}
