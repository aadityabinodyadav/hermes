// pkg/observability/tracing.go
package observability

// DistributedTracing implements OpenTelemetry-compatible tracing
//
// A trace represents ONE user request flowing through the system.
// A span represents ONE operation within that request.
//
// Example trace for PUT "balance:alice" = 100:
//
//   Trace: PUT balance:alice  (TraceID: abc123)
//   │
//   ├── Span: gRPC.Put  (2ms)
//   │     node_id: hermes-0
//   │     status: ok
//   │
//   ├── Span: Router.Route  (0.1ms)
//   │     key: balance:alice
//   │     shard_id: 0
//   │     target: hermes-0
//   │
//   ├── Span: Raft.Propose  (11ms)
//   │     term: 5
//   │     log_index: 1042
//   │
//   │   ├── Span: WAL.Write  (3ms)
//   │   │     bytes: 128
//   │   │     fsync: true
//   │   │
//   │   └── Span: Raft.Replicate  (8ms)
//   │         follower_count: 2
//   │         quorum_at: 5ms
//   │
//   └── Span: Storage.Apply  (0.5ms)
//         key: balance:alice
//         bytes: 64

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// TraceID is a 128-bit trace identifier
type TraceID [16]byte

// SpanID is a 64-bit span identifier
type SpanID [8]byte

func (t TraceID) String() string {
	return fmt.Sprintf("%x", t[:])
}

func (s SpanID) String() string {
	return fmt.Sprintf("%x", s[:])
}

// SpanContext carries the trace context across process boundaries
// This gets propagated via gRPC metadata
type SpanContext struct {
	TraceID TraceID
	SpanID  SpanID
	Sampled bool // Whether this trace is being recorded
}

// Span represents one operation in a trace
type Span struct {
	mu sync.Mutex

	// Context
	TraceID  TraceID
	SpanID   SpanID
	ParentID SpanID // zero if root span
	Name     string
	Sampled  bool   // Whether this span/trace is being sampled

	// Timing
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration

	// Metadata
	NodeID     string
	Service    string
	Attributes map[string]interface{}
	Events     []SpanEvent
	Status     SpanStatus
	Error      error

	// Children
	Children []*Span

	// Tracer reference
	tracer *Tracer
}

type SpanStatus uint8

const (
	StatusOK       SpanStatus = 0
	StatusError    SpanStatus = 1
	StatusCanceled SpanStatus = 2
)

// SpanEvent is a timestamped note within a span
type SpanEvent struct {
	Name       string
	Time       time.Time
	Attributes map[string]interface{}
}

// SetAttribute adds a key-value attribute to the span
func (s *Span) SetAttribute(key string, value interface{}) *Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Attributes == nil {
		s.Attributes = make(map[string]interface{})
	}
	s.Attributes[key] = value
	return s
}

// AddEvent adds a timestamped event to the span
func (s *Span) AddEvent(name string, attrs map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Events = append(s.Events, SpanEvent{
		Name:       name,
		Time:       time.Now(),
		Attributes: attrs,
	})
}

// SetError marks the span as failed
func (s *Span) SetError(err error) *Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Status = StatusError
	s.Error = err
	return s
}

// End completes the span
func (s *Span) End() {
	s.mu.Lock()
	s.EndTime = time.Now()
	s.Duration = s.EndTime.Sub(s.StartTime)
	s.mu.Unlock()

	// Export to tracer
	if s.tracer != nil {
		s.tracer.export(s)
	}
}

// contextKey for span in context
type tracingKey struct{}

// ─────────────────────────────────────────────────────────────────────────────

// Tracer creates and manages spans
type Tracer struct {
	mu       sync.Mutex
	nodeID   string
	service  string
	sampler  Sampler
	spans    []*Span // for demo: store in memory
	exporter SpanExporter
}

// Sampler decides whether to record a trace
type Sampler interface {
	ShouldSample(traceID TraceID) bool
}

// AlwaysSample records all traces (use for testing, not production!)
type AlwaysSample struct{}

func (s AlwaysSample) ShouldSample(_ TraceID) bool { return true }

// RateSampler samples a percentage of traces
type RateSampler struct {
	rate float64
}

func NewRateSampler(rate float64) *RateSampler {
	return &RateSampler{rate: rate}
}

func (s *RateSampler) ShouldSample(traceID TraceID) bool {
	// Use first byte of trace ID as a random value
	return float64(traceID[0]) < s.rate*256
}

// SpanExporter exports spans to a backend (Jaeger, Zipkin, etc.)
type SpanExporter interface {
	Export(span *Span)
}

// NewTracer creates a new tracer
func NewTracer(nodeID, service string) *Tracer {
	return &Tracer{
		nodeID:  nodeID,
		service: service,
		sampler: AlwaysSample{},
	}
}

// Start creates a new span
// If ctx contains a parent span, this becomes a child span
func (t *Tracer) Start(ctx context.Context, name string) (*Span, context.Context) {
	span := &Span{
		Name:      name,
		StartTime: time.Now(),
		NodeID:    t.nodeID,
		Service:   t.service,
		tracer:    t,
	}

	// Check for parent span in context
	if parent := SpanFromContext(ctx); parent != nil {
		span.TraceID = parent.TraceID
		span.ParentID = parent.SpanID

		parent.mu.Lock()
		parent.Children = append(parent.Children, span)
		parent.mu.Unlock()
	} else {
		// New root span
		span.TraceID = newTraceID()
		span.Sampled = t.sampler.ShouldSample(span.TraceID)
	}

	span.SpanID = newSpanID()

	// Store span in context for child spans
	ctx = context.WithValue(ctx, tracingKey{}, span)

	return span, ctx
}

// SpanFromContext retrieves the current span from context
func SpanFromContext(ctx context.Context) *Span {
	span, _ := ctx.Value(tracingKey{}).(*Span)
	return span
}

func (t *Tracer) export(span *Span) {
	if !span.Sampled {
		return
	}

	if t.exporter != nil {
		t.exporter.Export(span)
	} else {
		// Default: keep in memory
		t.mu.Lock()
		t.spans = append(t.spans, span)
		t.mu.Unlock()
	}
}

// GetTraces returns all recorded traces (for testing/debugging)
func (t *Tracer) GetTraces() []*Span {
	t.mu.Lock()
	defer t.mu.Unlock()

	result := make([]*Span, len(t.spans))
	copy(result, t.spans)
	return result
}

// PrintTrace prints a trace in a human-readable format
func PrintTrace(span *Span, indent int) {
	prefix := strings.Repeat("  ", indent)
	icon := "├──"
	if indent == 0 {
		icon = "┌──"
	}

	statusIcon := "✅"
	if span.Status == StatusError {
		statusIcon = "❌"
	}

	fmt.Printf("%s%s %s %s [%v]\n",
		prefix, icon, span.Name, statusIcon, span.Duration.Round(time.Microsecond))

	// Print attributes
	for k, v := range span.Attributes {
		fmt.Printf("%s      %s: %v\n", prefix, k, v)
	}

	// Print events
	for _, event := range span.Events {
		fmt.Printf("%s      📍 %s at T+%v\n", prefix, event.Name,
			event.Time.Sub(span.StartTime).Round(time.Microsecond))
	}

	// Print children
	for _, child := range span.Children {
		PrintTrace(child, indent+1)
	}
}

func newTraceID() TraceID {
	var id TraceID
	rand.Read(id[:])
	return id
}

func newSpanID() SpanID {
	var id SpanID
	rand.Read(id[:])
	return id
}
