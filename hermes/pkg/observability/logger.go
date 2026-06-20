// pkg/observability/logger.go
package observability

// StructuredLogger provides structured logging for Hermes
//
// Every log entry is a JSON object with consistent fields:
//   - time: ISO 8601 timestamp
//   - level: debug/info/warn/error
//   - msg: human-readable message
//   - node_id: which node logged this
//   - trace_id: for correlation with traces
//   - component: which subsystem (raft, wal, membership, etc.)
//   - <context-specific fields>

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

// LogLevel represents the severity of a log entry
type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

func (l LogLevel) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	case LevelFatal:
		return "fatal"
	}
	return "unknown"
}

// LogEntry is a single structured log entry
type LogEntry struct {
	// Standard fields (always present)
	Time      string `json:"time"`
	Level     string `json:"level"`
	Msg       string `json:"msg"`
	NodeID    string `json:"node_id,omitempty"`
	TraceID   string `json:"trace_id,omitempty"`
	Component string `json:"component,omitempty"`

	// Caller information
	Caller string `json:"caller,omitempty"`

	// Context-specific fields
	Fields map[string]interface{} `json:"fields,omitempty"`

	// Error (if any)
	Error string `json:"error,omitempty"`
}

// Logger is a structured logger
type Logger struct {
	mu        sync.Mutex
	nodeID    string
	component string
	level     LogLevel
	output    io.Writer
	fields    map[string]interface{} // default fields
}

// NewLogger creates a new structured logger
func NewLogger(nodeID, component string) *Logger {
	return &Logger{
		nodeID:    nodeID,
		component: component,
		level:     LevelInfo,
		output:    os.Stdout,
		fields:    make(map[string]interface{}),
	}
}

// WithLevel sets the minimum log level
func (l *Logger) WithLevel(level LogLevel) *Logger {
	return &Logger{
		nodeID:    l.nodeID,
		component: l.component,
		level:     level,
		output:    l.output,
		fields:    l.copyFields(),
	}
}

// WithComponent creates a child logger with a different component
func (l *Logger) WithComponent(component string) *Logger {
	return &Logger{
		nodeID:    l.nodeID,
		component: component,
		level:     l.level,
		output:    l.output,
		fields:    l.copyFields(),
	}
}

// WithFields creates a child logger with additional fields
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	newFields := l.copyFields()
	for k, v := range fields {
		newFields[k] = v
	}
	return &Logger{
		nodeID:    l.nodeID,
		component: l.component,
		level:     l.level,
		output:    l.output,
		fields:    newFields,
	}
}

// WithTraceID creates a child logger with a trace ID
func (l *Logger) WithTraceID(traceID string) *Logger {
	return l.WithFields(map[string]interface{}{"trace_id": traceID})
}

// Debug logs at DEBUG level
func (l *Logger) Debug(msg string, fields ...map[string]interface{}) {
	l.log(LevelDebug, msg, nil, fields...)
}

// Info logs at INFO level
func (l *Logger) Info(msg string, fields ...map[string]interface{}) {
	l.log(LevelInfo, msg, nil, fields...)
}

// Warn logs at WARN level
func (l *Logger) Warn(msg string, fields ...map[string]interface{}) {
	l.log(LevelWarn, msg, nil, fields...)
}

// Error logs at ERROR level with optional error
func (l *Logger) Error(msg string, err error, fields ...map[string]interface{}) {
	l.log(LevelError, msg, err, fields...)
}

// Fatal logs at FATAL level and exits
func (l *Logger) Fatal(msg string, err error, fields ...map[string]interface{}) {
	l.log(LevelFatal, msg, err, fields...)
	os.Exit(1)
}

func (l *Logger) log(level LogLevel, msg string, err error, extraFields ...map[string]interface{}) {
	if level < l.level {
		return
	}

	// Merge fields
	allFields := l.copyFields()
	for _, ef := range extraFields {
		for k, v := range ef {
			allFields[k] = v
		}
	}

	// Get caller info (for debugging)
	caller := ""
	if level >= LevelWarn {
		_, file, line, ok := runtime.Caller(2)
		if ok {
			// Trim path to just package/file.go
			parts := strings.Split(file, "/")
			if len(parts) > 2 {
				file = strings.Join(parts[len(parts)-2:], "/")
			}
			caller = fmt.Sprintf("%s:%d", file, line)
		}
	}

	entry := LogEntry{
		Time:      time.Now().UTC().Format(time.RFC3339Nano),
		Level:     level.String(),
		Msg:       msg,
		NodeID:    l.nodeID,
		Component: l.component,
		Caller:    caller,
	}

	if len(allFields) > 0 {
		entry.Fields = allFields
	}

	if err != nil {
		entry.Error = err.Error()
	}

	// Extract trace_id if present in fields
	if traceID, ok := allFields["trace_id"].(string); ok {
		entry.TraceID = traceID
		delete(entry.Fields, "trace_id")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	data, jsonErr := json.Marshal(entry)
	if jsonErr != nil {
		fmt.Fprintf(l.output, `{"level":"error","msg":"failed to marshal log entry","error":%q}`+"\n",
			jsonErr.Error())
		return
	}

	fmt.Fprintf(l.output, "%s\n", data)
}

func (l *Logger) copyFields() map[string]interface{} {
	result := make(map[string]interface{}, len(l.fields))
	for k, v := range l.fields {
		result[k] = v
	}
	return result
}

// ─────────────────────────────────────────────────────────────────────────────
// COMPONENT-SPECIFIC LOGGERS
// These provide type-safe, domain-specific logging
// ─────────────────────────────────────────────────────────────────────────────

// RaftLogger logs Raft-specific events
type RaftLogger struct {
	log *Logger
}

func NewRaftLogger(nodeID string) *RaftLogger {
	return &RaftLogger{
		log: NewLogger(nodeID, "raft"),
	}
}

func (r *RaftLogger) LeaderElected(term uint64, duration time.Duration, votesReceived, clusterSize int) {
	r.log.Info("leader elected", map[string]interface{}{
		"term":                 term,
		"election_duration_ms": duration.Milliseconds(),
		"votes_received":       votesReceived,
		"cluster_size":         clusterSize,
	})
}

func (r *RaftLogger) LeaderStepDown(term uint64, reason string) {
	r.log.Warn("leader stepping down", map[string]interface{}{
		"term":   term,
		"reason": reason,
	})
}

func (r *RaftLogger) EntryCommitted(index, term uint64, commitLatency time.Duration) {
	r.log.Debug("log entry committed", map[string]interface{}{
		"index":             index,
		"term":              term,
		"commit_latency_ms": commitLatency.Milliseconds(),
	})
}

func (r *RaftLogger) SnapshotInstalled(index, term uint64, size int64) {
	r.log.Info("snapshot installed", map[string]interface{}{
		"index":      index,
		"term":       term,
		"size_bytes": size,
	})
}

func (r *RaftLogger) ReplicationLag(followerID string, lag uint64) {
	level := LevelDebug
	if lag > 100 {
		level = LevelWarn
	}
	if lag > 1000 {
		level = LevelError
	}

	switch level {
	case LevelWarn:
		r.log.Warn("follower replication lag", map[string]interface{}{
			"follower_id": followerID,
			"lag_entries": lag,
		})
	case LevelError:
		r.log.Error("critical follower replication lag",
			fmt.Errorf("follower %s is %d entries behind", followerID, lag),
			map[string]interface{}{
				"follower_id": followerID,
				"lag_entries": lag,
			})
	default:
		r.log.Debug("follower replication lag", map[string]interface{}{
			"follower_id": followerID,
			"lag_entries": lag,
		})
	}
}

// WALLogger logs WAL-specific events
type WALLogger struct {
	log *Logger
}

func NewWALLogger(nodeID string) *WALLogger {
	return &WALLogger{
		log: NewLogger(nodeID, "wal"),
	}
}

func (w *WALLogger) WriteComplete(seq uint64, bytes int, duration time.Duration) {
	if duration > 10*time.Millisecond {
		w.log.Warn("slow WAL write", map[string]interface{}{
			"sequence":    seq,
			"bytes":       bytes,
			"duration_ms": duration.Milliseconds(),
		})
		return
	}
	w.log.Debug("WAL write complete", map[string]interface{}{
		"sequence":    seq,
		"bytes":       bytes,
		"duration_ms": duration.Milliseconds(),
	})
}

func (w *WALLogger) FsyncComplete(duration time.Duration, batchSize int) {
	if duration > 50*time.Millisecond {
		w.log.Error("very slow WAL fsync",
			fmt.Errorf("fsync took %v (> 50ms threshold)", duration),
			map[string]interface{}{
				"duration_ms": duration.Milliseconds(),
				"batch_size":  batchSize,
			})
		return
	}
	w.log.Debug("WAL fsync complete", map[string]interface{}{
		"duration_ms": duration.Milliseconds(),
		"batch_size":  batchSize,
	})
}

func (w *WALLogger) SegmentRotated(oldID, newID uint64, size int64) {
	w.log.Info("WAL segment rotated", map[string]interface{}{
		"old_segment_id": oldID,
		"new_segment_id": newID,
		"old_size_bytes": size,
	})
}

// MembershipLogger logs membership events
type MembershipLogger struct {
	log *Logger
}

func NewMembershipLogger(nodeID string) *MembershipLogger {
	return &MembershipLogger{
		log: NewLogger(nodeID, "membership"),
	}
}

func (m *MembershipLogger) NodeJoined(nodeID, address string) {
	m.log.Info("node joined cluster", map[string]interface{}{
		"joined_node_id": nodeID,
		"address":        address,
	})
}

func (m *MembershipLogger) NodeSuspected(nodeID string, phi float64) {
	m.log.Warn("node suspected dead", map[string]interface{}{
		"suspected_node": nodeID,
		"phi_value":      phi,
	})
}

func (m *MembershipLogger) NodeDead(nodeID string, suspicionDuration time.Duration) {
	m.log.Error("node confirmed dead",
		fmt.Errorf("node %s is dead after %v suspicion", nodeID, suspicionDuration),
		map[string]interface{}{
			"dead_node":          nodeID,
			"suspicion_duration": suspicionDuration.String(),
		})
}

func (m *MembershipLogger) NodeRevived(nodeID string) {
	m.log.Info("previously dead node revived", map[string]interface{}{
		"revived_node": nodeID,
	})
}
