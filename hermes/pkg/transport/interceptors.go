// pkg/transport/interceptors.go
package transport

// Interceptors are middleware for gRPC
// They run on EVERY call - perfect for cross-cutting concerns
// Think of them like HTTP middleware but for RPCs
//
// Interceptor chain:
//   Client call
//      │
//      ▼
//   [Logging interceptor]
//      │
//      ▼
//   [Metrics interceptor]
//      │
//      ▼
//   [Timeout interceptor]
//      │
//      ▼
//   [Recovery interceptor]  ← catches panics
//      │
//      ▼
//   Actual handler

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type contextKey string

const RequestIDKey contextKey = "request_id"

func LoggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	start := time.Now()

	reqID := "unknown"
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if ids := md.Get("x-request-id"); len(ids) > 0 {
			reqID = ids[0]
		}
	}

	resp, err := handler(ctx, req)

	duration := time.Since(start)
	statusCode := codes.OK
	if err != nil {
		if st, ok := status.FromError(err); ok {
			statusCode = st.Code()
		}
	}

	if err != nil {
		fmt.Printf("ERROR | %s | req=%s | dur=%v | code=%s | err=%v\n",
			info.FullMethod, reqID, duration, statusCode, err)
	} else {
		fmt.Printf("INFO  | %s | req=%s | dur=%v | code=%s\n",
			info.FullMethod, reqID, duration, statusCode)
	}

	return resp, err
}

type MethodMetrics struct {
	calls    int64
	errors   int64
	totalDur time.Duration
}

type MetricsCollector struct {
	methods map[string]*MethodMetrics
}

func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		methods: make(map[string]*MethodMetrics),
	}
}

func (m *MetricsCollector) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)

		method := info.FullMethod
		if _, exists := m.methods[method]; !exists {
			m.methods[method] = &MethodMetrics{}
		}
		m.methods[method].calls++
		m.methods[method].totalDur += duration
		if err != nil {
			m.methods[method].errors++
		}

		return resp, err
	}
}

func RecoveryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("PANIC RECOVERED | method=%s | panic=%v\n%s\n",
				info.FullMethod, r, debug.Stack())

			err = status.Errorf(codes.Internal,
				"internal error: %v", r)
		}
	}()

	return handler(ctx, req)
}

func TimeoutInterceptor(defaultTimeout time.Duration) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		if _, hasDeadline := ctx.Deadline(); hasDeadline {
			return handler(ctx, req)
		}

		ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
		defer cancel()

		return handler(ctx, req)
	}
}

func ChainUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		chain := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			next := chain
			chain = func(ctx context.Context, req interface{}) (interface{}, error) {
				return interceptor(ctx, req, info, next)
			}
		}
		return chain(ctx, req)
	}
}

func ClientRequestIDInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		// Add request ID to outgoing metadata
		reqID := fmt.Sprintf("%d", time.Now().UnixNano())
		ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", reqID)

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func RetryInterceptor(maxAttempts int) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		var lastErr error

		for attempt := 0; attempt < maxAttempts; attempt++ {
			if attempt > 0 {
				backoff := time.Duration(10<<uint(attempt-1)) * time.Millisecond
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return ctx.Err()
				}
				fmt.Printf("RETRY | method=%s | attempt=%d | backoff=%v\n",
					method, attempt+1, backoff)
			}

			lastErr = invoker(ctx, method, req, reply, cc, opts...)
			if lastErr == nil {
				return nil
			}

			st, ok := status.FromError(lastErr)
			if !ok {
				return lastErr
			}

			switch st.Code() {
			case codes.Unavailable, codes.ResourceExhausted:
				continue
			default:
				return lastErr
			}
		}

		return fmt.Errorf("max retries (%d) exceeded: %w", maxAttempts, lastErr)
	}
}
