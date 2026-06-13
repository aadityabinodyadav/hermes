package transport

import (
	"context"
	"time"

	"google.golang.org/grpc/metadata"
)

type ContextKey string

const RequestIDKey ContextKey = "request_id"

func LoggingInterceptor(
	ctx context.Context,
) (interface{}, error) {
	since := time.Now()

	reqID := "unknown"

	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if ids := md.Get("x-request-id"); len(ids) > 0 {
			reqID = ids[0]
		}
	}

}
