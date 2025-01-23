package telemetry

import (
	"context"
	"time"

	"connectrpc.com/connect"
)

func NewRPCInterceptor(clientName string) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			start := time.Now()
			endpoint := req.Spec().Procedure

			rpcInFlight.WithLabelValues(clientName, endpoint).Inc()
			defer rpcInFlight.WithLabelValues(clientName, endpoint).Dec()

			resp, err := next(ctx, req)

			rpcDuration.WithLabelValues(clientName, endpoint).Observe(time.Since(start).Seconds())

			return resp, err
		}
	}
}
