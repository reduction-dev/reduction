package rpc

import (
	"context"
	"log/slog"

	"connectrpc.com/connect"
)

func NewLoggingInterceptor(logger *slog.Logger, level slog.Level) connect.UnaryInterceptorFunc {
	interceptor := func(next connect.UnaryFunc) connect.UnaryFunc {
		return connect.UnaryFunc(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			logger.Log(ctx, level, "connect request", "url", req.Spec().Procedure, "msg", req.Any())
			return next(ctx, req)
		})
	}

	return connect.UnaryInterceptorFunc(interceptor)
}
