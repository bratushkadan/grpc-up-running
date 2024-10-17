package interceptors

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
)

func OrderUnaryInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// Preprocessing phase
	log.Print("[Client Unary Interceptor] Method: ", method)

	// Invoking the remote method
	if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
		return fmt.Errorf("[Client Unary Interceptor] Error executing unary interceptor: %w", err)
	}

	// Postprocessing phase
	log.Print(reply)

	return nil
}

func OrderStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	log.Print("[Client Stream Interceptor] ", method)
	s, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return nil, err
	}
	return newWrappedStream(s), nil
}

type wrappedStream struct {
	grpc.ClientStream
}

func newWrappedStream(s grpc.ClientStream) *wrappedStream {
	return &wrappedStream{ClientStream: s}
}

func (w *wrappedStream) RecvMsg(m any) error {
	log.Printf("[Client Stream Interceptor - Receive] Message (Type: %T) at %v", m, time.Now().Format(time.RFC3339))
	return w.ClientStream.RecvMsg(m)
}

func (w *wrappedStream) SendMsg(m any) error {
	log.Printf("[Client Stream Interceptor - Send] Message (Type: %T) at %v", m, time.Now().Format(time.RFC3339))
	return w.ClientStream.SendMsg(m)
}
