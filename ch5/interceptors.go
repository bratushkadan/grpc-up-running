package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
)

func orderUnaryServerInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	log.Print("=== [Server Interceptor] ", info.FullMethod)

	// Invoking the handler to complete the normal execution of a unary RPC
	m, err := handler(ctx, req)

	// Post processing logic - process the response from the RPC invocation

	log.Printf("Post processing message: %v, %v", m, err)
	return m, err
}

// Server Streaming Interceptor
// wrappedStream wraps around the embedded grpc.ServerStream,
// and intercepts the RecvMsg and SendMsg method call.
type wrappedStream struct {
	grpc.ServerStream
}

func (w *wrappedStream) RecvMsg(m any) error {
	log.Printf("=== [Server Stream Interceptor Wrapper] Receive a message (Type: %T) at %s", m, time.Now().Format(time.RFC3339))
	return w.ServerStream.RecvMsg(m)
}
func (w *wrappedStream) SendMsg(m any) error {
	log.Printf("=== [Server Stream Interceptor Wrapper] Send a message (Type: %T) at %s", m, time.Now().Format(time.RFC3339))
	return w.ServerStream.SendMsg(m)
}

func newWrappedStream(s grpc.ServerStream) grpc.ServerStream {
	return &wrappedStream{ServerStream: s}
}

func orderStreamServerInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	log.Print("=== [Server Stream Interceptor] ", info.FullMethod, " clientStreaming = ", info.IsClientStream, ", ", "serverStreaming = ", info.IsServerStream)
	err := handler(srv, newWrappedStream(ss))
	if err != nil {
		log.Printf("RPC Failed with error %v", err)
	}
	return err
}
