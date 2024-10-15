package main

import (
	"context"
	"log"
	"math/rand/v2"
	"net"

	pb "ch3/svc/protos/ordermgt/v1"

	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	port = ":50051"
)

type server struct {
	pb.UnimplementedOrderManagementServiceServer
}

var _ pb.OrderManagementServiceServer = (*server)(nil)

func (s *server) GetOrder(ctx context.Context, orderId *wrappers.StringValue) (*pb.GetOrderResponse, error) {
	log.Printf("Get order id = \"%s\"\n", orderId)
	return &pb.GetOrderResponse{Id: uuid.NewString(), Price: rand.Float32() * 5}, status.New(codes.OK, "").Err()
}

func (s *server) GetOrders(req *pb.GetOrdersRequest, stream grpc.ServerStreamingServer[pb.GetOrdersResponse]) error {
	nOrders := rand.IntN(7) + 1
	log.Printf("Get %d orders\n", nOrders)
	for i := 0; i < nOrders; i++ {
		if err := stream.Send(&pb.GetOrdersResponse{Id: uuid.NewString(), Price: rand.Float32() * 5}); err != nil {
			return err
		}
	}
	return status.New(codes.OK, "").Err()
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterOrderManagementServiceServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
