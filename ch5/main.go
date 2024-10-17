package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"

	pb "ch3/svc/protos/ordermgt/v1"

	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	port = ":50051"
)

type server struct {
	pb.UnimplementedOrderManagementServiceServer
}

var _ pb.OrderManagementServiceServer = (*server)(nil)

type Order struct {
	Id    string
	Price float32
}

var (
	orders = map[string]Order{}
)

func (s *server) CreateOrder(ctx context.Context, req *pb.CreateOrderRequest) (*pb.CreateOrderResponse, error) {
	log.Printf("Create order with price = %.2f", req.Price)
	id := uuid.NewString()
	orders[id] = Order{Id: id, Price: req.Price}
	return &pb.CreateOrderResponse{Id: id, Price: req.Price}, nil
}

func (s *server) CreateOrders(stream grpc.ClientStreamingServer[pb.CreateOrdersRequest, pb.CreateOrdersResponse]) error {
	log.Print("Create orders")
	var createdOrdersIds []string
	for {
		orderReq, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			log.Printf("Created %d orders", len(createdOrdersIds))
			err := stream.SendAndClose(&pb.CreateOrdersResponse{CreatedOrders: createdOrdersIds})
			if err != nil {
				return fmt.Errorf("failed to close CreateOrders stream: %v", err)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to receive CreateOrders request: %v", err)
		}

		orderId := uuid.NewString()
		orders[orderId] = Order{Id: orderId, Price: orderReq.Price}
		createdOrdersIds = append(createdOrdersIds, orderId)
	}
}

func (s *server) GetOrder(ctx context.Context, orderId *wrappers.StringValue) (*pb.GetOrderResponse, error) {
	log.Printf("Get order id = \"%s\"\n", orderId)

	order, ok := orders[orderId.String()]
	if !ok {
		return nil, status.New(codes.NotFound, fmt.Sprintf("order id=\"%s\" not found", orderId.String())).Err()
	}

	return &pb.GetOrderResponse{Id: order.Id, Price: order.Price}, status.New(codes.OK, "").Err()
}

func (s *server) GetOrders(_ *emptypb.Empty, stream grpc.ServerStreamingServer[pb.GetOrdersResponse]) error {
	for _, order := range orders {
		if err := stream.Send(&pb.GetOrdersResponse{Id: order.Id, Price: order.Price}); err != nil {
			return err
		}
	}
	return status.New(codes.OK, "").Err()
}

func (s *server) PackOrders(stream grpc.BidiStreamingServer[pb.PackOrdersRequest, pb.PackOrdersResponse]) error {
	var packedOrders []*pb.PackedOrder
	packSize := rand.IntN(3) + 2
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if err := stream.Send(&pb.PackOrdersResponse{Orders: packedOrders}); err != nil {
				return fmt.Errorf("failed to send PackOrders response: %v", err)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to receive PackOrders request from stream: %v", err)
		}
		order, ok := orders[req.Id]
		if !ok {
			return status.New(codes.NotFound, fmt.Sprintf("Order id=\"%s\" not found", req.Id)).Err()
		}
		packedOrders = append(packedOrders, &pb.PackedOrder{Id: req.Id, Price: order.Price})
		if len(packedOrders) == packSize {
			if err := stream.Send(&pb.PackOrdersResponse{Orders: packedOrders}); err != nil {
				return fmt.Errorf("failed to send PackOrders response: %v", err)
			}
			packSize = rand.IntN(3) + 2
			packedOrders = nil
		}
	}
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
