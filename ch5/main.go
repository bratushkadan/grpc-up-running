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
	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
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
	if req.Price < 0 {
		log.Printf("Invalid CreateOrder price requested: %.2f", req.Price)
		errorStatus := status.New(codes.InvalidArgument, "")
		ds, err := errorStatus.WithDetails(
			&epb.BadRequest_FieldViolation{
				Field:       "price",
				Description: fmt.Sprintf("Price received (%f) is not valid - can't be negative", req.Price),
			},
		)
		if err != nil {
			// log the error generating details and return the base error
			log.Printf("error generating validation details: %v", err)
			return nil, errorStatus.Err()
		}
		// return the generated error
		return nil, ds.Err()
	}

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

func (s *server) GetOrder(ctx context.Context, pbOrderId *wrappers.StringValue) (*pb.GetOrderResponse, error) {
	orderId := pbOrderId.GetValue()
	log.Printf("Get order id = \"%s\"", orderId)

	// Deadline is propagated
	// deadline, ok := ctx.Deadline()
	// if ok {
	// 	log.Printf("GetOrder context deadline: %v", deadline)
	// }
	// log.Print("Now ", time.Now().Format(time.RFC1123Z))

	// Testing DeadlineExceeded/Canceled errors
	// time.Sleep(1200 * time.Millisecond)

	// log.Printf("Context deadline exceeded: %t", errors.Is(ctx.Err(), context.DeadlineExceeded))
	// log.Printf("Client RPC cancelled: %t", errors.Is(ctx.Err(), context.Canceled))

	order, ok := orders[orderId]
	if !ok {
		return nil, status.New(codes.NotFound, fmt.Sprintf("order id=\"%s\" not found", orderId)).Err()
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
			if len(packedOrders) == 0 {
				return nil
			}
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
	// Register the Interceptor at the server-side
	s := grpc.NewServer(
		grpc.UnaryInterceptor(orderUnaryServerInterceptor),
		grpc.StreamInterceptor(orderStreamServerInterceptor),
	)
	pb.RegisterOrderManagementServiceServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
