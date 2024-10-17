package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"strings"
	"time"

	pb "ch3/svc/protos/ordermgt/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	address = "localhost:50051"
)

func main() {
	// FillWithOrders(100)

	orders := receiveOrders()
	packOrdersWithDuplexStreaming(orders)
}

func FillWithOrders(n int) {
	orders := make([]Order, 100)

	for i := 0; i < n; i++ {
		orders[i] = Order{Price: rand.Float32() * 75}
	}
	addOrdersStreaming(orders)
}

func speedTestClientUnaryVsStreaming() {
	orders := []Order{{1.15}, {3.44}, {6.26}, {5.52}, {0.37}}
	start := time.Now()
	addOrdersUnary(orders)
	unaryDur := time.Now().Sub(start)
	start = time.Now()
	addOrdersStreaming(orders)
	streamingDur := time.Now().Sub(start)

	fmt.Printf("addOrdersUnary took %v, addOrdersStreaming took %v", unaryDur, streamingDur)
	// addOrdersUnary took 44.634625ms, addOrdersStreaming took 20.9065ms
	// addOrdersUnary took 71.167459ms, addOrdersStreaming took 24.701834ms
}

type Order struct {
	Price float32
}

func packOrdersWithDuplexStreaming(orders []*pb.GetOrdersResponse) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()
	c := pb.NewOrderManagementServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := c.PackOrders(ctx)

	done := make(chan struct{})

	go printPackedOrders(done, stream)

	for _, o := range orders {
		if err := stream.Send(&pb.PackOrdersRequest{Id: o.Id}); err != nil {
			log.Fatalf("Couldn't send PackOrdersRequest: %v", err)
		}
	}
	log.Print("Sent all the orders for packing")

	if err := stream.CloseSend(); err != nil {
		log.Printf("Failed to close PackOrders request stream: %v", err)
	}

	<-done
}

func printPackedOrders(done chan<- struct{}, clientStream grpc.BidiStreamingClient[pb.PackOrdersRequest, pb.PackOrdersResponse]) {
	for {
		resp, err := clientStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Printf("[Error] Couldn't receive PackedOrders response: %v", err)
			break
		}
		packedOrderInfo := &strings.Builder{}
		packedOrderInfo.WriteString("Packed order: [")
		for i, o := range resp.Orders {
			fmt.Fprintf(packedOrderInfo, "{Id: %s, Price: %.2f}", o.Id, o.Price)
			if i != len(resp.Orders)-1 {
				packedOrderInfo.WriteString(", ")
			}
		}
		packedOrderInfo.WriteByte(']')
		log.Print(packedOrderInfo.String())
	}
	done <- struct{}{}
}

func addOrdersUnary(orders []Order) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()
	c := pb.NewOrderManagementServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for _, order := range orders {
		resp, err := c.CreateOrder(ctx, &pb.CreateOrderRequest{Price: order.Price})
		if err != nil {
			log.Fatalf("Failed to create order: %v", err)
		}
		log.Printf("Created order: id=\"%s\" price=\"%.2f\"", resp.Id, resp.Price)
	}
	log.Printf("Created all orders!")
}

func addOrdersStreaming(orders []Order) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()
	c := pb.NewOrderManagementServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := c.CreateOrders(ctx)
	if err != nil {
		log.Fatalf("Failed to open CreateOrders request stream: %v", err)
	}

	for _, order := range orders {
		if err := stream.Send(&pb.CreateOrdersRequest{Price: order.Price}); err != nil {
			log.Fatalf("Failed to create order: %v", err)
		}
		log.Printf("Placed with price=\"%.2f\"", order.Price)
	}
	log.Printf("Placed all orders!")
	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("error placing orders: %v", err)
	}
	log.Printf("Created orders: [%s]", strings.Join(resp.CreatedOrders, ", "))

}

func receiveOrders() []*pb.GetOrdersResponse {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()
	c := pb.NewOrderManagementServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ordersReq, err := c.GetOrders(ctx, &emptypb.Empty{})

	done := make(chan struct{})

	var orders []*pb.GetOrdersResponse

	go func() {
		for i := 0; ; i++ {

			o, err := ordersReq.Recv()
			if errors.Is(err, io.EOF) {
				done <- struct{}{}
				return
			}
			if err != nil {
				log.Fatal("cannot receive order")
			}
			orders = append(orders, o)
			log.Printf("Order %d: id=\"%s\", price = %.2f", i+1, o.Id, o.Price)
		}
	}()

	<-done
	fmt.Println("Received all orders")
	return orders
}
