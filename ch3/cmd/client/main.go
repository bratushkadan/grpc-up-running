package main

import (
	"context"
	"errors"
	"io"
	"log"
	"time"

	pb "ch3/svc/protos/ordermgt/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	address = "localhost:50051"
)

func main() {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()
	c := pb.NewOrderManagementServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	orders, err := c.GetOrders(ctx, &pb.GetOrdersRequest{})

	done := make(chan struct{})

	go func() {
		for i := 0; ; i++ {

			o, err := orders.Recv()
			if errors.Is(err, io.EOF) {
				done <- struct{}{}
				return
			}
			if err != nil {
				log.Fatal("cannot receive order")
			}
			log.Printf("Order %d: id=\"%s\", price = %.2f", i+1, o.Id, o.Price)
		}
	}()

	<-done
}
