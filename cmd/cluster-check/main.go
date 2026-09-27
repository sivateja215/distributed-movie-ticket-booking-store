package main

import (
	"context"
	"log"
	"time"

	movie "moviekv/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func checkNode(nodeID string, address string) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		log.Printf("[%s] connection failed: %v", nodeID, err)
		return
	}

	defer conn.Close()

	client := movie.NewMovieTicketServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	response, err := client.GetSeat(
		ctx,
		&movie.GetSeatRequest{
			ShowId: "show-101",
			SeatId: "A10",
		},
	)
	if err != nil {
		log.Printf("[%s] GetSeat failed: %v", nodeID, err)
		return
	}

	log.Printf(
		"[%s] GetSeat: show=%s seat=%s status=%s",
		nodeID,
		response.GetShowId(),
		response.GetSeatId(),
		response.GetStatus(),
	)
}

func main() {
	log.Println("Checking A10 across all Raft nodes...")

	checkNode("node1", "localhost:50051")
	checkNode("node2", "localhost:50052")
	checkNode("node3", "localhost:50053")

	log.Println("3-node cluster verification completed")
}
