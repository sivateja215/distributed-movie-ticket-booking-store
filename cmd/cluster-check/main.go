package main

import (
	"context"
	"log"
	"time"

	movie "moviekv/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func checkSeat(nodeID string, address string, showID string, seatID string) {
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
			ShowId: showID,
			SeatId: seatID,
		},
	)
	if err != nil {
		log.Printf(
			"[%s] GetSeat failed for %s: %v",
			nodeID,
			seatID,
			err,
		)
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
	log.Println("Checking C6 and C7 across all Raft nodes...")

	nodes := []struct {
		id      string
		address string
	}{
		{"node1", "localhost:50051"},
		{"node2", "localhost:50052"},
		{"node3", "localhost:50053"},
	}

	for _, node := range nodes {
		checkSeat(
			node.id,
			node.address,
			"show-102",
			"C6",
		)

		checkSeat(
			node.id,
			node.address,
			"show-102",
			"C7",
		)
	}

	log.Println("Final 3-node booking consistency verification completed")
}
