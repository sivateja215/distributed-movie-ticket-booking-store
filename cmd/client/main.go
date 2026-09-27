package main

import (
	"context"
	"log"
	"time"

	movie "moviekv/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	defer conn.Close()

	client := movie.NewMovieTicketServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	// Check the seat that was committed through Raft.
	getResponse, err := client.GetSeat(
		ctx,
		&movie.GetSeatRequest{
			ShowId: "show-101",
			SeatId: "A10",
		},
	)
	if err != nil {
		log.Fatalf("GetSeat failed: %v", err)
	}

	log.Printf(
		"GetSeat response: show=%s seat=%s status=%s",
		getResponse.GetShowId(),
		getResponse.GetSeatId(),
		getResponse.GetStatus(),
	)
}
