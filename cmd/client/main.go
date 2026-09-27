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

	// Test GetSeat.
	getResponse, err := client.GetSeat(
		ctx,
		&movie.GetSeatRequest{
			ShowId: "show-101",
			SeatId: "B10",
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

	// Test BookSeat.
	bookResponse, err := client.BookSeat(
		ctx,
		&movie.BookSeatRequest{
			ShowId:    "show-101",
			SeatId:    "B10",
			UserId:    "user-002",
			RequestId: "req-002",
		},
	)
	if err != nil {
		log.Fatalf("BookSeat failed: %v", err)
	}

	log.Printf(
		"BookSeat response: booking=%s show=%s seat=%s status=%s message=%s",
		bookResponse.GetBookingId(),
		bookResponse.GetShowId(),
		bookResponse.GetSeatId(),
		bookResponse.GetStatus(),
		bookResponse.GetMessage(),
	)
}
