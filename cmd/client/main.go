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
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	defer conn.Close()

	client := movie.NewMovieTicketServiceClient(conn)

	// ------------------------------------------------------------
	// Test data
	// ------------------------------------------------------------

	showID := "show-102"
	seatID := "D1"
	userID := "user-persistence"
	requestID := "req-persistence-001"

	// ------------------------------------------------------------
	// 1. Check seat before booking
	// ------------------------------------------------------------

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	getResponse, err := client.GetSeat(
		ctx,
		&movie.GetSeatRequest{
			ShowId: showID,
			SeatId: seatID,
		},
	)

	if err != nil {
		log.Fatalf("initial GetSeat failed: %v", err)
	}

	log.Printf(
		"Before booking: show=%s seat=%s status=%s",
		getResponse.GetShowId(),
		getResponse.GetSeatId(),
		getResponse.GetStatus(),
	)

	// ------------------------------------------------------------
	// 2. Book the seat
	// ------------------------------------------------------------

	bookResponse, err := client.BookSeat(
		ctx,
		&movie.BookSeatRequest{
			ShowId:    showID,
			SeatId:    seatID,
			UserId:    userID,
			RequestId: requestID,
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

	// ------------------------------------------------------------
	// 3. Check seat after booking
	// ------------------------------------------------------------

	getResponse, err = client.GetSeat(
		ctx,
		&movie.GetSeatRequest{
			ShowId: showID,
			SeatId: seatID,
		},
	)

	if err != nil {
		log.Fatalf("final GetSeat failed: %v", err)
	}

	log.Printf(
		"After booking: show=%s seat=%s status=%s",
		getResponse.GetShowId(),
		getResponse.GetSeatId(),
		getResponse.GetStatus(),
	)

	// ------------------------------------------------------------
	// 4. Repeat the exact same request
	//    to verify idempotency.
	// ------------------------------------------------------------

	retryResponse, err := client.BookSeat(
		ctx,
		&movie.BookSeatRequest{
			ShowId:    showID,
			SeatId:    seatID,
			UserId:    userID,
			RequestId: requestID,
		},
	)

	if err != nil {
		log.Fatalf("retry BookSeat failed: %v", err)
	}

	log.Printf(
		"Retry response: booking=%s show=%s seat=%s status=%s message=%s",
		retryResponse.GetBookingId(),
		retryResponse.GetShowId(),
		retryResponse.GetSeatId(),
		retryResponse.GetStatus(),
		retryResponse.GetMessage(),
	)

	log.Println("End-to-end Raft booking test completed")
}
