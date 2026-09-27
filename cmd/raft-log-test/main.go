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
		log.Fatalf("failed to connect to node1: %v", err)
	}

	defer conn.Close()

	client := movie.NewNodeServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)

	defer cancel()

	response, err := client.ReplicateLogEntry(
		ctx,
		&movie.ReplicateLogEntryRequest{
			Command:   "BOOK_SEAT",
			Key:       "seat:show-101:A10",
			Value:     "booked",
			RequestId: "req-raft-001",
		},
	)

	if err != nil {
		log.Fatalf(
			"ReplicateLogEntry failed: %v",
			err,
		)
	}

	log.Printf(
		"Log replication response: index=%d term=%d status=%s message=%s",
		response.GetIndex(),
		response.GetTerm(),
		response.GetStatus(),
		response.GetMessage(),
	)

	// Give the asynchronous replication requests
	// enough time to reach node2 and node3.
	time.Sleep(2 * time.Second)

	log.Println("Raft log replication test completed")
}
