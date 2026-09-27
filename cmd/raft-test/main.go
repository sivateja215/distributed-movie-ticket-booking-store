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
	log.Println("Starting Raft election on node2")

	conn, err := grpc.NewClient(
		"localhost:50052",
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		log.Fatalf("failed to connect to node2: %v", err)
	}

	defer conn.Close()

	client := movie.NewNodeServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	response, err := client.StartElection(
		ctx,
		&movie.StartElectionRequest{
			NodeId: "node2",
		},
	)
	if err != nil {
		log.Fatalf("StartElection failed: %v", err)
	}

	log.Printf(
		"Election started on %s | term=%d | role=%s",
		response.GetNodeId(),
		response.GetTerm(),
		response.GetRole(),
	)

	time.Sleep(2 * time.Second)

	log.Println("Node2 election test completed")
}
