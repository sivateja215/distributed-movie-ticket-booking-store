package main

import (
	"context"
	"log"
	"time"

	movie "moviekv/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func pingNode(address string, nodeID string) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to connect to %s: %v", address, err)
	}

	defer conn.Close()

	client := movie.NewNodeServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	response, err := client.Ping(
		ctx,
		&movie.PingRequest{
			NodeId: "node1",
		},
	)
	if err != nil {
		log.Fatalf("Ping to %s failed: %v", nodeID, err)
	}

	log.Printf(
		"Ping response from %s: node=%s status=%s",
		nodeID,
		response.GetNodeId(),
		response.GetStatus(),
	)
}

func main() {
	pingNode("localhost:50052", "node2")
	pingNode("localhost:50053", "node3")
}
