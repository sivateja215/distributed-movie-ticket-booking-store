package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	movie "moviekv/api"
	"moviekv/internal/node"
	"moviekv/internal/raft"
	"moviekv/internal/store"

	"google.golang.org/grpc"
)

type movieServer struct {
	movie.UnimplementedMovieTicketServiceServer
	movie.UnimplementedNodeServiceServer
	movie.UnimplementedRaftServiceServer

	store *store.Store
	node  node.Config
	raft  *raft.Node
}

// ------------------------------------------------------------
// Ping
// ------------------------------------------------------------

func (s *movieServer) Ping(
	ctx context.Context,
	req *movie.PingRequest,
) (*movie.PingResponse, error) {

	log.Printf(
		"[%s] Ping received from node=%s",
		s.node.ID,
		req.GetNodeId(),
	)

	return &movie.PingResponse{
		NodeId: s.node.ID,
		Status: "alive",
	}, nil
}

// ------------------------------------------------------------
// Raft RequestVote
// ------------------------------------------------------------

func (s *movieServer) RequestVote(
	ctx context.Context,
	req *movie.RequestVoteRequest,
) (*movie.RequestVoteResponse, error) {

	term, voteGranted := s.raft.HandleRequestVote(
		int(req.GetTerm()),
		req.GetCandidateId(),
	)

	log.Printf(
		"[%s] RequestVote: candidate=%s term=%d vote_granted=%t",
		s.node.ID,
		req.GetCandidateId(),
		req.GetTerm(),
		voteGranted,
	)

	return &movie.RequestVoteResponse{
		Term:        int32(term),
		VoteGranted: voteGranted,
	}, nil
}

// ------------------------------------------------------------
// Start Raft Election
// ------------------------------------------------------------

func (s *movieServer) StartElection(
	ctx context.Context,
	req *movie.StartElectionRequest,
) (*movie.StartElectionResponse, error) {

	log.Printf(
		"[%s] starting election request received from %s",
		s.node.ID,
		req.GetNodeId(),
	)

	s.raft.StartElection()

	return &movie.StartElectionResponse{
		NodeId: s.node.ID,
		Term:   int32(s.raft.GetTerm()),
		Role:   string(s.raft.GetRole()),
	}, nil
}

// ------------------------------------------------------------
// Replicate Log Entry
// ------------------------------------------------------------

func (s *movieServer) ReplicateLogEntry(
	ctx context.Context,
	req *movie.ReplicateLogEntryRequest,
) (*movie.ReplicateLogEntryResponse, error) {

	// Only the leader can accept a new log entry.
	if s.raft.GetRole() != raft.Leader {
		return &movie.ReplicateLogEntryResponse{
			Index:   int32(s.raft.GetLastLogIndex()),
			Term:    int32(s.raft.GetTerm()),
			Status:  "rejected",
			Message: "Node is not the leader",
		}, nil
	}

	// Create a Raft log entry.
	entry := raft.LogEntry{
		Term:      s.raft.GetTerm(),
		Command:   req.GetCommand(),
		Key:       req.GetKey(),
		Value:     req.GetValue(),
		RequestID: req.GetRequestId(),
	}

	// Append the entry to the leader's local log.
	index := s.raft.AppendLocalEntry(entry)

	// Replicate the entry to follower nodes.
	s.raft.ReplicateEntry(entry)

	log.Printf(
		"[%s] ReplicateLogEntry: index=%d term=%d command=%s key=%s request_id=%s",
		s.node.ID,
		index,
		entry.Term,
		entry.Command,
		entry.Key,
		entry.RequestID,
	)

	// Wait until the entry is committed by a majority.
	committed := s.raft.WaitForCommit(
		index,
		5*time.Second,
	)

	if !committed {
		return &movie.ReplicateLogEntryResponse{
			Index:   int32(index),
			Term:    int32(entry.Term),
			Status:  "uncommitted",
			Message: "Entry was replicated but majority commit was not reached",
		}, nil
	}

	// Apply all newly committed entries to the state machine.
	appliedEntries := s.raft.ApplyCommittedEntries()

	for _, committedEntry := range appliedEntries {

		if err := s.applyCommittedBooking(committedEntry); err != nil {
			return nil, err
		}
	}

	return &movie.ReplicateLogEntryResponse{
		Index:   int32(index),
		Term:    int32(entry.Term),
		Status:  "committed",
		Message: "Log entry committed by majority and applied to store",
	}, nil
}

// ------------------------------------------------------------
// Apply committed BOOK_SEAT entry
// ------------------------------------------------------------

func (s *movieServer) applyCommittedBooking(
	entry raft.LogEntry,
) error {

	if entry.Command != "BOOK_SEAT" {
		return nil
	}

	// The committed entry stores:
	// Value = user_id|show_id|seat_id
	parts := splitResult(entry.Value)

	if len(parts) != 3 {
		return fmt.Errorf(
			"invalid BOOK_SEAT value for request_id=%s",
			entry.RequestID,
		)
	}

	userID := parts[0]
	showID := parts[1]
	seatID := parts[2]

	seatKey := "seat:" + showID + ":" + seatID

	// Mark the seat as booked.
	if err := s.store.Put(seatKey, "booked"); err != nil {
		return err
	}

	// Store booking information.
	bookingKey := "booking:" + entry.RequestID
	bookingValue := userID + "|" + showID + "|" + seatID

	if err := s.store.Put(bookingKey, bookingValue); err != nil {
		return err
	}

	// Store the complete request result for idempotency.
	bookingID := "BK-" + entry.RequestID

	result := bookingID + "|" +
		showID + "|" +
		seatID + "|" +
		"confirmed|" +
		"Seat booked successfully"

	if err := s.store.SaveRequestResult(
		entry.RequestID,
		result,
	); err != nil {
		return err
	}

	log.Printf(
		"[%s] applied BOOK_SEAT: seat=%s booking=%s request_id=%s",
		s.node.ID,
		seatKey,
		bookingID,
		entry.RequestID,
	)

	return nil
}

// ------------------------------------------------------------
// Raft AppendEntries
// ------------------------------------------------------------

func (s *movieServer) AppendEntries(
	ctx context.Context,
	req *movie.AppendEntriesRequest,
) (*movie.AppendEntriesResponse, error) {

	term, success := s.raft.HandleAppendEntries(
		int(req.GetTerm()),
		req.GetLeaderId(),
		req.GetEntries(),
		int(req.GetLeaderCommit()),
	)

	log.Printf(
		"[%s] AppendEntries: leader=%s term=%d entries=%d leaderCommit=%d success=%t",
		s.node.ID,
		req.GetLeaderId(),
		req.GetTerm(),
		len(req.GetEntries()),
		req.GetLeaderCommit(),
		success,
	)

	if success {
		appliedEntries := s.raft.ApplyCommittedEntries()

		for _, committedEntry := range appliedEntries {

			if err := s.applyCommittedBooking(committedEntry); err != nil {
				return nil, err
			}
		}
	}

	return &movie.AppendEntriesResponse{
		Term:    int32(term),
		Success: success,
	}, nil
}

// ------------------------------------------------------------
// Get Seat
// ------------------------------------------------------------

func (s *movieServer) GetSeat(
	ctx context.Context,
	req *movie.GetSeatRequest,
) (*movie.GetSeatResponse, error) {

	key := "seat:" + req.GetShowId() + ":" + req.GetSeatId()

	value, ok := s.store.Get(key)

	if !ok {
		value = "available"
	}

	log.Printf(
		"[%s] GetSeat: show_id=%s seat_id=%s status=%s",
		s.node.ID,
		req.GetShowId(),
		req.GetSeatId(),
		value,
	)

	return &movie.GetSeatResponse{
		ShowId: req.GetShowId(),
		SeatId: req.GetSeatId(),
		Status: value,
	}, nil
}

// ------------------------------------------------------------
// Book Seat
// ------------------------------------------------------------

func (s *movieServer) BookSeat(
	ctx context.Context,
	req *movie.BookSeatRequest,
) (*movie.BookSeatResponse, error) {

	// Request ID is required for idempotency.
	if strings.TrimSpace(req.GetRequestId()) == "" {
		return &movie.BookSeatResponse{
			ShowId:  req.GetShowId(),
			SeatId:  req.GetSeatId(),
			Status:  "failed",
			Message: "request_id is required",
		}, nil
	}

	// Check whether this request was already processed.
	existingResult, exists := s.store.GetRequestResult(
		req.GetRequestId(),
	)

	if exists {
		parts := splitResult(existingResult)

		if len(parts) == 5 {
			return &movie.BookSeatResponse{
				BookingId: parts[0],
				ShowId:    parts[1],
				SeatId:    parts[2],
				Status:    parts[3],
				Message:   parts[4],
			}, nil
		}
	}

	// Build seat key.
	seatKey := "seat:" + req.GetShowId() + ":" + req.GetSeatId()

	// Check current seat status.
	currentStatus, exists := s.store.Get(seatKey)

	if exists && currentStatus == "booked" {
		return &movie.BookSeatResponse{
			ShowId:  req.GetShowId(),
			SeatId:  req.GetSeatId(),
			Status:  "failed",
			Message: "Seat is already booked",
		}, nil
	}

	// Only the Raft leader can accept a new booking.
	if s.raft.GetRole() != raft.Leader {
		return &movie.BookSeatResponse{
			ShowId:  req.GetShowId(),
			SeatId:  req.GetSeatId(),
			Status:  "failed",
			Message: "Node is not the Raft leader",
		}, nil
	}

	// Store booking information in the Raft log value.
	//
	// Format:
	// user_id|show_id|seat_id
	bookingValue := req.GetUserId() + "|" +
		req.GetShowId() + "|" +
		req.GetSeatId()

	entry := raft.LogEntry{
		Term:      s.raft.GetTerm(),
		Command:   "BOOK_SEAT",
		Key:       seatKey,
		Value:     bookingValue,
		RequestID: req.GetRequestId(),
	}

	// Append the booking to the leader's local Raft log.
	index := s.raft.AppendLocalEntry(entry)

	// Replicate the booking to follower nodes.
	s.raft.ReplicateEntry(entry)

	log.Printf(
		"[%s] BookSeat Raft entry: index=%d term=%d key=%s request_id=%s",
		s.node.ID,
		index,
		entry.Term,
		entry.Key,
		entry.RequestID,
	)

	// Wait for majority commit.
	committed := s.raft.WaitForCommit(
		index,
		5*time.Second,
	)

	if !committed {
		return &movie.BookSeatResponse{
			ShowId:  req.GetShowId(),
			SeatId:  req.GetSeatId(),
			Status:  "failed",
			Message: "Booking was not committed by a majority",
		}, nil
	}

	// Apply newly committed entries to the state machine.
	appliedEntries := s.raft.ApplyCommittedEntries()

	for _, committedEntry := range appliedEntries {
		if err := s.applyCommittedBooking(committedEntry); err != nil {
			return nil, err
		}
	}

	bookingID := "BK-" + req.GetRequestId()

	log.Printf(
		"[%s] BookSeat committed: booking_id=%s show_id=%s seat_id=%s user_id=%s request_id=%s",
		s.node.ID,
		bookingID,
		req.GetShowId(),
		req.GetSeatId(),
		req.GetUserId(),
		req.GetRequestId(),
	)

	return &movie.BookSeatResponse{
		BookingId: bookingID,
		ShowId:    req.GetShowId(),
		SeatId:    req.GetSeatId(),
		Status:    "confirmed",
		Message:   "Seat booked successfully",
	}, nil
}

// ------------------------------------------------------------
// Split stored request result
// ------------------------------------------------------------

func splitResult(result string) []string {

	resultParts := make([]string, 0, 5)

	current := ""

	for _, char := range result {

		if char == '|' {
			resultParts = append(resultParts, current)
			current = ""
		} else {
			current += string(char)
		}
	}

	resultParts = append(resultParts, current)

	return resultParts
}

// ------------------------------------------------------------
// Main
// ------------------------------------------------------------

func main() {

	nodeID := flag.String(
		"id",
		"node1",
		"node ID",
	)

	port := flag.String(
		"port",
		"50051",
		"gRPC port",
	)

	flag.Parse()

	// Cluster configuration.
	peers := map[string]string{
		"node1": "localhost:50051",
		"node2": "localhost:50052",
		"node3": "localhost:50053",
	}

	// Allow Kubernetes deployments to override peer addresses.
	if peerConfig := os.Getenv("RAFT_PEERS"); peerConfig != "" {
		peers = make(map[string]string)

		for _, peer := range strings.Split(peerConfig, ",") {
			parts := strings.SplitN(peer, "=", 2)
			if len(parts) == 2 {
				peers[parts[0]] = parts[1]
			}
		}
	}

	// Create node configuration.
	config := node.NewConfig(
		*nodeID,
		":"+*port,
		peers,
	)

	// Create Raft node.
	raftNode := raft.NewNode(
		*nodeID,
		peers,
	)

	// Create storage.
	walPath := "data/" + *nodeID + ".wal"

	storage, err := store.NewStore(walPath)

	if err != nil {
		log.Fatalf(
			"[%s] failed to create store: %v",
			config.ID,
			err,
		)
	}

	defer storage.Close()

	// Create TCP listener.
	listener, err := net.Listen(
		"tcp",
		config.Address,
	)

	if err != nil {
		log.Fatalf(
			"[%s] failed to listen on %s: %v",
			config.ID,
			config.Address,
			err,
		)
	}

	// Create gRPC server.
	grpcServer := grpc.NewServer()

	// Create movie server.
	server := &movieServer{
		store: storage,
		node:  config,
		raft:  raftNode,
	}

	// Register Movie Ticket service.
	movie.RegisterMovieTicketServiceServer(
		grpcServer,
		server,
	)

	// Register Node service.
	movie.RegisterNodeServiceServer(
		grpcServer,
		server,
	)

	// Register Raft service.
	movie.RegisterRaftServiceServer(
		grpcServer,
		server,
	)

	log.Printf(
		"Node %s listening on %s",
		config.ID,
		config.Address,
	)

	// Start gRPC server.
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf(
			"[%s] failed to serve: %v",
			config.ID,
			err,
		)
	}
}
