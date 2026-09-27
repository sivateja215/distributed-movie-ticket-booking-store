package main

import (
	"context"
	"log"
	"net"
	"strings"

	movie "moviekv/api"
	"moviekv/internal/store"

	"google.golang.org/grpc"
)

type movieServer struct {
	movie.UnimplementedMovieTicketServiceServer
	store *store.Store
}

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
		"GetSeat: show_id=%s seat_id=%s status=%s",
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

func (s *movieServer) BookSeat(
	ctx context.Context,
	req *movie.BookSeatRequest,
) (*movie.BookSeatResponse, error) {

	if strings.TrimSpace(req.GetRequestId()) == "" {
		return &movie.BookSeatResponse{
			ShowId:  req.GetShowId(),
			SeatId:  req.GetSeatId(),
			Status:  "failed",
			Message: "request_id is required",
		}, nil
	}

	existingResult, exists := s.store.GetRequestResult(req.GetRequestId())

	if exists {
		parts := strings.Split(existingResult, "|")

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

	seatKey := "seat:" + req.GetShowId() + ":" + req.GetSeatId()

	currentStatus, exists := s.store.Get(seatKey)

	if exists && currentStatus == "booked" {
		return &movie.BookSeatResponse{
			ShowId:  req.GetShowId(),
			SeatId:  req.GetSeatId(),
			Status:  "failed",
			Message: "Seat is already booked",
		}, nil
	}

	bookingID := "BK-" + req.GetRequestId()

	if err := s.store.Put(seatKey, "booked"); err != nil {
		return nil, err
	}

	bookingKey := "booking:" + req.GetRequestId()

	bookingValue := req.GetUserId() + "|" +
		req.GetShowId() + "|" +
		req.GetSeatId()

	if err := s.store.Put(bookingKey, bookingValue); err != nil {
		return nil, err
	}

	result := bookingID + "|" +
		req.GetShowId() + "|" +
		req.GetSeatId() + "|" +
		"confirmed|" +
		"Seat booked successfully"

	if err := s.store.SaveRequestResult(
		req.GetRequestId(),
		result,
	); err != nil {
		return nil, err
	}

	log.Printf(
		"BookSeat: show_id=%s seat_id=%s user_id=%s request_id=%s",
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

func main() {
	storage, err := store.NewStore("data/server.wal")
	if err != nil {
		log.Fatalf("failed to create store: %v", err)
	}
	defer storage.Close()

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	movie.RegisterMovieTicketServiceServer(
		grpcServer,
		&movieServer{
			store: storage,
		},
	)

	log.Println("Movie Ticket gRPC server listening on :50051")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
