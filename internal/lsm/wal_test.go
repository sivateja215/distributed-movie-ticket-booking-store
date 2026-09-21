package lsm

import (
	"path/filepath"
	"testing"
)

func TestWALAppendAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")

	wal, err := NewWAL(path)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	if err := wal.Append("seat:A10", "available"); err != nil {
		t.Fatalf("failed to append first operation: %v", err)
	}

	if err := wal.Append("seat:A11", "available"); err != nil {
		t.Fatalf("failed to append second operation: %v", err)
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("failed to close WAL: %v", err)
	}

	// Reopen the WAL to simulate a restart.
	wal, err = NewWAL(path)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer wal.Close()

	recovered, err := wal.Replay()
	if err != nil {
		t.Fatalf("failed to replay WAL: %v", err)
	}

	if recovered["seat:A10"] != "available" {
		t.Fatalf("expected seat:A10 to be available, got %q", recovered["seat:A10"])
	}

	if recovered["seat:A11"] != "available" {
		t.Fatalf("expected seat:A11 to be available, got %q", recovered["seat:A11"])
	}
}

func TestWALLatestValueWins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")

	wal, err := NewWAL(path)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	if err := wal.Append("seat:A10", "available"); err != nil {
		t.Fatalf("failed to append available state: %v", err)
	}

	if err := wal.Append("seat:A10", "booked"); err != nil {
		t.Fatalf("failed to append booked state: %v", err)
	}

	recovered, err := wal.Replay()
	if err != nil {
		t.Fatalf("failed to replay WAL: %v", err)
	}

	if recovered["seat:A10"] != "booked" {
		t.Fatalf(
			"expected latest value %q, got %q",
			"booked",
			recovered["seat:A10"],
		)
	}
}
