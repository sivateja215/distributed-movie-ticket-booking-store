package lsm

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteSSTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.sst")

	data := map[string]string{
		"seat:A10": "available",
		"seat:A03": "booked",
		"seat:A12": "available",
		"seat:A01": "booked",
	}

	if err := WriteSSTable(path, data); err != nil {
		t.Fatalf("failed to write SSTable: %v", err)
	}

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read SSTable file: %v", err)
	}

	expected := "seat:A01\tbooked\n" +
		"seat:A03\tbooked\n" +
		"seat:A10\tavailable\n" +
		"seat:A12\tavailable\n"

	if string(content) != expected {
		t.Fatalf(
			"unexpected SSTable content:\nexpected:\n%s\ngot:\n%s",
			expected,
			string(content),
		)
	}
}

func TestWriteEmptySSTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.sst")

	data := map[string]string{}

	if err := WriteSSTable(path, data); err != nil {
		t.Fatalf("failed to write empty SSTable: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("failed to stat SSTable: %v", err)
	}

	if info.Size() != 0 {
		t.Fatalf("expected empty SSTable, got %d bytes", info.Size())
	}
}
func TestSSTableGet(t *testing.T) {
	data := map[string]string{
		"seat:A10": "available",
		"seat:A11": "booked",
		"seat:A12": "available",
	}

	path := filepath.Join(t.TempDir(), "test.sst")

	err := WriteSSTable(path, data)
	if err != nil {
		t.Fatalf("failed to write SSTable: %v", err)
	}

	table, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("failed to open SSTable: %v", err)
	}
	defer table.Close()

	value, found, err := table.Get("seat:A11")
	if err != nil {
		t.Fatalf("failed to get key: %v", err)
	}

	if !found {
		t.Fatalf("expected key to be found")
	}

	if value != "booked" {
		t.Fatalf("expected value 'booked', got %q", value)
	}
}
func TestSSTableMissingKey(t *testing.T) {
	data := map[string]string{
		"seat:A10": "available",
		"seat:A11": "booked",
	}

	path := filepath.Join(t.TempDir(), "test.sst")

	err := WriteSSTable(path, data)
	if err != nil {
		t.Fatalf("failed to write SSTable: %v", err)
	}

	table, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("failed to open SSTable: %v", err)
	}
	defer table.Close()

	value, found, err := table.Get("seat:Z99")
	if err != nil {
		t.Fatalf("failed to get key: %v", err)
	}

	if found {
		t.Fatalf("expected key to be missing, but got value %q", value)
	}
}
