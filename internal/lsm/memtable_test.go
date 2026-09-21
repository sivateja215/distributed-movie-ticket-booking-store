package lsm

import "testing"

func TestMemTablePutAndGet(t *testing.T) {
	mem := NewMemTable()

	mem.Put("seat:A10", "available")

	value, ok := mem.Get("seat:A10")

	if !ok {
		t.Fatal("expected key to exist")
	}

	if value != "available" {
		t.Fatalf("expected value %q, got %q", "available", value)
	}
}

func TestMemTableUpdate(t *testing.T) {
	mem := NewMemTable()

	mem.Put("seat:A10", "available")
	mem.Put("seat:A10", "booked")

	value, ok := mem.Get("seat:A10")

	if !ok {
		t.Fatal("expected key to exist")
	}

	if value != "booked" {
		t.Fatalf("expected updated value %q, got %q", "booked", value)
	}
}

func TestMemTableDelete(t *testing.T) {
	mem := NewMemTable()

	mem.Put("seat:A10", "available")
	mem.Delete("seat:A10")

	_, ok := mem.Get("seat:A10")

	if ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestMemTableMissingKey(t *testing.T) {
	mem := NewMemTable()

	_, ok := mem.Get("seat:Z99")

	if ok {
		t.Fatal("expected missing key to return false")
	}
}
