package lsm

import (
	"strconv"
	"sync"
	"testing"
)

func TestMemTableConcurrentAccess(t *testing.T) {
	mem := NewMemTable()

	var wg sync.WaitGroup

	// Start 100 goroutines.
	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			key := "seat:" + strconv.Itoa(i)
			mem.Put(key, "available")

			value, found := mem.Get(key)
			if !found {
				t.Errorf("expected key %s to exist", key)
			}

			if value != "available" {
				t.Errorf(
					"expected value 'available' for %s, got %q",
					key,
					value,
				)
			}
		}(i)
	}

	wg.Wait()
}
func TestMemTableConcurrentUpdates(t *testing.T) {
	mem := NewMemTable()

	var wg sync.WaitGroup

	const goroutines = 100

	for i := 0; i < goroutines; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			value := "booking:" + strconv.Itoa(i)
			mem.Put("seat:A10", value)
		}(i)
	}

	wg.Wait()

	value, found := mem.Get("seat:A10")

	if !found {
		t.Fatalf("expected seat:A10 to exist")
	}

	if value == "" {
		t.Fatalf("expected seat:A10 to have a value")
	}
}
