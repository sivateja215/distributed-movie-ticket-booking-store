package lsm

import (
	"strconv"
	"testing"
)

func BenchmarkMemTablePut(b *testing.B) {
	mem := NewMemTable()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := "seat:" + strconv.Itoa(i)
		mem.Put(key, "available")
	}
}

func BenchmarkMemTableGet(b *testing.B) {
	mem := NewMemTable()

	for i := 0; i < b.N; i++ {
		key := "seat:" + strconv.Itoa(i)
		mem.Put(key, "available")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := "seat:" + strconv.Itoa(i)
		mem.Get(key)
	}
}
func BenchmarkMemTableConcurrentPut(b *testing.B) {
	mem := NewMemTable()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0

		for pb.Next() {
			key := "seat:" + strconv.Itoa(i)
			mem.Put(key, "available")
			i++
		}
	})
}

func BenchmarkMemTableConcurrentGet(b *testing.B) {
	mem := NewMemTable()

	// Pre-populate the MemTable.
	for i := 0; i < 10000; i++ {
		key := "seat:" + strconv.Itoa(i)
		mem.Put(key, "available")
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0

		for pb.Next() {
			key := "seat:" + strconv.Itoa(i%10000)
			mem.Get(key)
			i++
		}
	})
}
