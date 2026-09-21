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
