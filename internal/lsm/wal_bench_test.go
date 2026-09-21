package lsm

import (
	"path/filepath"
	"strconv"
	"testing"
)

func BenchmarkWALAppend(b *testing.B) {
	path := filepath.Join(b.TempDir(), "benchmark.wal")

	wal, err := NewWAL(path)
	if err != nil {
		b.Fatalf("failed to create WAL: %v", err)
	}
	defer wal.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := "seat:" + strconv.Itoa(i)

		if err := wal.Append(key, "available"); err != nil {
			b.Fatalf("failed to append WAL record: %v", err)
		}
	}
}
