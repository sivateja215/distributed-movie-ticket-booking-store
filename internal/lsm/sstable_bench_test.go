package lsm

import (
	"path/filepath"
	"strconv"
	"testing"
)

func BenchmarkSSTableGet(b *testing.B) {
	data := make(map[string]string)

	for i := 0; i < 10000; i++ {
		key := "seat:" + strconv.Itoa(i)
		data[key] = "available"
	}

	path := filepath.Join(b.TempDir(), "benchmark.sst")

	err := WriteSSTable(path, data)
	if err != nil {
		b.Fatalf("failed to write SSTable: %v", err)
	}

	table, err := OpenSSTable(path)
	if err != nil {
		b.Fatalf("failed to open SSTable: %v", err)
	}
	defer table.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := "seat:" + strconv.Itoa(i%10000)

		_, _, err := table.Get(key)
		if err != nil {
			b.Fatalf("failed to get key: %v", err)
		}
	}
}
func BenchmarkSSTableWrite(b *testing.B) {
	data := make(map[string]string)

	for i := 0; i < 10000; i++ {
		key := "seat:" + strconv.Itoa(i)
		data[key] = "available"
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		path := filepath.Join(b.TempDir(), "benchmark.sst")

		err := WriteSSTable(path, data)
		if err != nil {
			b.Fatalf("failed to write SSTable: %v", err)
		}
	}
}
