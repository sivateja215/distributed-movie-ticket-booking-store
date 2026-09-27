package lsm

import (
	"strconv"
	"testing"
)

// BenchmarkRocksDBPut benchmarks RocksDB writes.
func BenchmarkRocksDBPut(b *testing.B) {
	removeRocksDB()

	db, options, readOptions, writeOptions, err := openRocksDB()
	if err != nil {
		b.Fatal(err)
	}

	defer closeRocksDB(
		db,
		options,
		readOptions,
		writeOptions,
	)

	defer removeRocksDB()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := "seat:" + strconv.Itoa(i%10000)

		if err := putRocksDB(
			db,
			writeOptions,
			key,
			"available",
		); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRocksDBGet benchmarks RocksDB reads.
func BenchmarkRocksDBGet(b *testing.B) {
	removeRocksDB()

	db, options, readOptions, writeOptions, err := openRocksDB()
	if err != nil {
		b.Fatal(err)
	}

	defer closeRocksDB(
		db,
		options,
		readOptions,
		writeOptions,
	)

	defer removeRocksDB()

	// Pre-populate RocksDB.
	for i := 0; i < 10000; i++ {
		key := "seat:" + strconv.Itoa(i)

		if err := putRocksDB(
			db,
			writeOptions,
			key,
			"available",
		); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := "seat:" + strconv.Itoa(i%10000)

		_, found, err := getRocksDB(
			db,
			readOptions,
			key,
		)

		if err != nil {
			b.Fatal(err)
		}

		if !found {
			b.Fatalf("expected key %s to exist", key)
		}
	}
}
