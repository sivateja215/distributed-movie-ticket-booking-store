package lsm

/*
#cgo LDFLAGS: -lrocksdb
#include <rocksdb/c.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"os"
	"unsafe"
)

const rocksDBPath = "/tmp/moviekv-rocksdb-bench"

// openRocksDB opens a temporary RocksDB database.
func openRocksDB() (
	*C.rocksdb_t,
	*C.rocksdb_options_t,
	*C.rocksdb_readoptions_t,
	*C.rocksdb_writeoptions_t,
	error,
) {
	options := C.rocksdb_options_create()

	if options == nil {
		return nil, nil, nil, nil,
			fmt.Errorf("failed to create RocksDB options")
	}

	C.rocksdb_options_set_create_if_missing(options, C.uchar(1))

	var err *C.char

	dbPath := C.CString(rocksDBPath)
	defer C.free(unsafe.Pointer(dbPath))

	db := C.rocksdb_open(options, dbPath, &err)

	if err != nil {
		message := C.GoString(err)
		C.rocksdb_free(unsafe.Pointer(err))
		C.rocksdb_options_destroy(options)

		return nil, nil, nil, nil,
			fmt.Errorf("failed to open RocksDB: %s", message)
	}

	readOptions := C.rocksdb_readoptions_create()
	writeOptions := C.rocksdb_writeoptions_create()

	return db, options, readOptions, writeOptions, nil
}

// closeRocksDB closes all RocksDB resources.
func closeRocksDB(
	db *C.rocksdb_t,
	options *C.rocksdb_options_t,
	readOptions *C.rocksdb_readoptions_t,
	writeOptions *C.rocksdb_writeoptions_t,
) {
	C.rocksdb_readoptions_destroy(readOptions)
	C.rocksdb_writeoptions_destroy(writeOptions)
	C.rocksdb_close(db)
	C.rocksdb_options_destroy(options)
}

// putRocksDB inserts one key-value pair.
func putRocksDB(
	db *C.rocksdb_t,
	writeOptions *C.rocksdb_writeoptions_t,
	key string,
	value string,
) error {
	cKey := C.CBytes([]byte(key))
	cValue := C.CBytes([]byte(value))

	defer C.free(cKey)
	defer C.free(cValue)

	var err *C.char

	C.rocksdb_put(
		db,
		writeOptions,
		(*C.char)(cKey),
		C.size_t(len(key)),
		(*C.char)(cValue),
		C.size_t(len(value)),
		&err,
	)

	if err != nil {
		message := C.GoString(err)
		C.rocksdb_free(unsafe.Pointer(err))

		return fmt.Errorf("rocksdb put failed: %s", message)
	}

	return nil
}

// getRocksDB retrieves one key.
func getRocksDB(
	db *C.rocksdb_t,
	readOptions *C.rocksdb_readoptions_t,
	key string,
) (string, bool, error) {
	cKey := C.CBytes([]byte(key))
	defer C.free(cKey)

	var valueLength C.size_t
	var err *C.char

	value := C.rocksdb_get(
		db,
		readOptions,
		(*C.char)(cKey),
		C.size_t(len(key)),
		&valueLength,
		&err,
	)

	if err != nil {
		message := C.GoString(err)
		C.rocksdb_free(unsafe.Pointer(err))

		return "", false,
			fmt.Errorf("rocksdb get failed: %s", message)
	}

	if value == nil {
		return "", false, nil
	}

	defer C.rocksdb_free(unsafe.Pointer(value))

	result := C.GoBytes(
		unsafe.Pointer(value),
		C.int(valueLength),
	)

	return string(result), true, nil
}

// removeRocksDB removes the temporary benchmark database.
func removeRocksDB() {
	_ = os.RemoveAll(rocksDBPath)
}
