package lsm

import (
	"bufio"
	"os"
	"sort"
	"strings"
)

// SSTable represents an immutable sorted table stored on disk.
type SSTable struct {
	path string
	file *os.File
}

// WriteSSTable creates an SSTable from key-value data.
func WriteSSTable(path string, data map[string]string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	// Collect all keys.
	keys := make([]string, 0, len(data))

	for key := range data {
		keys = append(keys, key)
	}

	// Sort keys so the SSTable is stored in sorted order.
	sort.Strings(keys)

	// Write key-value pairs.
	for _, key := range keys {
		if _, err := writer.WriteString(key + "\t" + data[key] + "\n"); err != nil {
			return err
		}
	}

	return writer.Flush()
}

// OpenSSTable opens an existing SSTable.
func OpenSSTable(path string) (*SSTable, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &SSTable{
		path: path,
		file: file,
	}, nil
}

// Get searches the SSTable for a key.
//
// This first implementation still uses a sequential scan.
// Later we will optimize this using the sorted-key property.
func (s *SSTable) Get(key string) (string, bool, error) {
	// Start reading from the beginning of the SSTable.
	if _, err := s.file.Seek(0, 0); err != nil {
		return "", false, err
	}

	scanner := bufio.NewScanner(s.file)

	for scanner.Scan() {
		line := scanner.Text()

		parts := strings.SplitN(line, "\t", 2)

		if len(parts) != 2 {
			continue
		}

		if parts[0] == key {
			return parts[1], true, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return "", false, err
	}

	return "", false, nil
}

// Close closes the SSTable file.
func (s *SSTable) Close() error {
	return s.file.Close()
}
