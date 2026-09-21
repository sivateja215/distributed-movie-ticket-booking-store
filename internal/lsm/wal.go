package lsm

import (
	"bufio"
	"os"
)

// WAL (Write-Ahead Log) records operations on disk
// so they can be replayed after a crash.
type WAL struct {
	file *os.File
}

// NewWAL opens or creates a WAL file.
func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
	}, nil
}

// Append writes a key-value operation to the WAL.
func (w *WAL) Append(key string, value string) error {
	_, err := w.file.WriteString(key + "\t" + value + "\n")
	if err != nil {
		return err
	}

	// Ensure the operation reaches the operating system's
	// file buffers before returning.
	return w.file.Sync()
}

// Replay reads the WAL from the beginning and returns
// all recorded key-value operations.
func (w *WAL) Replay() (map[string]string, error) {
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}

	result := make(map[string]string)

	scanner := bufio.NewScanner(w.file)

	for scanner.Scan() {
		line := scanner.Text()

		// Split the line into key and value.
		for i := 0; i < len(line); i++ {
			if line[i] == '\t' {
				key := line[:i]
				value := line[i+1:]
				result[key] = value
				break
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	return w.file.Close()
}
