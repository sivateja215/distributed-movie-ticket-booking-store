package raft

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

type LogWAL struct {
	file *os.File
}

func NewLogWAL(path string) (*LogWAL, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	return &LogWAL{
		file: file,
	}, nil
}

func (w *LogWAL) Append(entry LogEntry) error {
	line := strconv.Itoa(entry.Term) + "\t" +
		entry.Command + "\t" +
		entry.Key + "\t" +
		entry.Value + "\t" +
		entry.RequestID + "\n"

	if _, err := w.file.WriteString(line); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *LogWAL) Replay() ([]LogEntry, error) {
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}

	entries := make([]LogEntry, 0)

	scanner := bufio.NewScanner(w.file)

	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "\t", 5)

		if len(parts) != 5 {
			continue
		}

		term, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}

		entries = append(entries, LogEntry{
			Term:      term,
			Command:   parts[1],
			Key:       parts[2],
			Value:     parts[3],
			RequestID: parts[4],
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

func (w *LogWAL) Close() error {
	return w.file.Close()
}
