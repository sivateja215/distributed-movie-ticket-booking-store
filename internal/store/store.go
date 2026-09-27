package store

import (
	"fmt"
	"os"
	"sync"

	"moviekv/internal/lsm"
)

type Store struct {
	mu       sync.RWMutex
	memTable *lsm.MemTable
	wal      *lsm.WAL
}

func NewStore(walPath string) (*Store, error) {
	if err := os.MkdirAll("data", 0755); err != nil {
		return nil, err
	}

	wal, err := lsm.NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	memTable := lsm.NewMemTable()

	records, err := wal.Replay()
	if err != nil {
		wal.Close()
		return nil, err
	}

	for key, value := range records {
		memTable.Put(key, value)
	}

	return &Store{
		memTable: memTable,
		wal:      wal,
	}, nil
}

func (s *Store) Put(key string, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.wal.Append(key, value); err != nil {
		return fmt.Errorf("wal append failed: %w", err)
	}

	s.memTable.Put(key, value)

	return nil
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.memTable.Get(key)
}

func (s *Store) GetRequestResult(requestID string) (string, bool) {
	return s.Get("request:" + requestID)
}

func (s *Store) SaveRequestResult(requestID string, result string) error {
	return s.Put("request:"+requestID, result)
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.wal.Close()
}
