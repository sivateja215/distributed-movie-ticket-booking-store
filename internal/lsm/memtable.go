package lsm

import "sync"

// MemTable stores key-value pairs in memory.
//
// RWMutex allows multiple readers at the same time,
// while writes are protected so that only one write
// happens at a time.
type MemTable struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewMemTable creates and returns an empty MemTable.
func NewMemTable() *MemTable {
	return &MemTable{
		data: make(map[string]string),
	}
}

// Put inserts or updates a key-value pair.
func (m *MemTable) Put(key string, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value
}

// Get retrieves the value associated with a key.
func (m *MemTable) Get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, ok := m.data[key]
	return value, ok
}

// Delete removes a key from the MemTable.
func (m *MemTable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
}
