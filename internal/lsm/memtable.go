package lsm

// MemTable stores key-value pairs in memory.
type MemTable struct {
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
	m.data[key] = value
}

// Get retrieves the value associated with a key.
func (m *MemTable) Get(key string) (string, bool) {
	value, ok := m.data[key]
	return value, ok
}

// Delete removes a key from the MemTable.
func (m *MemTable) Delete(key string) {
	delete(m.data, key)
}
