package raft

type LogEntry struct {
	Term      int
	Command   string
	Key       string
	Value     string
	RequestID string
}

type Log struct {
	Entries []LogEntry
}

func NewLog() *Log {
	return &Log{
		Entries: make([]LogEntry, 0),
	}
}

func (l *Log) LastIndex() int {
	return len(l.Entries) - 1
}

func (l *Log) LastTerm() int {
	if len(l.Entries) == 0 {
		return 0
	}

	return l.Entries[len(l.Entries)-1].Term
}

func (l *Log) Append(entry LogEntry) int {
	l.Entries = append(l.Entries, entry)

	return l.LastIndex()
}

func (l *Log) Get(index int) (LogEntry, bool) {
	if index < 0 || index >= len(l.Entries) {
		return LogEntry{}, false
	}

	return l.Entries[index], true
}
