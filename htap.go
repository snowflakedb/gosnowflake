package gosnowflake

import "sync"

type queryContextEntry struct {
	ID        int   `json:"id"`
	Timestamp int64 `json:"timestamp"`
	Priority  int   `json:"priority"`
	Context   any   `json:"context,omitempty"`
}

type queryContextCache struct {
	mutex   *sync.Mutex
	entries []queryContextEntry
}

func (qcc *queryContextCache) init() *queryContextCache {
	qcc.mutex = &sync.Mutex{}
	return qcc
}

func (qcc *queryContextCache) add(qces ...queryContextEntry) {
	qcc.mutex.Lock()
	defer qcc.mutex.Unlock()
	qcc.entries = append(qcc.entries, qces...)
}
