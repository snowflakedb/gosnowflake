package gosnowflake

import (
	"strconv"
	"sync"
)

const (
	queryContextCacheSizeParamName = "QUERY_CONTEXT_CACHE_SIZE"
	defaultQueryContextCacheSize   = 5
)

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

func (qcc *queryContextCache) add(sc *snowflakeConn, qces ...queryContextEntry) {
	qcc.mutex.Lock()
	defer qcc.mutex.Unlock()
	if len(qces) == 0 {
		qcc.prune(0)
	} else {
		qcc.entries = append(qcc.entries, qces...)
		qcc.prune(qcc.getQueryContextCacheSize(sc))
	}
}

func (qcc *queryContextCache) prune(size int) {
	if len(qcc.entries) > size {
		qcc.entries = qcc.entries[0:size]
	}
}

func (qcc *queryContextCache) getQueryContextCacheSize(sc *snowflakeConn) int {
	if sizeStr, ok := sc.cfg.Params[queryContextCacheSizeParamName]; ok {
		size, err := strconv.Atoi(*sizeStr)
		if err != nil {
			logger.Warnf("cannot parse %v as int as query context cache size: %v", sizeStr, err)
		} else {
			return size
		}
	}
	return defaultQueryContextCacheSize
}
