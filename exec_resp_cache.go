package gosnowflake

import (
	"sync"
	"sync/atomic"
	"time"
)

// A reference counted cache from string -> *execResponse. The
// refcount is a reference counter that is used to gc the cache
// so we don't leak memory. We use a sync.Map as the golang docs
// indicate the performance is better than a Mutex + native map.
type execRespCache struct {
	id       string
	refcount int64
	table    sync.Map
}

// An entry in the exec response cache. The entry has a TTL
// since the URLs to S3 do have an access token that can
// expire. At the time of writing this TTL was 6 hours.
type execRespCacheEntry struct {
	created time.Time
	respd   *execResponse
}

const (
	execRespCacheEntryTTL = 1 * time.Hour
)

// A global table of exec response caches. We need this since
// the gosnowflake driver does not do its own connection
// pooling and we want a shared cache across all sql.Conn
// instances created over the course of the sql.Driver lifetime.
// We use a native map + lock here to ensure there aren't race
// conditions in the acquire and release code. There should not be
// a performance implication since these fns are called infrequently.
var (
	globalExecRespCacheMu = sync.Mutex{}
	globalExecRespCache   = map[string]*execRespCache{}
)

func acquireExecRespCache(id string) *execRespCache {
	globalExecRespCacheMu.Lock()
	defer globalExecRespCacheMu.Unlock()

	entry, found := globalExecRespCache[id]
	if found {
		atomic.AddInt64(&entry.refcount, 1)
		return entry
	}

	cache := &execRespCache{id, 1, sync.Map{}}
	globalExecRespCache[id] = cache
	return cache
}

func releaseExecRespCache(cache *execRespCache) {
	if cache == nil {
		return
	}

	globalExecRespCacheMu.Lock()
	defer globalExecRespCacheMu.Unlock()

	refcount := atomic.AddInt64(&cache.refcount, -1)
	if refcount <= 0 {
		delete(globalExecRespCache, cache.id)
	}
}

func (c *execRespCache) load(key string) (*execResponse, bool) {
	if c == nil {
		return nil, false
	}

	val, ok := c.table.Load(key)
	if !ok {
		return nil, false
	}

	entry := val.(execRespCacheEntry)
	if entry.isExpired() {
		c.table.Delete(key)
		return nil, false
	}
	return entry.respd, true
}

func (c *execRespCache) store(key string, val *execResponse) {
	if c == nil {
		return
	}
	c.table.Store(key, execRespCacheEntry{time.Now(), val})
}

func (e execRespCacheEntry) isExpired() bool {
	return time.Since(e.created) >= execRespCacheEntryTTL
}
