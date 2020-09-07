package distributor

import (
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/ring"
)

const (
	cacheTTL               = 10 * time.Minute
	numSubringCacheStripes = 32
)

type subringCache struct {
	// The cache is split into stripes, each one with a dedicated lock, in
	// order to reduce lock contention.
	stripes [numSubringCacheStripes]subringCacheStripe
}

type subringCacheStripe struct {
	mu      sync.RWMutex
	entries map[string]*subringCacheEntry
}

type subringCacheEntry struct {
	cache     ring.ReadRing
	touchedAt atomic.Int64
}

// NewRefCache makes a new RefCache.
func newSubringCache() *subringCache {
	c := &subringCache{}

	// Stripes are pre-allocated so that we only read on them and no lock is required.
	for i := 0; i < numSubringCacheStripes; i++ {
		c.stripes[i].entries = map[string]*subringCacheEntry{}
	}

	return c
}

// userSubring returns user's cache, or nil, if not available.
func (c *subringCache) userSubring(now time.Time, user string) ring.ReadRing {
	stripeID := shardByUser(user) % numSubringCacheStripes

	return c.stripes[stripeID].userSubring(now, user)
}

func (c *subringCache) setUserSubring(now time.Time, user string, ring ring.ReadRing) {
	stripeID := shardByUser(user) % numSubringCacheStripes

	c.stripes[stripeID].setUserSubring(now, user, ring)
}

// purge removes entries older than given timestamp from the cache. This function should be called
// periodically to avoid memory leaks. Returns number of entries left in the cache.
func (c *subringCache) purge(keepUntil time.Time) int {
	entries := 0
	for s := 0; s < numSubringCacheStripes; s++ {
		entries += c.stripes[s].purge(keepUntil)
	}
	return entries
}

func (s *subringCacheStripe) userSubring(now time.Time, user string) ring.ReadRing {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e := s.entries[user]
	if e != nil {
		e.touchedAt.Store(now.UnixNano())
		return e.cache
	}
	return nil
}

func (s *subringCacheStripe) setUserSubring(now time.Time, user string, ring ring.ReadRing) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ring == nil {
		delete(s.entries, user)
		return
	}

	e := s.entries[user]
	if e == nil {
		e = &subringCacheEntry{}
		s.entries[user] = e
	}
	e.cache = ring
	e.touchedAt.Store(now.UnixNano())
}

// Returns number of entries left in the stripe.
func (s *subringCacheStripe) purge(keepUntil time.Time) int {
	keepUntilNanos := keepUntil.UnixNano()

	s.mu.Lock()
	defer s.mu.Unlock()

	entries := 0

	for u, e := range s.entries {
		ts := e.touchedAt.Load()
		if ts < keepUntilNanos {
			delete(s.entries, u)
		} else {
			entries++
		}
	}
	return entries
}
