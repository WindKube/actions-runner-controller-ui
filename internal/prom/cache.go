package prom

import (
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/singleflight"
)

const cacheEntries = 2048

// cache shares answers between SSE clients for one TTL and collapses
// concurrent identical queries into one request. Errors are never cached.
type cache struct {
	lru    *expirable.LRU[string, any]
	flight singleflight.Group
}

func newCache(ttl time.Duration) *cache {
	if ttl <= 0 {
		return &cache{}
	}
	return &cache{lru: expirable.NewLRU[string, any](cacheEntries, nil, ttl)}
}

func (c *cache) do(key string, fetch func() (any, error)) (any, error) {
	if c.lru == nil {
		v, err, _ := c.flight.Do(key, fetch)
		return v, err
	}
	if v, ok := c.lru.Get(key); ok {
		return v, nil
	}
	v, err, _ := c.flight.Do(key, func() (any, error) {
		if v, ok := c.lru.Get(key); ok {
			return v, nil
		}
		v, err := fetch()
		if err == nil {
			c.lru.Add(key, v)
		}
		return v, err
	})
	return v, err
}
