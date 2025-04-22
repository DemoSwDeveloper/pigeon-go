package cache

import (
	"github.com/patrickmn/go-cache"
	"time"
)

var Cache *cache.Cache

func init() {
	Cache = cache.New(5*time.Minute, 15*time.Minute)
}
