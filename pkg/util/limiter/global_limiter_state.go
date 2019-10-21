package limiter

import "time"

type GlobalLimiterCountersStore interface {
	IncreaseTokens(currBucketKey int64, tokens int) int
	GetTokens(currBucketKey, prevBucketKey int64) (int, int)
}

type GlobalLimiterBlacklistStore interface {
	Block(now, until time.Time)
	Blocked(now time.Time) bool
}
