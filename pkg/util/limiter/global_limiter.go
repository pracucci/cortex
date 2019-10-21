package limiter

import (
	"fmt"
	"math"
	"time"
)

// TODO: rename period into bucketPeriod
type GlobalLimiter struct {
	// Max number of tokens per second, averaged within the bucket period
	limit float64

	// Bucket time span period in seconds
	period int64

	countersStore  GlobalLimiterCountersStore
	blacklistStore GlobalLimiterBlacklistStore

	// Internal state
	periodDuration time.Duration
	periodFloat    float64
}

func NewGlobalLimiter(limit int, period int, countersStore GlobalLimiterCountersStore, blacklistStore GlobalLimiterBlacklistStore) *GlobalLimiter {
	return &GlobalLimiter{
		limit:          float64(limit),
		period:         int64(period),
		countersStore:  countersStore,
		blacklistStore: blacklistStore,
		periodDuration: time.Duration(period) * time.Second,
		periodFloat:    float64(period),
	}
}

func (r *GlobalLimiter) Consume(now time.Time, tokens int, debug bool) {
	currBucketStartTime := now.Truncate(r.periodDuration)
	currBucketKey := currBucketStartTime.Unix()
	prevBucketKey := currBucketKey - r.period

	// Increase current bucket tokens
	r.countersStore.IncreaseTokens(currBucketKey, tokens)

	// Get tokens
	currTokens, prevTokens := r.countersStore.GetTokens(currBucketKey, prevBucketKey)

	// Check if the rate limit has been exceeded
	if exceeded, until := r.calculateRate(now, prevTokens, currTokens, debug); exceeded {
		r.blacklistStore.Block(now, *until)
	}
}

func (r *GlobalLimiter) Exceeded(now time.Time) bool {
	return r.blacklistStore.Blocked(now)
}

func (r *GlobalLimiter) calculateRate(now time.Time, prevTokens int, currTokens int, debug bool) (bool, *time.Time) {
	currBucketStartTime := now.Truncate(r.periodDuration)
	currBucketAgeSecs := now.Sub(currBucketStartTime).Seconds()

	// Calculate the current rate
	prevBucketLeftTime := r.periodFloat - currBucketAgeSecs
	prevBucketPartialTokens := float64(prevTokens) * (prevBucketLeftTime / r.periodFloat)
	currRatePerSec := (prevBucketPartialTokens + float64(currTokens)) / r.periodFloat

	if debug {
		fmt.Println("now:                ", now)
		fmt.Println("currBucketAgeSecs:  ", currBucketAgeSecs)
		fmt.Println("prevBucketPartialTokens:", prevBucketPartialTokens)
		fmt.Println("currTokens:             ", currTokens)
		fmt.Println("currRatePerSec:         ", currRatePerSec)
	}

	// Check if limit has been exceeded
	if currRatePerSec < r.limit {
		return false, nil
	}

	// Calculate for how long the rate limit will be exceeded
	prevDecayRatePerSec := float64(prevTokens) / r.periodFloat
	prevBucketAgeSecs := r.periodFloat - currBucketAgeSecs
	exceedingRatePerSec := currRatePerSec - r.limit
	var exceedUntilSecs float64

	if float64(currTokens) >= (r.limit*r.periodFloat) || prevDecayRatePerSec <= 0 {
		// No more requests accepted until the end of this time span
		exceedUntilSecs = (r.periodFloat - currBucketAgeSecs)
	} else {
		exceedUntilSecs = math.Min(prevBucketAgeSecs, exceedingRatePerSec/prevDecayRatePerSec)
	}

	exceedUntil := now.Add(time.Duration(exceedUntilSecs * float64(time.Second)))

	if debug {
		fmt.Println("prevDecayRatePerSec:", prevDecayRatePerSec)
		fmt.Println("exceedingRatePerSec:", exceedingRatePerSec)
		fmt.Println("exceedUntilSecs:    ", exceedUntilSecs)
		fmt.Println("exceedUntil:        ", exceedUntil)
	}

	return true, &exceedUntil
}
