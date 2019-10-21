package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/cortexproject/cortex/pkg/util/limiter"
)

var (
	debug = false
)

func main() {
	runMultiSimulations()
}

func runSingleSimulation() {
	// Config
	limit := 3000
	bucketPeriodSecs := 10
	distributors := 60
	clientSamplesPerRequest := 1000
	clientRequestsPerSec := 20
	duration := 60 * time.Second
	sharedStateEnabled := false
	sharedStateLatency := 1000 * time.Millisecond

	report := runSimulation("Test", limit, bucketPeriodSecs, distributors, clientSamplesPerRequest, clientRequestsPerSec, duration, sharedStateEnabled, sharedStateLatency)

	// Print report
	fmt.Println("== RESULT ==")
	fmt.Println(report.String())
}

func runMultiSimulations() {
	// Config
	limit := 3000
	bucketPeriodSecs := 10
	distributors := 60
	clientSamplesPerRequest := []int{150, 300, 450, 600, 750, 1000}
	clientRequestsPerSec := 20
	duration := 60 * time.Second
	sharedStateLatency := []time.Duration{100 * time.Millisecond, 250 * time.Millisecond, 500 * time.Millisecond, 1000 * time.Millisecond}

	// Print CSV header
	header := strings.Builder{}
	header.WriteString("limit,bucket period,distributors,client req/s,client tokens/req,local blacklist")
	for _, latency := range sharedStateLatency {
		header.WriteString(fmt.Sprintf(",shared shared blacklist %v latency", latency))
	}
	fmt.Println(header.String())

	// Run simulation
	for _, samplesPerReq := range clientSamplesPerRequest {
		entry := strings.Builder{}
		entry.WriteString(fmt.Sprintf("%d,%d,%d,%d,%d", limit, bucketPeriodSecs, distributors, clientRequestsPerSec, samplesPerReq))

		// Local blacklist store
		report := runSimulation("local blacklist", limit, bucketPeriodSecs, distributors, samplesPerReq, clientRequestsPerSec, duration, false, time.Duration(0))
		entry.WriteString(fmt.Sprintf(",%.0f", report.avgAcceptedTokensPerSec()))

		// Shared blacklist store
		for _, latency := range sharedStateLatency {
			name := fmt.Sprintf("shared blacklist %v latency", latency)
			report := runSimulation(name, limit, bucketPeriodSecs, distributors, samplesPerReq, clientRequestsPerSec, duration, true, latency)
			entry.WriteString(fmt.Sprintf(",%.0f", report.avgAcceptedTokensPerSec()))
		}

		// Print CSV entry
		fmt.Println(entry.String())
	}
}

func runSimulation(name string, limit, bucketPeriodSecs, distributors, clientSamplesPerRequest, clientRequestsPerSec int, duration time.Duration, sharedStateEnabled bool, sharedStateLatency time.Duration) *report {
	balancer := newRoundRobinBalancer(distributors)
	report := newReport(name, limit, bucketPeriodSecs, distributors, clientSamplesPerRequest, clientRequestsPerSec)

	// Init stores
	sharedCounters := newSharedCountersStore()
	var sharedBlacklist limiter.GlobalLimiterBlacklistStore
	if sharedStateEnabled {
		sharedBlacklist = newBlacklistSharedStore(sharedStateLatency)
	}

	// Init limiters (1 for each simulated distributor)
	limiters := map[int]*limiter.GlobalLimiter{}
	for i := 0; i < distributors; i++ {
		if sharedStateEnabled {
			limiters[i] = limiter.NewGlobalLimiter(limit, bucketPeriodSecs, sharedCounters, sharedBlacklist)
		} else {
			limiters[i] = limiter.NewGlobalLimiter(limit, bucketPeriodSecs, sharedCounters, newBlacklistLocalStore())
		}
	}

	// Run simulation
	reqInterval := int64((1 / float64(clientRequestsPerSec)) * float64(time.Second))
	currTime := time.Date(2019, time.October, 10, 10, 10, 0, 0, time.UTC)
	endTime := currTime.Add(duration)

	for ; currTime.Before(endTime); currTime = currTime.Add(time.Duration(reqInterval)) {
		nextDistributorID := balancer.nextTargetID()
		limiter, _ := limiters[nextDistributorID]

		if debug {
			fmt.Println("DISTRIBUTOR", nextDistributorID)
		}

		// Get the report for the current bucket
		currBucketKey := currTime.Truncate(time.Duration(bucketPeriodSecs) * time.Second).Unix()

		// Do not track requests which have been blocked
		if !limiter.Exceeded(currTime) {
			report.addAccepted(currBucketKey, clientSamplesPerRequest)
			limiter.Consume(currTime, clientSamplesPerRequest, debug)
		} else {
			report.addBlocked(currBucketKey, clientSamplesPerRequest)

			if debug {
				fmt.Println("Skipped because rate limited")
			}
		}
	}

	if sharedStateEnabled && debug {
		sharedStateUpdatesCount := sharedBlacklist.(*blacklistSharedStore).updatesCount
		fmt.Println("")
		fmt.Println("Shared blacklist state updates count:", sharedStateUpdatesCount, float64(sharedStateUpdatesCount)/duration.Seconds(), "/sec")
	}

	return report
}
