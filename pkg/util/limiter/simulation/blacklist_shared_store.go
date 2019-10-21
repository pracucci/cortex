package main

import (
	"time"
)

type blacklistSharedStore struct {
	blockedUntil time.Time
	latency      time.Duration

	// To simulate the update latency, we keep a list of pending update requests
	pending []blacklistSharedStoreUpdateRequest

	// Keep some statistics
	updatesCount int
}

type blacklistSharedStoreUpdateRequest struct {
	updatedAt    time.Time
	blockedUntil time.Time
}

func newBlacklistSharedStore(latency time.Duration) *blacklistSharedStore {
	return &blacklistSharedStore{
		blockedUntil: time.Unix(0, 0),
		latency:      latency,
		pending:      make([]blacklistSharedStoreUpdateRequest, 0),
	}
}

func (s *blacklistSharedStore) Block(now, until time.Time) {
	s.pending = append(s.pending, blacklistSharedStoreUpdateRequest{
		updatedAt:    now.Add(s.latency),
		blockedUntil: until,
	})

	s.updatesCount++
}

func (s *blacklistSharedStore) Blocked(now time.Time) bool {
	// To simulate the update latency we keep a list of pending update
	// requests, which we're now going to process
	keep := 0

	for _, req := range s.pending {
		if req.updatedAt.After(now) {
			s.pending[keep] = req
			keep++
			continue
		}

		// Update the timestamp, keeping the max value
		if s.blockedUntil.Before(req.blockedUntil) {
			s.blockedUntil = req.blockedUntil
		}
	}

	// Since we have moved the requests not yet processed to the beginning
	// of the slice, we can reduce the slice to only keep them
	s.pending = s.pending[:keep]

	return now.Before(s.blockedUntil)
}
