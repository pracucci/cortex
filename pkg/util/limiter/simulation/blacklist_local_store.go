package main

import (
	"time"
)

type blacklistLocalStore struct {
	blockedUntil time.Time
}

func newBlacklistLocalStore() *blacklistLocalStore {
	return &blacklistLocalStore{
		blockedUntil: time.Unix(0, 0),
	}
}

func (s *blacklistLocalStore) Block(now, until time.Time) {
	s.blockedUntil = until
}

func (s *blacklistLocalStore) Blocked(now time.Time) bool {
	return s.blockedUntil.After(now)
}
