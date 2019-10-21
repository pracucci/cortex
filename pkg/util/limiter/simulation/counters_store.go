package main

type sharedCountersStore struct {
	tokens map[int64]int
}

func newSharedCountersStore() *sharedCountersStore {
	return &sharedCountersStore{
		tokens: map[int64]int{},
	}
}

func (s *sharedCountersStore) IncreaseTokens(currBucketKey int64, tokens int) int {
	value, _ := s.tokens[currBucketKey]
	value += tokens
	s.tokens[currBucketKey] = value

	return value
}

func (s *sharedCountersStore) GetTokens(currBucketKey, prevBucketKey int64) (int, int) {
	currValue, _ := s.tokens[currBucketKey]
	prevValue, _ := s.tokens[prevBucketKey]

	return currValue, prevValue
}
