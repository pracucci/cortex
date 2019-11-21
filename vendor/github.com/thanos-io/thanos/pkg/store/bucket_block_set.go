package store

import (
	"sync"
	"sort"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

// bucketBlockSet holds all blocks of an equal label set. It internally splits
// them up by downsampling resolution and allows querying.
type bucketBlockSet struct {
	labels      labels.Labels
	mtx         sync.RWMutex
	resolutions []int64          // Available resolution, high to low (in milliseconds).
	blocks      [][]*bucketBlock // Ordered buckets for the existing resolutions.
}

// newBucketBlockSet initializes a new set with the known downsampling windows hard-configured.
// The set currently does not support arbitrary ranges.
func newBucketBlockSet(lset labels.Labels) *bucketBlockSet {
	return &bucketBlockSet{
		labels:      lset,
		resolutions: []int64{downsample.ResLevel2, downsample.ResLevel1, downsample.ResLevel0},
		blocks:      make([][]*bucketBlock, 3),
	}
}

func (s *bucketBlockSet) add(b *bucketBlock) error {
	if !s.labels.Equals(labels.FromMap(b.meta.Thanos.Labels)) {
		return errors.New("block's label set does not match set")
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	i := int64index(s.resolutions, b.meta.Thanos.Downsample.Resolution)
	if i < 0 {
		return errors.Errorf("unsupported downsampling resolution %d", b.meta.Thanos.Downsample.Resolution)
	}
	bs := append(s.blocks[i], b)
	s.blocks[i] = bs

	sort.Slice(bs, func(j, k int) bool {
		return bs[j].meta.MinTime < bs[k].meta.MinTime
	})
	return nil
}

func (s *bucketBlockSet) remove(id ulid.ULID) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for i, bs := range s.blocks {
		for j, b := range bs {
			if b.meta.ULID != id {
				continue
			}
			s.blocks[i] = append(bs[:j], bs[j+1:]...)
			return
		}
	}
}

func int64index(s []int64, x int64) int {
	for i, v := range s {
		if v == x {
			return i
		}
	}
	return -1
}

// getFor returns a time-ordered list of blocks that cover date between mint and maxt.
// Blocks with the biggest resolution possible but not bigger than the given max resolution are returned.
func (s *bucketBlockSet) getFor(mint, maxt, maxResolutionMillis int64) (bs []*bucketBlock) {
	if mint == maxt {
		return nil
	}

	s.mtx.RLock()
	defer s.mtx.RUnlock()

	// Find first matching resolution.
	i := 0
	for ; i < len(s.resolutions) && s.resolutions[i] > maxResolutionMillis; i++ {
	}

	// Fill the given interval with the blocks for the current resolution.
	// Our current resolution might not cover all data, so recursively fill the gaps with higher resolution blocks if there is any.
	start := mint
	for _, b := range s.blocks[i] {
		if b.meta.MaxTime <= mint {
			continue
		}
		if b.meta.MinTime >= maxt {
			break
		}

		if i+1 < len(s.resolutions) {
			bs = append(bs, s.getFor(start, b.meta.MinTime, s.resolutions[i+1])...)
		}
		bs = append(bs, b)
		start = b.meta.MaxTime
	}

	if i+1 < len(s.resolutions) {
		bs = append(bs, s.getFor(start, maxt, s.resolutions[i+1])...)
	}
	return bs
}

// labelMatchers verifies whether the block set matches the given matchers and returns a new
// set of matchers that is equivalent when querying data within the block.
func (s *bucketBlockSet) labelMatchers(matchers ...labels.Matcher) ([]labels.Matcher, bool) {
	res := make([]labels.Matcher, 0, len(matchers))

	for _, m := range matchers {
		v := s.labels.Get(m.Name())
		if v == "" {
			res = append(res, m)
			continue
		}
		if !m.Matches(v) {
			return nil, false
		}
	}
	return res, true
}
