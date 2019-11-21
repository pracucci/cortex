package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/storage/tsdb/util"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"
)

// BucketMapper implements an asynchronous mapper to keep an updated list
// of blocks stored with a bucket.
type BucketMapper struct {
	bucket          objstore.BucketReader
	maxBlockRangeMs int64

	blocksLock   sync.RWMutex
	blocksRanges TimeRanges
}

// NewBucketMapper makes a new BucketMapper.
func NewBucketMapper(bucket objstore.BucketReader, maxBlockRange time.Duration) *BucketMapper {
	return &BucketMapper{
		bucket:          bucket,
		maxBlockRangeMs: maxBlockRange.Milliseconds(),
	}
}

// GetBlocks returns a list of blocks containing samples within the input time
// range. Blocks are directly returned from the local cache if available, otherwise
// they're fetched iterating the bucket.
func (m *BucketMapper) GetBlocks(ctx context.Context, mint, maxt int64) {
	// Ensure all blocks within the input time range have been fetched from the bucket
	// TODO

	// Build the list of blocks to return
	// TODO
}

// TODO how to deal with the fact that queries against the current time range will always be
// 		greater than the last maxt?
func (m *BucketMapper) getMissingBlocks(ctx context.Context, mint, maxt int64) error {
	reqs := m.getMissingBlocksRequests(mint, maxt)

	for _, req := range reqs {
		// Skip if the time range has already been fetched in the meanwhile
		m.blocksLock.RLock()
		skip := len(m.blocksRanges.Xor(req.timeRange)) == 0
		m.blocksLock.RUnlock()

		if skip {
			continue
		}

		// Iterate the bucket with the given prefix and collect all blocks
		// TODO we need to hack around the bucket implementation, cause it currently enforce the delimiter at the end
		fmt.Println("bucket.IterPrefix() with prefix:", req.bucketPrefix)
		err := m.bucket.IterPrefix(ctx, req.bucketPrefix, func(name string) error {
			// TODO extract mint, maxt and block id
			fmt.Println("Name:", name)

			return nil
		})

		if err != nil {
			return errors.Wrap(err, "error while listing blocks")
		}

		// Store listed blocks
		m.blocksLock.Lock()
		// TODO
		m.blocksLock.Unlock()

		// TODO where should we push the job to fetch metadata for required blocks?
		// 		if we do it here, we may end up fetching a bunch of blocks we don't really care
		// 		it could be wiser to do it in the caller
	}

	/*
		err := m.bucket.Iter(ctx, "", func(name string) error {
			// Strip trailing slash indicating a directory.
			id, err := ulid.Parse(name[:len(name)-1])
			if err != nil {
				return nil
			}

			inRange, err := s.isBlockInMinMaxRange(ctx, id)
			if err != nil {
				level.Warn(s.logger).Log("msg", "error parsing block range", "block", id, "err", err)
				return nil
			}

			if !inRange {
				return nil
			}

			allIDs[id] = struct{}{}

			if b := s.getBlock(id); b != nil {
				return nil
			}
			select {
			case <-ctx.Done():
			case blockc <- id:
			}
			return nil
		})
	*/

	return nil
}

/* TODO where should I put this?
func (i *BucketMapper) GetBlocksFor(matchers []labels.Matcher, mint, maxt, maxResolutionMillis int64) (bs []*bucketBlock) {

	// TODO Get the list of blocks between the input time range
	// - Check if the input time range is already within the sparse ranges we've iterated over. If not:
	//   - Calculate the time ranges (multiple) over which we wanna iterate
	//     - How to deal with compaction?
	//   - Iterate over the bucket and - for each new block - issue a sync

	// TODO(pracucci) build me based on getFor()

	return nil
}
*/

type blocksListRequest struct {
	timeRange    TimeRange
	bucketPrefix string
}

// TODO(pracucci) this can be optimized splitting multiple months ranges into single month ranges
func (m *BucketMapper) getMissingBlocksRequests(mint, maxt int64) []blocksListRequest {
	// Given we iterate on a subset of the bucket based on the blocks mint,
	// we need to make sure that we fetch all blocks with the oldest sample
	// < mint and the newest sample >= mint.
	mint = mint - m.maxBlockRangeMs

	// Build a list of time ranges for which blocks are missing
	m.blocksLock.RLock()
	missingRanges := m.blocksRanges.Xor(TimeRange{mint, maxt})
	m.blocksLock.RUnlock()

	// Normalize the list of time ranges into daily time ranges, to avoid very
	// fragmented requests.
	normalizedRanges := missingRanges.Normalize((24 * time.Hour).Milliseconds())

	reqs := make([]blocksListRequest, 0, len(normalizedRanges))

	for _, r := range normalizedRanges {
		// Look for the longest common prefix between the mint and maxt
		minTimestamp := r.MinTime().Format(time.RFC3339Nano)
		maxTimestamp := r.MaxTime().Format(time.RFC3339Nano)
		prefixTimestamp := util.CommonPrefix(minTimestamp, maxTimestamp)

		// Given the constructed prefix may be larger then the requested range,
		// we have to rebuild the actual time range
		// TODO(pracucci) It's late and I can't come up with a smart idea to solve it

		reqs = append(reqs, blocksListRequest{
			timeRange:    r,
			bucketPrefix: "mint-" + prefixTimestamp,
		})
	}

	return reqs
}

func mustParseTime(layout string, input string) time.Time {
	t, err := time.Parse(layout, input)
	if err != nil {
		panic(err)
	}

	return t
}

func toMillis(input time.Time) int64 {
	return input.Unix() * 1000
}
