package store

import (
	"context"
	"regexp"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/strutil"
)

var (
	blockRefPattern = regexp.MustCompile("^mint-(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{9}Z)-maxt-(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{9}Z)-([A-Z0-9]+)$")
)

// BucketDiscovery keeps an updated list of blocks stored in the bucket
// without requiring to initially scan the entire bucket. The bucket is
// listed lazily upon requests for a specific time range.
type BucketDiscovery struct {
	logger          log.Logger
	bucket          objstore.BucketReader
	maxBlockRangeMs int64

	blocksLock   sync.RWMutex
	blocksRanges TimeRanges
	blocks       map[ulid.ULID]BlockRef
}

type BlockRef struct {
	id   ulid.ULID
	mint int64
	maxt int64
}

type blocksListRequest struct {
	timeRange    TimeRange
	bucketPrefix string
}

// NewBucketDiscovery makes a new BucketDiscovery.
func NewBucketDiscovery(bucket objstore.BucketReader, maxBlockRange time.Duration, logger log.Logger) *BucketDiscovery {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	return &BucketDiscovery{
		logger:          logger,
		bucket:          bucket,
		maxBlockRangeMs: maxBlockRange.Milliseconds(),
		blocks:          make(map[ulid.ULID]BlockRef, 0),
	}
}

// GetBlocks returns a list of blocks containing samples within the input time
// range. Blocks are directly returned from the local cache if available, otherwise
// they're fetched iterating the bucket.
func (d *BucketDiscovery) GetBlocks(ctx context.Context, mint, maxt int64) ([]BlockRef, error) {
	// Ensure all blocks within the input time range have been fetched from the bucket
	err := d.getMissingBlocks(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}

	// Build the list of blocks to return
	result := make([]BlockRef, 0)

	d.blocksLock.RLock()
	// TODO(pracucci) optimize it with an ordered list by mint
	for _, ref := range d.blocks {
		if ref.mint <= maxt && ref.maxt >= mint {
			result = append(result, ref)
		}
	}
	d.blocksLock.RUnlock()

	return result, nil
}

// TODO how to deal with the fact that queries against the current time range will always be
// 		greater than the last maxt?
func (d *BucketDiscovery) getMissingBlocks(ctx context.Context, mint, maxt int64) error {
	reqs := d.getMissingBlocksRequests(mint, maxt)

	for _, req := range reqs {
		// Skip if the time range has already been fetched in the meanwhile
		d.blocksLock.RLock()
		skip := len(d.blocksRanges.Xor(req.timeRange)) == 0
		d.blocksLock.RUnlock()

		if skip {
			continue
		}

		var discovered []BlockRef

		// Iterate the bucket with the given prefix and discover all blocks within the range
		err := d.bucket.IterPrefix(ctx, req.bucketPrefix, func(name string) error {
			// Parse it stripping the trailing slash indicating a directory.
			parts := blockRefPattern.FindStringSubmatch(name[:len(name)-1])
			if parts == nil || len(parts) != 4 {
				return nil
			}

			// Parse mint, maxt and block ID
			mint, err := time.Parse(time.RFC3339Nano, parts[1])
			if err != nil {
				level.Warn(d.logger).Log("msg", "unable to parse mint from block name", "err", err, "name", name)
				return nil
			}

			maxt, err := time.Parse(time.RFC3339Nano, parts[2])
			if err != nil {
				level.Warn(d.logger).Log("msg", "unable to parse maxt from block name", "err", err, "name", name)
				return nil
			}

			id, err := ulid.Parse(parts[3])
			if err != nil {
				level.Warn(d.logger).Log("msg", "unable to parse block ID block name", "err", err, "name", name)
				return nil
			}

			discovered = append(discovered, BlockRef{
				id:   id,
				mint: toMillis(mint),
				maxt: toMillis(maxt),
			})

			return nil
		})

		if err != nil {
			return errors.Wrap(err, "error while listing blocks")
		}

		// Store discovered blocks
		d.blocksLock.Lock()

		for _, ref := range discovered {
			if _, ok := d.blocks[ref.id]; ok {
				continue
			}

			d.blocks[ref.id] = ref
		}

		d.blocksRanges.Add(req.timeRange)
		d.blocksLock.Unlock()
	}

	return nil
}

// TODO(pracucci) this can be optimized splitting multiple months ranges into single month ranges
func (d *BucketDiscovery) getMissingBlocksRequests(mint, maxt int64) []blocksListRequest {
	// Given we iterate on a subset of the bucket based on the blocks mint,
	// we need to make sure that we fetch all blocks with the oldest sample
	// < mint and the newest sample >= mint.
	mint = mint - d.maxBlockRangeMs

	// Build a list of time ranges for which blocks are missing
	d.blocksLock.RLock()
	missingRanges := d.blocksRanges.Xor(TimeRange{mint, maxt})
	d.blocksLock.RUnlock()

	// Normalize the list of time ranges into daily time ranges, to avoid very
	// fragmented requests.
	normalizedRanges := missingRanges.Normalize((24 * time.Hour).Milliseconds())

	reqs := make([]blocksListRequest, 0, len(normalizedRanges))

	for _, r := range normalizedRanges {
		// Look for the longest common prefix between the mint and maxt
		minTimestamp := r.MinTime().Format(time.RFC3339Nano)
		maxTimestamp := r.MaxTime().Format(time.RFC3339Nano)
		prefixTimestamp := strutil.CommonPrefix(minTimestamp, maxTimestamp)

		// Given the constructed prefix may be larger then the requested range,
		// we have to rebuild the actual time range
		// TODO(pracucci) need to find a smart idea to solve it

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
	return input.UnixNano() / 1000000
}
