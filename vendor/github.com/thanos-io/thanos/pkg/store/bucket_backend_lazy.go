package store

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/pool"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type BucketStoreLazyBackend struct {
	logger    log.Logger
	bucket    objstore.BucketReader
	discovery *BucketDiscovery
	blocks    *bucketBlocks
}

func NewBucketStoreLazyBackend(
	logger log.Logger,
	reg prometheus.Registerer,
	bucket objstore.BucketReader,
	dir string,
	indexCache indexCache,
	relabelConfig []*relabel.Config,
	chunkPool *pool.BytesPool,
	// TODO we need this as well filterConfig *FilterConfig,
	partitioner partitioner,
	maxBlockRange time.Duration,
) *BucketStoreLazyBackend {
	blocks := newBucketBlocks(
		logger,
		reg,
		bucket,
		dir,
		indexCache,
		chunkPool,
		partitioner,
		relabelConfig)

	return &BucketStoreLazyBackend{
		logger:    logger,
		bucket:    bucket,
		discovery: NewBucketDiscovery(bucket, maxBlockRange, logger),
		blocks:    blocks,
	}
}

func (b *BucketStoreLazyBackend) InitialSync(ctx context.Context) error {
	// No initial sync
	return nil
}

func (b *BucketStoreLazyBackend) SyncBlocks(ctx context.Context) error {
	return nil
}

func (b *BucketStoreLazyBackend) GetBlocksFor(ctx context.Context, matchers []labels.Matcher, mint, maxt, maxResolutionMillis int64) ([]*bucketBlocksWithMatcher, error) {
	fmt.Println("BucketStoreLazyBackend.GetBlocksFor() mint:", time.Unix(0, mint*1000000).UTC().Format(time.RFC3339Nano), "maxt:", time.Unix(0, maxt*1000000).UTC().Format(time.RFC3339Nano))

	// Get block references
	refs, err := b.discovery.GetBlocks(ctx, mint, maxt)
	fmt.Println("BucketStoreLazyBackend.GetBlocksFor() refs:", refs, "err:", err)
	if err != nil {
		return nil, err
	}

	// Ensure all blocks are added
	// TODO this is very inefficient (the way we check if already exist, no parallelization on loading, etc...)
	// 		but it's just an easy way to prove this works (hopefully)
	for _, ref := range refs {
		if b.blocks.getBlock(ref.id) != nil {
			continue
		}

		fmt.Println("BucketStoreLazyBackend.GetBlocksFor() adding block:", ref.id.String())
		if err := b.blocks.addBlock(ctx, ref.id); err != nil {
			return nil, err
		}
	}
	fmt.Println("BucketStoreLazyBackend.GetBlocksFor() all blocks adedd")

	return b.blocks.getBlocksFor(matchers, mint, maxt, maxResolutionMillis), nil
}

func (b *BucketStoreLazyBackend) LabelSets() []storepb.LabelSet {
	// TODO build me
	return nil
}

func (b *BucketStoreLazyBackend) NumBlocks() int {
	// TODO build me
	return 0
}

func (b *BucketStoreLazyBackend) TimeRange() (mint, maxt int64) {
	// TODO build me
	return 0, 0
}

func (b *BucketStoreLazyBackend) IndexReaderForAllBlocks(ctx context.Context) []*bucketIndexReader {
	// TODO build me
	return nil
}

func (b *BucketStoreLazyBackend) Close() error {
	// TODO build me
	return nil
}
