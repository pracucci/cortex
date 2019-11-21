package store

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type BucketStoreLazyBackend struct {
	logger    log.Logger
	bucket    objstore.BucketReader
	discovery *BucketDiscovery
}

func NewBucketStoreLazyBackend(bucket objstore.BucketReader, maxBlockRange time.Duration, logger log.Logger) *BucketStoreLazyBackend {
	return &BucketStoreLazyBackend{
		logger:    logger,
		bucket:    bucket,
		discovery: NewBucketDiscovery(bucket, maxBlockRange, logger),
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
	// Get block references
	refs, err := b.discovery.GetBlocks(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}

	// TODO continue implementation
	fmt.Println("refs:", refs)
	return nil, fmt.Errorf("not implemented")
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
