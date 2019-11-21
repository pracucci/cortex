package store

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/objstore"
)

type BucketLazyReader struct {
	logger    log.Logger
	bucket    objstore.BucketReader
	discovery *BucketDiscovery
}

func NewBucketLazyReader(bucket objstore.BucketReader, maxBlockRange time.Duration, logger log.Logger) *BucketLazyReader {
	return &BucketLazyReader{
		logger:    logger,
		bucket:    bucket,
		discovery: NewBucketDiscovery(bucket, maxBlockRange, logger),
	}
}

func (r *BucketLazyReader) GetBlocks(ctx context.Context, mint, maxt, maxResolutionMillis int64) ([]*bucketBlock, error) {
	// Get block references
	refs, err := r.discovery.GetBlocks(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}

	// TODO continue implementation
	fmt.Println("refs:", refs)
	return nil, fmt.Errorf("not implemented")
}

func (r *BucketLazyReader) Close() error {
	// TODO build me
	return nil
}
