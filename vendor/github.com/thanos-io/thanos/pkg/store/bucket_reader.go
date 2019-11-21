package store

import "context"

// TODO(pracucci) rename, because conflicts with objstore.BucketReader
type BucketReader interface {
	GetBlocks(ctx context.Context, mint, maxt, maxResolutionMillis int64) ([]*bucketBlock, error)
	Close() error
}
