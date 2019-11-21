package store

import (
	"context"

	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type BucketStoreBackend interface {
	InitialSync(ctx context.Context) error
	SyncBlocks(ctx context.Context) error
	GetBlocksFor(ctx context.Context, matchers []labels.Matcher, mint, maxt, maxResolutionMillis int64) ([]*bucketBlocksWithMatcher, error)
	IndexReaderForAllBlocks(ctx context.Context) []*bucketIndexReader
	NumBlocks() int
	TimeRange() (mint, maxt int64)
	LabelSets() []storepb.LabelSet
	Close() error
}
