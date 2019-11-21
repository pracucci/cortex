package store

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/pool"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type BucketStoreGreedyBackend struct {
	logger       log.Logger
	bucket       objstore.BucketReader
	dir          string
	filterConfig *FilterConfig

	// Number of goroutines to use when syncing blocks from object storage.
	blockSyncConcurrency int

	blocks *bucketBlocks

	// Labels within all blocks
	labelSetsMtx sync.RWMutex
	labelSets    map[uint64]labels.Labels
}

func NewBucketStoreGreedyBackend(
	logger log.Logger,
	reg prometheus.Registerer,
	bucket objstore.BucketReader,
	dir string,
	indexCache indexCache,
	blockSyncConcurrency int,
	relabelConfig []*relabel.Config,
	chunkPool *pool.BytesPool,
	filterConfig *FilterConfig,
	partitioner partitioner,
) *BucketStoreGreedyBackend {
	blocks := newBucketBlocks(
		logger,
		reg,
		bucket,
		dir,
		indexCache,
		chunkPool,
		partitioner,
		relabelConfig)

	return &BucketStoreGreedyBackend{
		logger:               logger,
		bucket:               bucket,
		dir:                  dir,
		blockSyncConcurrency: blockSyncConcurrency,
		blocks:               blocks,
		filterConfig:         filterConfig,
	}
}

func (b *BucketStoreGreedyBackend) GetBlocksFor(ctx context.Context, matchers []labels.Matcher, mint, maxt, maxResolutionMillis int64) ([]*bucketBlocksWithMatcher, error) {
	return b.blocks.getBlocksFor(matchers, mint, maxt, maxResolutionMillis), nil
}

func (b *BucketStoreGreedyBackend) NumBlocks() int {
	return b.blocks.numBlocks()
}

func (b *BucketStoreGreedyBackend) TimeRange() (mint, maxt int64) {
	return b.blocks.timeRange()
}

func (b *BucketStoreGreedyBackend) IndexReaderForAllBlocks(ctx context.Context) []*bucketIndexReader {
	return b.blocks.indexReaderForAllBlocks(ctx)
}

func (b *BucketStoreGreedyBackend) Close() error {
	return b.blocks.close()
}

// SyncBlocks synchronizes the stores state with the Bucket bucket.
// It will reuse disk space as persistent cache based on s.dir param.
func (b *BucketStoreGreedyBackend) SyncBlocks(ctx context.Context) error {
	var wg sync.WaitGroup
	blockc := make(chan ulid.ULID)

	for i := 0; i < b.blockSyncConcurrency; i++ {
		wg.Add(1)
		go func() {
			for id := range blockc {
				if err := b.blocks.addBlock(ctx, id); err != nil {
					level.Warn(b.logger).Log("msg", "loading block failed", "id", id, "err", err)
					continue
				}
			}
			wg.Done()
		}()
	}

	allIDs := map[ulid.ULID]struct{}{}

	err := b.bucket.Iter(ctx, "", func(name string) error {
		// Strip trailing slash indicating a directory.
		id, err := ulid.Parse(name[:len(name)-1])
		if err != nil {
			return nil
		}

		inRange, err := b.isBlockInMinMaxRange(ctx, id)
		if err != nil {
			level.Warn(b.logger).Log("msg", "error parsing block range", "block", id, "err", err)
			return nil
		}

		if !inRange {
			return nil
		}

		allIDs[id] = struct{}{}

		if b := b.blocks.getBlock(id); b != nil {
			return nil
		}
		select {
		case <-ctx.Done():
		case blockc <- id:
		}
		return nil
	})

	close(blockc)
	wg.Wait()

	if err != nil {
		return errors.Wrap(err, "iter")
	}

	// Drop all blocks that are no longer present in the bucket.
	b.blocks.removeBlocksExcept(allIDs)

	// Sync advertise labels.
	b.labelSetsMtx.Lock()
	b.labelSets = b.blocks.labelSets()
	b.labelSetsMtx.Unlock()

	return nil
}

// InitialSync perform blocking sync with extra step at the end to delete locally saved blocks that are no longer
// present in the bucket. The mismatch of these can only happen between restarts, so we can do that only once per startup.
func (b *BucketStoreGreedyBackend) InitialSync(ctx context.Context) error {
	if err := b.SyncBlocks(ctx); err != nil {
		return errors.Wrap(err, "sync block")
	}

	names, err := fileutil.ReadDir(b.dir)
	if err != nil {
		return errors.Wrap(err, "read dir")
	}
	for _, n := range names {
		id, ok := block.IsBlockDir(n)
		if !ok {
			continue
		}
		if b := b.blocks.getBlock(id); b != nil {
			continue
		}

		// No such block loaded, remove the local dir.
		if err := os.RemoveAll(path.Join(b.dir, id.String())); err != nil {
			level.Warn(b.logger).Log("msg", "failed to remove block which is not needed", "err", err)
		}
	}

	return nil
}

func (b *BucketStoreGreedyBackend) LabelSets() []storepb.LabelSet {
	b.labelSetsMtx.RLock()

	output := make([]storepb.LabelSet, 0, len(b.labelSets))
	for _, ls := range b.labelSets {
		lset := make([]storepb.Label, 0, len(ls))
		for _, l := range ls {
			lset = append(lset, storepb.Label{Name: l.Name, Value: l.Value})
		}
		output = append(output, storepb.LabelSet{Labels: lset})
	}

	b.labelSetsMtx.RUnlock()

	return output
}

func (b *BucketStoreGreedyBackend) isBlockInMinMaxRange(ctx context.Context, id ulid.ULID) (bool, error) {
	dir := filepath.Join(b.dir, id.String())

	err, meta := loadMeta(ctx, b.logger, b.bucket, dir, id)
	if err != nil {
		return false, err
	}

	// We check for blocks in configured minTime, maxTime range.
	switch {
	case meta.MaxTime <= b.filterConfig.MinTime.PrometheusTimestamp():
		return false, nil

	case meta.MinTime >= b.filterConfig.MaxTime.PrometheusTimestamp():
		return false, nil
	}

	return true, nil
}
