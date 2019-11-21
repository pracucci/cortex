package store

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	promlabels "github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/pool"
)

type bucketBlocks struct {
	logger        log.Logger
	reg           prometheus.Registerer
	bucket        objstore.BucketReader
	dir           string
	indexCache    indexCache
	chunkPool     *pool.BytesPool
	partitioner   partitioner
	relabelConfig []*relabel.Config

	// Sets of blocks that have the same labels. They are indexed by a hash over their label set.
	mtx       sync.RWMutex
	blocks    map[ulid.ULID]*bucketBlock
	blockSets map[uint64]*bucketBlockSet
}

func newBucketBlocks(
	logger log.Logger,
	reg prometheus.Registerer,
	bucket objstore.BucketReader,
	dir string,
	indexCache indexCache,
	chunkPool *pool.BytesPool,
	partitioner partitioner,
	relabelConfig []*relabel.Config,
) *bucketBlocks {
	return &bucketBlocks{
		logger:        logger,
		reg:           reg,
		bucket:        bucket,
		dir:           dir,
		indexCache:    indexCache,
		chunkPool:     chunkPool,
		partitioner:   partitioner,
		relabelConfig: relabelConfig,
		blocks:        map[ulid.ULID]*bucketBlock{},
		blockSets:     map[uint64]*bucketBlockSet{},
	}
}

func (s *bucketBlocks) getBlock(id ulid.ULID) *bucketBlock {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.blocks[id]
}

func (s *bucketBlocks) addBlock(ctx context.Context, id ulid.ULID) (err error) {
	dir := filepath.Join(s.dir, id.String())

	defer func() {
		if err != nil {
			// TODO s.metrics.blockLoadFailures.Inc()
			if err2 := os.RemoveAll(dir); err2 != nil {
				level.Warn(s.logger).Log("msg", "failed to remove block we cannot load", "err", err2)
			}
		}
	}()
	// TODO s.metrics.blockLoads.Inc()

	b, err := newBucketBlock(
		ctx,
		log.With(s.logger, "block", id),
		s.bucket,
		id,
		dir,
		s.indexCache,
		s.chunkPool,
		s.partitioner,
	)
	if err != nil {
		return errors.Wrap(err, "new bucket block")
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()

	lset := labels.FromMap(b.meta.Thanos.Labels)
	h := lset.Hash()

	// Check for block labels by relabeling.
	// If output is empty, the block will be dropped.
	if processedLabels := relabel.Process(promlabels.FromMap(lset.Map()), s.relabelConfig...); processedLabels == nil {
		level.Debug(s.logger).Log("msg", "dropping block(drop in relabeling)", "block", id)
		return os.RemoveAll(dir)
	}
	b.labels = lset
	sort.Sort(b.labels)

	set, ok := s.blockSets[h]
	if !ok {
		set = newBucketBlockSet(lset)
		s.blockSets[h] = set
	}

	if err = set.add(b); err != nil {
		return errors.Wrap(err, "add block to set")
	}
	s.blocks[b.meta.ULID] = b

	// TODO s.metrics.blocksLoaded.Inc()

	return nil
}

func (s *bucketBlocks) removeBlock(id ulid.ULID) error {
	s.mtx.Lock()
	b, ok := s.blocks[id]
	if ok {
		lset := labels.FromMap(b.meta.Thanos.Labels)
		s.blockSets[lset.Hash()].remove(id)
		delete(s.blocks, id)
	}
	s.mtx.Unlock()

	if !ok {
		return nil
	}

	// TODO s.metrics.blocksLoaded.Dec()
	if err := b.Close(); err != nil {
		return errors.Wrap(err, "close block")
	}
	return os.RemoveAll(b.dir)
}

func (s *bucketBlocks) removeBlocksExcept(keepIds map[ulid.ULID]struct{}) {
	s.mtx.Lock()

	for id := range s.blocks {
		if _, ok := keepIds[id]; ok {
			continue
		}
		if err := s.removeBlock(id); err != nil {
			level.Warn(s.logger).Log("msg", "drop outdated block", "block", id, "err", err)
			// TODO s.metrics.blockDropFailures.Inc()
		}
		// TODO s.metrics.blockDrops.Inc()
	}

	s.mtx.Unlock()
}

func (s *bucketBlocks) labelSets() map[uint64]labels.Labels {
	s.mtx.Lock()
	labelSets := make(map[uint64]labels.Labels, len(s.blocks))
	for _, bs := range s.blocks {
		labelSets[bs.labels.Hash()] = append(labels.Labels(nil), bs.labels...)
	}
	s.mtx.Unlock()

	return labelSets
}

func (s *bucketBlocks) numBlocks() int {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return len(s.blocks)
}

func (s *bucketBlocks) timeRange() (mint, maxt int64) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	mint = math.MaxInt64
	maxt = math.MinInt64

	for _, b := range s.blocks {
		if b.meta.MinTime < mint {
			mint = b.meta.MinTime
		}
		if b.meta.MaxTime > maxt {
			maxt = b.meta.MaxTime
		}
	}

	return mint, maxt
}

func (s *bucketBlocks) indexReaderForAllBlocks(ctx context.Context) []*bucketIndexReader {
	s.mtx.RLock()

	readers := make([]*bucketIndexReader, 0, len(s.blocks))
	for _, b := range s.blocks {
		readers = append(readers, b.indexReader(ctx))
	}

	s.mtx.RUnlock()

	return readers
}

type bucketBlocksWithMatcher struct {
	blocks   []*bucketBlock
	matchers []labels.Matcher
}

func (s *bucketBlocks) getBlocksFor(matchers []labels.Matcher, mint, maxt, maxResolutionWindow int64) []*bucketBlocksWithMatcher {
	output := make([]*bucketBlocksWithMatcher, 0, 0)

	s.mtx.RLock()

	for _, bs := range s.blockSets {
		blockMatchers, ok := bs.labelMatchers(matchers...)
		if !ok {
			continue
		}

		output = append(output, &bucketBlocksWithMatcher{
			matchers: blockMatchers,
			blocks:   bs.getFor(mint, maxt, maxResolutionWindow),
		})

		// TODO
		// if s.debugLogging {
		// 	debugFoundBlockSetOverview(s.logger, req.MinTime, req.MaxTime, req.MaxResolutionWindow, bs.labels, blocks)
		// }
	}

	s.mtx.RUnlock()

	return output
}

func (s *bucketBlocks) close() error {
	var err error

	s.mtx.Lock()
	defer s.mtx.Unlock()

	for _, b := range s.blocks {
		if e := b.Close(); e != nil {
			level.Warn(s.logger).Log("msg", "closing Bucket block failed", "err", e)
			err = e
		}
	}
	return err
}
