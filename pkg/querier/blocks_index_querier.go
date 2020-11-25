package querier

import (
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type BlocksIndexQuerierConfig struct {
	IndexUpdateInterval time.Duration
	IndexIdleTimeout    time.Duration
}

type BlocksIndexQuerier struct {
	services.Service

	manager *bucketindex.ReaderManager

	// Subservices manager.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

func NewBlocksIndexQuerier(cfg BlocksIndexQuerierConfig, bkt objstore.Bucket, logger log.Logger) (*BlocksIndexQuerier, error) {
	f := &BlocksIndexQuerier{
		manager: bucketindex.NewReaderManager(bkt, logger, cfg.IndexUpdateInterval, cfg.IndexIdleTimeout),
	}

	var err error
	f.subservices, err = services.NewManager(f.manager)
	if err != nil {
		return nil, err
	}

	f.Service = services.NewBasicService(f.starting, f.running, f.stopping)

	return f, nil
}

func (q *BlocksIndexQuerier) starting(ctx context.Context) error {
	q.subservicesWatcher.WatchManager(q.subservices)

	if err := services.StartManagerAndAwaitHealthy(ctx, q.subservices); err != nil {
		return errors.Wrap(err, "unable to start blocks index querier subservices")
	}

	return nil
}

func (q *BlocksIndexQuerier) running(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-q.subservicesWatcher.Chan():
			return errors.Wrap(err, "blocks undex querier set subservice failed")
		}
	}
}

func (q *BlocksIndexQuerier) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), q.subservices)
}

func (q *BlocksIndexQuerier) GetBlocks(ctx context.Context, userID string, minT, maxT int64) (bucketindex.Blocks, map[ulid.ULID]*bucketindex.BlockDeletionMark, error) {
	if maxT < minT {
		return nil, nil, errInvalidBlocksRange
	}

	// Get the bucket index for this user.
	idx, err := q.manager.GetIndex(ctx, userID)
	if err != nil {
		return nil, nil, err
	}

	// TODO it's very unoptimised to sort it each time, but we can address it later.
	blocks := append([]*bucketindex.Block{}, idx.Blocks...)
	sortBlockMetasByMaxTime(blocks)

	// Given we do expect the large majority of queries to have a time range close
	// to "now", we're going to find matching blocks iterating the list in reverse order.
	var matchingBlocks bucketindex.Blocks
	matchingIDs := map[ulid.ULID]struct{}{}
	for i := len(blocks) - 1; i >= 0; i-- {
		// NOTE: Block intervals are half-open: [MinTime, MaxTime).
		if blocks[i].MinTime <= maxT && minT < blocks[i].MaxTime {
			matchingBlocks = append(matchingBlocks, blocks[i])
			matchingIDs[blocks[i].ID] = struct{}{}
		}

		// We can safely break the loop because metas are sorted by MaxTime.
		if blocks[i].MaxTime <= minT {
			break
		}
	}

	// Filter deletion marks by matching blocks only.
	matchingDeletionMarks := map[ulid.ULID]*bucketindex.BlockDeletionMark{}
	for _, mark := range idx.BlockDeletionMarks {
		if _, ok := matchingIDs[mark.ID]; ok {
			matchingDeletionMarks[mark.ID] = mark
		}
	}

	return matchingBlocks, matchingDeletionMarks, nil
}
