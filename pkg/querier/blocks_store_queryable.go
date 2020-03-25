package querier

import (
	"context"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc/metadata"

	"github.com/cortexproject/cortex/pkg/querier/series"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

// BlocksStoreQueryable is a queryable which queries blocks storage via
// the store-gateway.
type BlocksStoreQueryable struct {
	services.Service

	stores  BlocksStoreSet
	scanner *BlocksScanner

	// Subservices manager.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

func NewBlocksStoreQueryable(stores BlocksStoreSet, scanner *BlocksScanner) (*BlocksStoreQueryable, error) {
	util.WarnExperimentalUse("Blocks storage engine")

	manager, err := services.NewManager(scanner)
	if err != nil {
		return nil, errors.Wrap(err, "register blocks storage queryable subservices")
	}

	q := &BlocksStoreQueryable{
		stores:             stores,
		scanner:            scanner,
		subservices:        manager,
		subservicesWatcher: services.NewFailureWatcher(),
	}

	q.Service = services.NewBasicService(q.starting, q.running, q.stopping)

	return q, nil
}

func (q *BlocksStoreQueryable) starting(ctx context.Context) error {
	q.subservicesWatcher.WatchManager(q.subservices)

	if err := services.StartManagerAndAwaitHealthy(ctx, q.subservices); err != nil {
		return errors.Wrap(err, "unable to start blocks storage queryable subservices")
	}

	return nil
}

func (q *BlocksStoreQueryable) running(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-q.subservicesWatcher.Chan():
			return errors.Wrap(err, "block storage queryable subservice failed")
		}
	}
}

func (q *BlocksStoreQueryable) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), q.subservices)
}

// Querier returns a new Querier on the storage.
func (q *BlocksStoreQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	if s := q.State(); s != services.Running {
		return nil, promql.ErrStorage{Err: errors.Errorf("BlocksStoreQueryable is not running: %v", s)}
	}

	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, promql.ErrStorage{Err: err}
	}

	return &blocksStoreQuerier{
		ctx:     ctx,
		minT:    mint,
		maxT:    maxt,
		userID:  userID,
		scanner: q.scanner,
		stores:  q.stores,
	}, nil
}

type blocksStoreQuerier struct {
	ctx        context.Context
	minT, maxT int64
	userID     string
	scanner    *BlocksScanner
	stores     BlocksStoreSet
}

func (q *blocksStoreQuerier) Select(sp *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	// TODO may we benefit by non a sorted implementation? What Thanos does?
	return q.SelectSorted(sp, matchers...)
}

// TODO it's too easy to forget to wrap the error with promql.ErrStorage. We may move the logic in a function and wrap it outside.
func (q *blocksStoreQuerier) SelectSorted(sp *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	// TODO DEBUG
	fmt.Println("blocksStoreQuerier.SelectSorted()")

	log, _ := spanlogger.New(q.ctx, "blocksStoreQuerier.SelectSorted")
	defer log.Span.Finish()

	minT, maxT := q.minT, q.maxT
	if sp != nil {
		minT, maxT = sp.Start, sp.End
	}

	// Find the list of blocks we need to query given the time range.
	metas, err := q.scanner.GetBlocks(q.userID, minT, maxT)
	if err != nil {
		return nil, nil, promql.ErrStorage{Err: err}
	}

	if len(metas) == 0 {
		return series.NewEmptySeriesSet(), nil, nil
	}

	// Find the set of store-gateway instances having the blocks.
	clients, err := q.stores.GetClientsFor(metas)
	if err != nil {
		return nil, nil, promql.ErrStorage{Err: err}
	}

	// TODO DEBUG
	fmt.Println("blocksStoreQuerier.SelectSorted() got clients:", clients)

	req := &storepb.SeriesRequest{
		MinTime:                 minT,
		MaxTime:                 maxT,
		Matchers:                convertMatchersToLabelMatcher(matchers),
		PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
	}

	reqCtx := metadata.AppendToOutgoingContext(q.ctx, cortex_tsdb.TenantIDExternalLabel, q.userID)

	var seriesSets []storage.SeriesSet
	warnings := storage.Warnings(nil)

	for _, c := range clients {
		// TODO DEBUG
		fmt.Println("blocksStoreQuerier.SelectSorted() sending req to client:", c)

		// TODO parallelly fetch the series from clients
		seriesClient, err := c.Series(reqCtx, req)
		if err != nil {
			return nil, nil, promql.ErrStorage{Err: err}
		}

		series := []*storepb.Series(nil)

		for {
			resp, err := seriesClient.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, nil, promql.ErrStorage{Err: err}
			}

			// response may either contain series or warning. If it's warning, we get nil here.
			s := resp.GetSeries()
			if s != nil {
				series = append(series, s)
			}

			// collect and return warnings too
			w := resp.GetWarning()
			if w != "" {
				warnings = append(warnings, errors.New(w))
			}
		}

		seriesSets = append(seriesSets, &blockQuerierSeriesSet{series: series})

		// TODO DEBUG
		fmt.Println("client:", c, "num series:", len(series), "warnings:", warnings)
	}

	// TODO is this correct?
	return storage.NewMergeSeriesSet(seriesSets, nil), warnings, nil
}

func (q *blocksStoreQuerier) LabelValues(name string) ([]string, storage.Warnings, error) {
	// Cortex doesn't use this. It will ask ingesters for metadata.
	return nil, nil, errors.New("not implemented")
}

func (q *blocksStoreQuerier) LabelNames() ([]string, storage.Warnings, error) {
	// Cortex doesn't use this. It will ask ingesters for metadata.
	return nil, nil, errors.New("not implemented")
}

func (q *blocksStoreQuerier) Close() error {
	return nil
}
