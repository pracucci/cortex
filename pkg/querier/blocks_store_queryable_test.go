package querier

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/store/hintspb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	promchunk "github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/batch"
	seriesset "github.com/cortexproject/cortex/pkg/querier/series"
	"github.com/cortexproject/cortex/pkg/storegateway/storegatewaypb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestBlocksStoreQuerier_SelectSorted(t *testing.T) {
	const (
		metricName = "test_metric"
		minT       = int64(10)
		maxT       = int64(20)
	)

	var (
		block1          = ulid.MustNew(1, nil)
		block2          = ulid.MustNew(2, nil)
		block3          = ulid.MustNew(3, nil)
		block4          = ulid.MustNew(4, nil)
		metricNameLabel = labels.Label{Name: labels.MetricName, Value: metricName}
		series1Label    = labels.Label{Name: "series", Value: "1"}
		series2Label    = labels.Label{Name: "series", Value: "2"}
	)

	type valueResult struct {
		t int64
		v float64
	}

	type seriesResult struct {
		lbls   labels.Labels
		values []valueResult
	}

	tests := map[string]struct {
		finderResult   []*BlockMeta
		finderErr      error
		storeSetResult map[BlocksStoreClient][]ulid.ULID
		storeSetErr    error
		expectedSeries []seriesResult
		expectedErr    string
	}{
		"no block in the storage matching the query time range": {
			finderResult: nil,
			expectedErr:  "",
		},
		"error while finding blocks matching the query time range": {
			finderErr:   errors.New("unable to find blocks"),
			expectedErr: "unable to find blocks",
		},
		"error while getting clients to query the store-gateway": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
			},
			storeSetErr: errors.New("no client found"),
			expectedErr: "no client found",
		},
		"a single store-gateway instance holds the required blocks (single returned series)": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
			},
			storeSetResult: map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
					mockHintsResponse(block1, block2),
				}}: {block1, block2},
			},
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
		},
		"a single store-gateway instance holds the required blocks (multiple returned series)": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
			},
			storeSetResult: map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
					mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 3),
					mockHintsResponse(block1, block2),
				}}: {block1, block2},
			},
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel, series1Label),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				}, {
					lbls: labels.New(metricNameLabel, series2Label),
					values: []valueResult{
						{t: minT, v: 3},
					},
				},
			},
		},
		"multiple store-gateway instances holds the required blocks without overlapping series (single returned series)": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
			},
			storeSetResult: map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT, 1),
					mockHintsResponse(block1),
				}}: {block1},
				&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
					mockHintsResponse(block2),
				}}: {block2},
			},
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
		},
		"multiple store-gateway instances holds the required blocks with overlapping series (single returned series)": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
			},
			storeSetResult: map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
					mockHintsResponse(block1),
				}}: {block1},
				&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
					mockHintsResponse(block2),
				}}: {block2},
			},
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				},
			},
		},
		"multiple store-gateway instances holds the required blocks with overlapping series (multiple returned series)": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
			},
			storeSetResult: map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
					mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 1),
					mockHintsResponse(block1),
				}}: {block1},
				&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
					mockHintsResponse(block2),
				}}: {block2},
				&storeGatewayClientMock{remoteAddr: "3.3.3.3", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel, series2Label}, minT+1, 3),
					mockHintsResponse(block3),
				}}: {block3},
			},
			expectedSeries: []seriesResult{
				{
					lbls: labels.New(metricNameLabel, series1Label),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 2},
					},
				}, {
					lbls: labels.New(metricNameLabel, series2Label),
					values: []valueResult{
						{t: minT, v: 1},
						{t: minT + 1, v: 3},
					},
				},
			},
		},
		"a single store-gateway instance has some missing blocks (consistency check failed)": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
			},
			storeSetResult: map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT, 1),
					mockSeriesResponse(labels.Labels{metricNameLabel, series1Label}, minT+1, 2),
					mockHintsResponse(block1),
				}}: {block1},
			},
			expectedErr: fmt.Sprintf("consistency check failed because some blocks were not queried: %s", block2.String()),
		},
		"multiple store-gateway instances have some missing blocks (consistency check failed)": {
			finderResult: []*BlockMeta{
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block3}}},
				{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block4}}},
			},
			storeSetResult: map[BlocksStoreClient][]ulid.ULID{
				&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
					mockHintsResponse(block1),
				}}: {block1},
				&storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedResponses: []*storepb.SeriesResponse{
					mockSeriesResponse(labels.Labels{metricNameLabel}, minT+1, 2),
					mockHintsResponse(block2),
				}}: {block2},
			},
			expectedErr: fmt.Sprintf("consistency check failed because some blocks were not queried: %s %s", block3.String(), block4.String()),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			stores := &blocksStoreSetMock{
				mockedResult: testData.storeSetResult,
				mockedErr:    testData.storeSetErr,
			}
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", "user-1", minT, maxT).Return(testData.finderResult, map[ulid.ULID]*metadata.DeletionMark(nil), testData.finderErr)

			q := &blocksStoreQuerier{
				ctx:         ctx,
				minT:        minT,
				maxT:        maxT,
				userID:      "user-1",
				finder:      finder,
				stores:      stores,
				consistency: NewBlocksConsistencyChecker(0, 0, log.NewNopLogger(), nil),
				logger:      log.NewNopLogger(),
				storesHit:   prometheus.NewHistogram(prometheus.HistogramOpts{}),
			}

			matchers := []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName),
			}

			set, warnings, err := q.Select(true, nil, matchers...)
			if testData.expectedErr != "" {
				assert.EqualError(t, err, testData.expectedErr)
				assert.Nil(t, set)
				assert.Nil(t, warnings)
				return
			}

			require.NoError(t, err)
			assert.Len(t, warnings, 0)

			// Read all returned series and their values.
			var actualSeries []seriesResult
			for set.Next() {
				var actualValues []valueResult

				it := set.At().Iterator()
				for it.Next() {
					t, v := it.At()
					actualValues = append(actualValues, valueResult{
						t: t,
						v: v,
					})
				}

				require.NoError(t, it.Err())

				actualSeries = append(actualSeries, seriesResult{
					lbls:   set.At().Labels(),
					values: actualValues,
				})
			}
			require.NoError(t, set.Err())
			assert.Equal(t, testData.expectedSeries, actualSeries)
		})
	}
}

func TestBlocksStoreQuerier_SelectSortedShouldHonorQueryStoreAfter(t *testing.T) {
	now := time.Now()

	tests := map[string]struct {
		queryStoreAfter time.Duration
		queryMinT       int64
		queryMaxT       int64
		expectedMinT    int64
		expectedMaxT    int64
	}{
		"should not manipulate query time range if queryStoreAfter is disabled": {
			queryStoreAfter: 0,
			queryMinT:       util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:       util.TimeToMillis(now.Add(-30 * time.Minute)),
			expectedMinT:    util.TimeToMillis(now.Add(-100 * time.Minute)),
			expectedMaxT:    util.TimeToMillis(now.Add(-30 * time.Minute)),
		},
		"should not manipulate query time range if queryStoreAfter is enabled but query max time is older": {
			queryStoreAfter: time.Hour,
			queryMinT:       util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:       util.TimeToMillis(now.Add(-70 * time.Minute)),
			expectedMinT:    util.TimeToMillis(now.Add(-100 * time.Minute)),
			expectedMaxT:    util.TimeToMillis(now.Add(-70 * time.Minute)),
		},
		"should manipulate query time range if queryStoreAfter is enabled and query max time is recent": {
			queryStoreAfter: time.Hour,
			queryMinT:       util.TimeToMillis(now.Add(-100 * time.Minute)),
			queryMaxT:       util.TimeToMillis(now.Add(-30 * time.Minute)),
			expectedMinT:    util.TimeToMillis(now.Add(-100 * time.Minute)),
			expectedMaxT:    util.TimeToMillis(now.Add(-60 * time.Minute)),
		},
		"should skip the query if the query min time is more recent than queryStoreAfter": {
			queryStoreAfter: time.Hour,
			queryMinT:       util.TimeToMillis(now.Add(-50 * time.Minute)),
			queryMaxT:       util.TimeToMillis(now.Add(-20 * time.Minute)),
			expectedMinT:    0,
			expectedMaxT:    0,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", "user-1", mock.Anything, mock.Anything).Return([]*BlockMeta(nil), map[ulid.ULID]*metadata.DeletionMark(nil), error(nil))

			q := &blocksStoreQuerier{
				ctx:             context.Background(),
				minT:            testData.queryMinT,
				maxT:            testData.queryMaxT,
				userID:          "user-1",
				finder:          finder,
				stores:          &blocksStoreSetMock{},
				consistency:     NewBlocksConsistencyChecker(0, 0, log.NewNopLogger(), nil),
				logger:          log.NewNopLogger(),
				storesHit:       prometheus.NewHistogram(prometheus.HistogramOpts{}),
				queryStoreAfter: testData.queryStoreAfter,
			}

			sp := &storage.SelectHints{
				Start: testData.queryMinT,
				End:   testData.queryMaxT,
			}

			_, _, err := q.selectSorted(sp, nil)
			require.NoError(t, err)

			if testData.expectedMinT == 0 && testData.expectedMaxT == 0 {
				assert.Len(t, finder.Calls, 0)
			} else {
				require.Len(t, finder.Calls, 1)
				assert.Equal(t, testData.expectedMinT, finder.Calls[0].Arguments.Get(1))
				assert.InDelta(t, testData.expectedMaxT, finder.Calls[0].Arguments.Get(2), float64(5*time.Second.Milliseconds()))
			}
		})
	}
}

func TestBlocksStoreQuerier_PromQLExecution(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	series1 := []storepb.Label{{Name: "__name__", Value: "metric_1"}}
	series2 := []storepb.Label{{Name: "__name__", Value: "metric_2"}}

	series1Samples := []promql.Point{
		{T: 1589759955000, V: 1},
		{T: 1589759970000, V: 1},
		{T: 1589759985000, V: 1},
		{T: 1589760000000, V: 1},
		{T: 1589760015000, V: 1},
		{T: 1589760030000, V: 1},
	}

	series2Samples := []promql.Point{
		{T: 1589759955000, V: 2},
		{T: 1589759970000, V: 2},
		{T: 1589759985000, V: 2},
		{T: 1589760000000, V: 2},
		{T: 1589760015000, V: 2},
		{T: 1589760030000, V: 2},
	}

	// Mock the finder to simulate we need to query two blocks.
	finder := &blocksFinderMock{
		Service: services.NewIdleService(nil, nil),
	}
	finder.On("GetBlocks", "user-1", mock.Anything, mock.Anything).Return([]*BlockMeta{
		{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block1}}},
		{Meta: metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: block2}}},
	}, map[ulid.ULID]*metadata.DeletionMark(nil), error(nil))

	// Mock the store to simulate each block is queried from a different store-gateway.
	gateway1 := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedResponses: []*storepb.SeriesResponse{
		{
			Result: &storepb.SeriesResponse_Series{
				Series: &storepb.Series{
					Labels: series1,
					Chunks: []storepb.AggrChunk{
						createAggrChunkWithSamples(series1Samples[:3]...), // First half.
					},
				},
			},
		}, {
			Result: &storepb.SeriesResponse_Series{
				Series: &storepb.Series{
					Labels: series2,
					Chunks: []storepb.AggrChunk{
						createAggrChunkWithSamples(series2Samples[:3]...),
					},
				},
			},
		},
		mockHintsResponse(block1),
	}}

	gateway2 := &storeGatewayClientMock{remoteAddr: "2.2.2.2", mockedResponses: []*storepb.SeriesResponse{
		{
			Result: &storepb.SeriesResponse_Series{
				Series: &storepb.Series{
					Labels: series1,
					Chunks: []storepb.AggrChunk{
						createAggrChunkWithSamples(series1Samples[3:]...), // Second half.
					},
				},
			},
		}, {
			Result: &storepb.SeriesResponse_Series{
				Series: &storepb.Series{
					Labels: series2,
					Chunks: []storepb.AggrChunk{
						createAggrChunkWithSamples(series2Samples[3:]...),
					},
				},
			},
		},
		mockHintsResponse(block2),
	}}

	stores := &blocksStoreSetMock{
		Service: services.NewIdleService(nil, nil),
		mockedResult: map[BlocksStoreClient][]ulid.ULID{
			gateway1: {block1},
			gateway2: {block2},
		},
	}

	// Instance the querier that will be executed to run the query.
	logger := log.NewNopLogger()
	queryable, err := NewBlocksStoreQueryable(stores, finder, NewBlocksConsistencyChecker(0, 0, logger, nil), 0, logger, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), queryable))
	defer services.StopAndAwaitTerminated(context.Background(), queryable) // nolint:errcheck

	engine := promql.NewEngine(promql.EngineOpts{
		Logger:     logger,
		Timeout:    10 * time.Second,
		MaxSamples: 1e6,
	})

	// Run a query.
	q, err := engine.NewRangeQuery(queryable, `{__name__=~"metric.*"}`, time.Unix(1589759955, 0), time.Unix(1589760030, 0), 15*time.Second)
	require.NoError(t, err)

	ctx := user.InjectOrgID(context.Background(), "user-1")
	res := q.Exec(ctx)
	require.NoError(t, err)
	require.NoError(t, res.Err)

	matrix, err := res.Matrix()
	require.NoError(t, err)
	require.Len(t, matrix, 2)

	assert.Equal(t, storepb.LabelsToPromLabels(series1), matrix[0].Metric)
	assert.Equal(t, storepb.LabelsToPromLabels(series2), matrix[1].Metric)
	assert.Equal(t, series1Samples, matrix[0].Points)
	assert.Equal(t, series2Samples, matrix[1].Points)
}

type blocksStoreSetMock struct {
	services.Service

	mockedResult map[BlocksStoreClient][]ulid.ULID
	mockedErr    error
}

func (m *blocksStoreSetMock) GetClientsFor(_ []ulid.ULID) (map[BlocksStoreClient][]ulid.ULID, error) {
	return m.mockedResult, m.mockedErr
}

type blocksFinderMock struct {
	services.Service
	mock.Mock
}

func (m *blocksFinderMock) GetBlocks(userID string, minT, maxT int64) ([]*BlockMeta, map[ulid.ULID]*metadata.DeletionMark, error) {
	args := m.Called(userID, minT, maxT)
	return args.Get(0).([]*BlockMeta), args.Get(1).(map[ulid.ULID]*metadata.DeletionMark), args.Error(2)
}

type storeGatewayClientMock struct {
	remoteAddr      string
	mockedResponses []*storepb.SeriesResponse
}

func (m *storeGatewayClientMock) Series(ctx context.Context, in *storepb.SeriesRequest, opts ...grpc.CallOption) (storegatewaypb.StoreGateway_SeriesClient, error) {
	seriesClient := &storeGatewaySeriesClientMock{
		mockedResponses: m.mockedResponses,
	}

	return seriesClient, nil
}

func (m *storeGatewayClientMock) RemoteAddress() string {
	return m.remoteAddr
}

type storeGatewaySeriesClientMock struct {
	grpc.ClientStream

	mockedResponses []*storepb.SeriesResponse
}

func (m *storeGatewaySeriesClientMock) Recv() (*storepb.SeriesResponse, error) {
	// Ensure some concurrency occurs.
	time.Sleep(10 * time.Millisecond)

	if len(m.mockedResponses) == 0 {
		return nil, io.EOF
	}

	res := m.mockedResponses[0]
	m.mockedResponses = m.mockedResponses[1:]
	return res, nil
}

func mockSeriesResponse(lbls labels.Labels, timeMillis int64, value float64) *storepb.SeriesResponse {
	// Generate a chunk containing a single value (for simplicity).
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	if err != nil {
		panic(err)
	}
	appender.Append(timeMillis, value)
	chunkData := chunk.Bytes()

	return &storepb.SeriesResponse{
		Result: &storepb.SeriesResponse_Series{
			Series: &storepb.Series{
				Labels: storepb.PromLabelsToLabels(lbls),
				Chunks: []storepb.AggrChunk{
					{MinTime: timeMillis, MaxTime: timeMillis, Raw: &storepb.Chunk{Type: storepb.Chunk_XOR, Data: chunkData}},
				},
			},
		},
	}
}

func mockHintsResponse(ids ...ulid.ULID) *storepb.SeriesResponse {
	hints := &hintspb.SeriesResponseHints{}
	for _, id := range ids {
		hints.AddQueriedBlock(id)
	}

	any, err := types.MarshalAny(hints)
	if err != nil {
		panic(err)
	}

	return &storepb.SeriesResponse{
		Result: &storepb.SeriesResponse_Hints{
			Hints: any,
		},
	}
}

func mkAggrChunk(minT, maxT, step int64) storepb.AggrChunk {
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	if err != nil {
		panic(err)
	}

	for ts := minT; ts < maxT; ts += step {
		appender.Append(ts, float64(ts))
	}

	return storepb.AggrChunk{
		MinTime: minT,
		MaxTime: maxT,
		Raw:     &storepb.Chunk{Type: storepb.Chunk_XOR, Data: chunk.Bytes()},
	}
}

func BenchmarkMergeSeriesSet_Iterator(b *testing.B) {
	const (
		numStoreGateways         = 10 // Each store gateway has the same series, so this is also the duplication factor.
		numSeriesPerStoreGateway = 2000
		numChunksPerSeries       = 10
		numSamplesPerChunk       = 120
	)

	seriesSets := []storage.SeriesSet(nil)
	for g := 0; g < numStoreGateways; g++ {
		mySeries := []*storepb.Series(nil)

		for s := 0; s < numSeriesPerStoreGateway; s++ {
			lbl := mkLabels("series_id", strconv.Itoa(s))
			chunks := []storepb.AggrChunk(nil)

			for c := 0; c < numChunksPerSeries; c++ {
				chunks = append(chunks, mkAggrChunk(int64(c*numSamplesPerChunk), int64((c*numSamplesPerChunk)+numSamplesPerChunk), 1))
			}

			mySeries = append(mySeries, &storepb.Series{
				Labels: lbl,
				Chunks: chunks,
			})
		}

		seriesSets = append(seriesSets, &blockQuerierSeriesSet{series: mySeries})
	}
	merged := storage.NewMergeSeriesSet(seriesSets, storage.ChainedSeriesMerge)

	b.ResetTimer()

	for merged.Next() {
		for it := merged.At().Iterator(); it.Next(); {
			it.At()
		}
	}
}

func BenchmarkMergeSeriesSet_IteratorOptimized(b *testing.B) {
	const (
		numStoreGateways         = 10 // Each store gateway has the same series, so this is also the duplication factor.
		numSeriesPerStoreGateway = 2000
		numChunksPerSeries       = 10
		numSamplesPerChunk       = 120
	)

	// TODO implement it in a hash collision resistant
	chunksBySeries := map[model.Fingerprint]*aggrChunkSeries{}

	for g := 0; g < numStoreGateways; g++ {

		for s := 0; s < numSeriesPerStoreGateway; s++ {
			lbl := mkLabels("series_id", strconv.Itoa(s))

			for c := 0; c < numChunksPerSeries; c++ {
				ck := mkAggrChunk(int64(c*numSamplesPerChunk), int64((c*numSamplesPerChunk)+numSamplesPerChunk), 1)

				adapted, err := newChunkAdapter(ck)
				if err != nil {
					b.Fatal(err)
				}

				fp := client.Fingerprint(storepb.LabelsToPromLabelsUnsafe(lbl))
				if cbs, ok := chunksBySeries[fp]; ok {
					cbs.chunks = append(cbs.chunks, adapted)
				} else {
					chunksBySeries[fp] = &aggrChunkSeries{
						labels: storepb.LabelsToPromLabelsUnsafe(lbl),
						chunks: []batch.Chunk{adapted},
					}
				}

				// Convert chunks
				/*
					convertedChunks = append(convertedChunks, chunk.Chunk{
						Fingerprint: client.Fingerprint(storepb.LabelsToPromLabelsUnsafe(lbl)),
						From:        model.Time(ck.MinTime),
						Through:     model.Time(ck.MaxTime),
						Metric:      storepb.LabelsToPromLabelsUnsafe(lbl),
						Encoding:    0, // this shouldn't be used
						Data:        newChunkAdapter(ck.Raw),
					})
				*/
			}
		}

	}

	b.ResetTimer()

	//merged := partitionChunks(convertedChunks, 0, math.MaxInt64, batch.NewChunkMergeIterator)

	series := make([]storage.Series, 0, len(chunksBySeries))
	for _, cbs := range chunksBySeries {
		series = append(series, cbs)
	}

	merged := seriesset.NewConcreteSeriesSet(series)

	numSamples := 0
	for merged.Next() {
		for it := merged.At().Iterator(); it.Next(); {
			it.At()
			numSamples++
		}
	}

	expectedSamples := numSeriesPerStoreGateway * numChunksPerSeries * numSamplesPerChunk
	if numSamples != expectedSamples {
		b.Fatalf("unexpected number of samples, got %d expected %d", numSamplesPerChunk, expectedSamples)
	}
}

type aggrChunkSeries struct {
	labels labels.Labels
	chunks []batch.Chunk
}

func (s *aggrChunkSeries) Labels() labels.Labels {
	return s.labels
}

func (s *aggrChunkSeries) Iterator() chunkenc.Iterator {
	return batch.NewGenericChunkMergeIterator(s.chunks)
}

type chunkAdapter struct {
	minTime int64
	maxTime int64
	chunk   chunkenc.Chunk
}

func newChunkAdapter(chunkAggr storepb.AggrChunk) (chunkAdapter, error) {
	// TODO the encoding type must be based on AggrChunk's encoding
	chunk, err := chunkenc.FromData(chunkenc.EncXOR, chunkAggr.Raw.Data)
	if err != nil {
		return chunkAdapter{}, err
	}

	return chunkAdapter{
		minTime: chunkAggr.MinTime,
		maxTime: chunkAggr.MaxTime,
		chunk:   chunk,
	}, nil
}

func (a chunkAdapter) MinTime() int64 {
	return a.minTime
}

func (a chunkAdapter) MaxTime() int64 {
	return a.maxTime
}

func (a chunkAdapter) Iterator(reuse encoding.Iterator) encoding.Iterator {
	// TODO add support to reuse the iterator
	return newIteratorAdapter(a.chunk.Iterator(nil))
}

/*
type chunkAdapter struct {
	ck chunkenc.Chunk
}

func newChunkAdapter(ck *storepb.Chunk) chunkAdapter {
	rightType, err := chunkenc.FromData(chunkenc.EncXOR, ck.Data)
	if err != nil {
		panic(err)
	}

	return chunkAdapter{
		ck: rightType,
	}
}

func (a chunkAdapter) Add(sample model.SamplePair) (promchunk.Chunk, error) {
	return nil, errors.New("unsupported")
}

func (a chunkAdapter) NewIterator(it promchunk.Iterator) promchunk.Iterator {
	return newIteratorAdapter(a.ck.Iterator(nil))
}

func (a chunkAdapter) Marshal(io.Writer) error {
	return errors.New("unsupported")
}
func (a chunkAdapter) UnmarshalFromBuf([]byte) error {
	return errors.New("unsupported")
}
func (a chunkAdapter) Encoding() promchunk.Encoding {
	panic(errors.New("unsupported"))
}
func (a chunkAdapter) Utilization() float64 {
	panic(errors.New("unsupported"))
}
func (a chunkAdapter) Slice(start, end model.Time) promchunk.Chunk {
	panic(errors.New("unsupported"))
}
func (a chunkAdapter) Len() int {
	panic(errors.New("unsupported"))
}
func (a chunkAdapter) Size() int {
	panic(errors.New("unsupported"))
}
*/

type iteratorAdapter struct {
	it chunkenc.Iterator
}

func newIteratorAdapter(it chunkenc.Iterator) iteratorAdapter {
	return iteratorAdapter{
		it: it,
	}
}

// Scans the next value in the chunk. Directly after the iterator has
// been created, the next value is the first value in the
// chunk. Otherwise, it is the value following the last value scanned or
// found (by one of the Find... methods). Returns false if either the
// end of the chunk is reached or an error has occurred.
func (a iteratorAdapter) Scan() bool {
	return a.it.Next()
}

// Finds the oldest value at or after the provided time. Returns false
// if either the chunk contains no value at or after the provided time,
// or an error has occurred.
func (a iteratorAdapter) FindAtOrAfter(t model.Time) bool {
	return a.it.Seek(int64(t))
}

// Returns the last value scanned (by the scan method) or found (by one
// of the find... methods). It returns model.ZeroSamplePair before any of
// those methods were called.
func (a iteratorAdapter) Value() model.SamplePair {
	t, v := a.it.At()
	return model.SamplePair{
		Timestamp: model.Time(t),
		Value:     model.SampleValue(v),
	}
}

// Returns a batch of the provisded size; NB not idempotent!  Should only be called
// once per Scan.
func (a iteratorAdapter) Batch(size int) promchunk.Batch {
	var batch promchunk.Batch
	j := 0
	for j < size {
		currTs, currValue := a.it.At()
		batch.Timestamps[j] = currTs
		batch.Values[j] = currValue
		j++
		if j < size && !a.it.Next() {
			break
		}
	}
	batch.Index = 0
	batch.Length = j
	return batch
}

// Returns the last error encountered. In general, an error signals data
// corruption in the chunk and requires quarantining.
func (a iteratorAdapter) Err() error {
	return a.it.Err()
}
