package storegateway

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/logging"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/storage/backend/filesystem"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestStoreGateway_InitialSyncWithShardingEnabled(t *testing.T) {
	tests := map[string]struct {
		initialExists bool
		initialState  ring.IngesterState
		initialTokens ring.Tokens
	}{
		"instance not in the ring": {
			initialExists: false,
		},
		"instance already in the ring with PENDING state and has no tokens": {
			initialExists: true,
			initialState:  ring.PENDING,
			initialTokens: ring.Tokens{},
		},
		"instance already in the ring with JOINING state and has some tokens": {
			initialExists: true,
			initialState:  ring.JOINING,
			initialTokens: ring.Tokens{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		"instance already in the ring with ACTIVE state and has all tokens": {
			initialExists: true,
			initialState:  ring.ACTIVE,
			initialTokens: ring.Tokens(ring.GenerateTokens(RingNumTokens, ring.Tokens{})),
		},
		"instance already in the ring with LEAVING state and has all tokens": {
			initialExists: true,
			initialState:  ring.LEAVING,
			initialTokens: ring.Tokens(ring.GenerateTokens(RingNumTokens, ring.Tokens{})),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			gatewayCfg := mockGatewayConfig()
			gatewayCfg.ShardingEnabled = true
			storageCfg, cleanup := mockStorageConfig(t)
			defer cleanup()
			ringStore := consul.NewInMemoryClient(ring.GetCodec())
			bucketClient := &cortex_tsdb.BucketClientMock{}

			// Setup the initial instance state in the ring.
			if testData.initialExists {
				require.NoError(t, ringStore.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
					ringDesc := ring.GetOrCreateRingDesc(in)
					ringDesc.AddIngester(gatewayCfg.ShardingRing.InstanceID, gatewayCfg.ShardingRing.InstanceAddr, "", testData.initialTokens, testData.initialState)
					return ringDesc, true, nil
				}))
			}

			g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, mockLoggingLevel(), log.NewNopLogger(), nil)
			require.NoError(t, err)
			defer services.StopAndAwaitTerminated(ctx, g) //nolint:errcheck
			assert.False(t, g.ringLifecycler.IsRegistered())

			bucketClient.MockIterWithCallback("", []string{"user-1", "user-2"}, nil, func() {
				// During the initial sync, we expect the instance to always be in the JOINING
				// state within the ring.
				assert.True(t, g.ringLifecycler.IsRegistered())
				assert.Equal(t, ring.JOINING, g.ringLifecycler.GetState())
				assert.Equal(t, RingNumTokens, len(g.ringLifecycler.GetTokens()))
				assert.Subset(t, g.ringLifecycler.GetTokens(), testData.initialTokens)
			})
			bucketClient.MockIter("user-1/", []string{}, nil)
			bucketClient.MockIter("user-2/", []string{}, nil)

			// Once successfully started, the instance should be ACTIVE in the ring.
			require.NoError(t, g.StartAsync(ctx))
			require.NoError(t, g.AwaitRunning(ctx))

			assert.True(t, g.ringLifecycler.IsRegistered())
			assert.Equal(t, ring.ACTIVE, g.ringLifecycler.GetState())
			assert.Equal(t, RingNumTokens, len(g.ringLifecycler.GetTokens()))
			assert.Subset(t, g.ringLifecycler.GetTokens(), testData.initialTokens)

			assert.True(t, g.stores.HasUser("user-1"))
			assert.True(t, g.stores.HasUser("user-2"))
			assert.False(t, g.stores.HasUser("user-unknown"))
		})
	}
}

func TestStoreGateway_InitialSyncWithShardingDisabled(t *testing.T) {
	ctx := context.Background()
	gatewayCfg := mockGatewayConfig()
	gatewayCfg.ShardingEnabled = false
	storageCfg, cleanup := mockStorageConfig(t)
	defer cleanup()
	bucketClient := &cortex_tsdb.BucketClientMock{}

	g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, nil, mockLoggingLevel(), log.NewNopLogger(), nil)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, g) //nolint:errcheck

	bucketClient.MockIter("", []string{"user-1", "user-2"}, nil)
	bucketClient.MockIter("user-1/", []string{}, nil)
	bucketClient.MockIter("user-2/", []string{}, nil)

	require.NoError(t, g.StartAsync(ctx))
	require.NoError(t, g.AwaitRunning(ctx))

	assert.True(t, g.stores.HasUser("user-1"))
	assert.True(t, g.stores.HasUser("user-2"))
	assert.False(t, g.stores.HasUser("user-unknown"))
}

func TestStoreGateway_InitialSyncFailure(t *testing.T) {
	ctx := context.Background()
	gatewayCfg := mockGatewayConfig()
	gatewayCfg.ShardingEnabled = true
	storageCfg, cleanup := mockStorageConfig(t)
	defer cleanup()
	ringStore := consul.NewInMemoryClient(ring.GetCodec())
	bucketClient := &cortex_tsdb.BucketClientMock{}

	g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, mockLoggingLevel(), log.NewNopLogger(), nil)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, g) //nolint:errcheck

	bucketClient.MockIter("", []string{}, errors.New("network error"))

	require.NoError(t, g.StartAsync(ctx))
	require.Error(t, g.AwaitRunning(ctx))
	assert.Equal(t, services.Failed, g.State())

	// We expect a clean shutdown, including unregistering the instance from the ring.
	assert.False(t, g.ringLifecycler.IsRegistered())
}

func TestStoreGateway_BlocksSharding(t *testing.T) {
	storageDir, err := ioutil.TempDir(os.TempDir(), "")
	require.NoError(t, err)
	defer os.RemoveAll(storageDir) //nolint:errcheck

	// This tests uses real TSDB blocks. 24h time range, 2h block range period,
	// 2 users = total (24 / 12) * 2 = 24 blocks.
	numBlocks := 24
	now := time.Now()
	require.NoError(t, mockTSDB(path.Join(storageDir, "user-1"), 24, now.Add(-24*time.Hour).Unix()*1000, now.Unix()*1000))
	require.NoError(t, mockTSDB(path.Join(storageDir, "user-2"), 24, now.Add(-24*time.Hour).Unix()*1000, now.Unix()*1000))

	bucketClient, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	tests := map[string]struct {
		shardingEnabled      bool
		numGateways          int
		expectedBlocksLoaded int
	}{
		"1 gateway, sharding disabled": {
			shardingEnabled:      false,
			numGateways:          1,
			expectedBlocksLoaded: numBlocks,
		},
		"2 gateways, sharding disabled": {
			shardingEnabled:      false,
			numGateways:          2,
			expectedBlocksLoaded: 2 * numBlocks, // each gateway loads all the blocks
		},
		"1 gateway, sharding enabled": {
			shardingEnabled:      true,
			numGateways:          1,
			expectedBlocksLoaded: numBlocks,
		},
		"2 gateways, sharding enabled": {
			shardingEnabled:      true,
			numGateways:          2,
			expectedBlocksLoaded: numBlocks, // blocks are sharded across gateways
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			storageCfg, cleanup := mockStorageConfig(t)
			defer cleanup()
			ringStore := consul.NewInMemoryClient(ring.GetCodec())

			// Start the configure number of gateways.
			var gateways []*StoreGateway
			registries := map[string]*prometheus.Registry{}

			for i := 1; i <= testData.numGateways; i++ {
				instanceID := fmt.Sprintf("gateway-%d", i)

				gatewayCfg := mockGatewayConfig()
				gatewayCfg.ShardingEnabled = testData.shardingEnabled
				gatewayCfg.ShardingRing.InstanceID = instanceID
				gatewayCfg.ShardingRing.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)

				reg := prometheus.NewPedanticRegistry()
				g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, mockLoggingLevel(), log.NewNopLogger(), reg)
				require.NoError(t, err)
				defer services.StopAndAwaitTerminated(ctx, g) //nolint:errcheck

				require.NoError(t, g.StartAsync(ctx))
				require.NoError(t, g.AwaitRunning(ctx))

				gateways = append(gateways, g)
				registries[instanceID] = reg
			}

			// Re-sync the stores because the ring topology has changed in the meanwhile
			// (when the 1st gateway has synched the 2nd gateway didn't run yet).
			for _, g := range gateways {
				g.syncStores(ctx, syncReasonRingChange)
			}

			// Assert on the number of blocks loaded extracting this information from metrics.
			metrics := util.BuildMetricFamiliesPerUserFromUserRegistries(registries)
			assert.Equal(t, float64(testData.expectedBlocksLoaded), metrics.GetSumOfGauges("cortex_querier_bucket_store_blocks_loaded"))
		})
	}
}

func mockGatewayConfig() Config {
	cfg := Config{}
	flagext.DefaultValues(&cfg)

	cfg.ShardingRing.InstanceID = "test"
	cfg.ShardingRing.InstanceAddr = "127.0.0.1"

	return cfg
}

func mockStorageConfig(t *testing.T) (cortex_tsdb.Config, func()) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "store-gateway-test-*")
	require.NoError(t, err)

	cfg := cortex_tsdb.Config{}
	flagext.DefaultValues(&cfg)

	cfg.BucketStore.ConsistencyDelay = 0
	cfg.BucketStore.SyncDir = tmpDir

	cleanup := func() {
		require.NoError(t, os.RemoveAll(tmpDir))
	}

	return cfg, cleanup
}

func mockLoggingLevel() logging.Level {
	level := logging.Level{}
	err := level.Set("info")
	if err != nil {
		panic(err)
	}

	return level
}

// mockTSDB create 1+ TSDB blocks storing numSeries of series with
// timestamp evenly distributed between minT and maxT.
func mockTSDB(dir string, numSeries int, minT, maxT int64) error {
	// Create a new TSDB on a temporary directory. The blocks
	// will be then snapshotted to the input dir.
	tempDir, err := ioutil.TempDir(os.TempDir(), "tsdb")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	db, err := tsdb.Open(tempDir, nil, nil, &tsdb.Options{
		BlockRanges:       []int64{int64(2 * 60 * 60 * 1000)}, // 2h period
		RetentionDuration: uint64(15 * 86400 * 1000),          // 15 days
	})
	if err != nil {
		return err
	}

	db.DisableCompactions()

	step := (maxT - minT) / int64(numSeries)
	for i := 0; i < numSeries; i++ {
		lbls := labels.Labels{labels.Label{Name: "series_id", Value: strconv.Itoa(i)}}

		app := db.Appender()
		app.Add(lbls, minT+(step*int64(i)), float64(i))
		app.Commit()

		db.Compact()
	}

	db.Snapshot(dir, true)

	return db.Close()
}
