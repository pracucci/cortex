package querier

import (
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/client"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storegateway/storegatewaypb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/grpcclient"
)

type BlocksStoreSet interface {
	// GetClientsFor returns the store gateway clients that should be used to
	// query the set of blocks in input.
	GetClientsFor(metas []*metadata.Meta) ([]storegatewaypb.StoreGatewayClient, error)
}

type blocksShardedStoreSet struct {
	storesRing   *ring.Ring
	clientsPool  *client.Pool
	clientsCount prometheus.Gauge
}

func NewBlocksShardedStoreSet(storesRing *ring.Ring, logger log.Logger, reg prometheus.Registerer) *blocksShardedStoreSet {
	// We prefer sane defaults instead of exposing further config options.
	clientsPoolCfg := client.PoolConfig{
		CheckInterval:      time.Minute,
		HealthCheckEnabled: true,
		HealthCheckTimeout: 10 * time.Second,
	}

	clientsGrpcCfg := grpcclient.Config{
		MaxRecvMsgSize:      100 << 20,
		MaxSendMsgSize:      16 << 20,
		UseGzipCompression:  false,
		RateLimit:           0,
		RateLimitBurst:      0,
		BackoffOnRatelimits: false,
	}

	s := &blocksShardedStoreSet{
		storesRing: storesRing,

		clientsCount: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "querier_storegateway_clients",
			Help:      "The current number of store-gateway clients.",
		}),
	}

	// TODO pool is a service, so this needs to be a service as well which manages pool
	s.clientsPool = client.NewPool("store-gateway", clientsPoolCfg, storesRing, storegatewaypb.NewStoreGatewayClientFactory(clientsGrpcCfg), s.clientsCount, logger)

	return s
}

func (s *blocksShardedStoreSet) GetClientsFor(metas []*metadata.Meta) ([]storegatewaypb.StoreGatewayClient, error) {
	var instances []string
	tokens := s.storesRing.GetOwnedTokens(ring.BlocksQuery)

	for _, m := range metas {
		owner, err := tokens.GetOwner(cortex_tsdb.HashBlockID(m.ULID))
		if err != nil {
			return nil, errors.Wrapf(err, "get store-gateway owning the block %s", m.ULID.String())
		}

		// Ensure instances are unique, to avoid querying the same store-gateway multiple times.
		// Here we use a simple slice iteration because we do expect the number of stores to
		// hit is low (few units).
		if !util.StringsContain(instances, owner) {
			instances = append(instances, owner)
		}
	}

	var clients []storegatewaypb.StoreGatewayClient

	// Get the client for each store-gateway.
	for _, id := range instances {
		addr, err := s.storesRing.GetInstanceAddr(id)
		if err != nil {
			return nil, errors.Wrapf(err, "get address of store-gateway instance %s", id)
		}

		c, err := s.clientsPool.GetClientFor(addr)
		if err != nil {
			return nil, errors.Wrapf(err, "get address of store-gateway instance %s", id)
		}

		clients = append(clients, c.(storegatewaypb.StoreGatewayClient))
	}

	return clients, nil
}
