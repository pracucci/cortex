package ui

import (
	"context"
	"math"
	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/querier"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type StorageAPI struct {
	services.Service

	bucketClient  objstore.Bucket
	blocksScanner *querier.BlocksScanner
}

func NewStorageAPI(config cortex_tsdb.Config, logger log.Logger) (*StorageAPI, error) {
	bucketClient, err := cortex_tsdb.NewBucketClient(context.Background(), config, "ui", logger, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create bucket client")
	}

	// TODO create based on the config
	cfg := querier.BlocksScannerConfig{
		ScanInterval:             time.Minute,
		TenantsConcurrency:       10,
		MetasConcurrency:         10,
		CacheDir:                 "/tmp",
		ConsistencyDelay:         0,
		IgnoreDeletionMarksDelay: time.Hour,
	}

	api := &StorageAPI{
		bucketClient:  bucketClient,
		blocksScanner: querier.NewBlocksScanner(cfg, bucketClient, logger, nil),
	}

	api.Service = services.NewIdleService(api.starting, api.stopping)

	return api, nil
}

func (s *StorageAPI) starting(ctx context.Context) error {
	return services.StartAndAwaitRunning(ctx, s.blocksScanner)
}

func (s *StorageAPI) stopping(_ error) error {
	return services.StopAndAwaitTerminated(context.Background(), s.blocksScanner)
}

type GetBucketResponse struct {
	Tenants []string `json:"tenants"`
}

func (s *StorageAPI) GetBucket(w http.ResponseWriter, r *http.Request) {
	util.WriteJSONResponse(w, APIResponse{
		Data: GetBucketResponse{
			Tenants: s.blocksScanner.GetUsers(),
		},
	})
}

type GetUserBlocksResponse struct {
	TenantID string  `json:"tenant_id"`
	Blocks   []Block `json:"blocks"`
}

type GetUserBlockResponse struct {
	TenantID string `json:"tenant_id"`
	Block    Block  `json:"block"`
}

func (s *StorageAPI) GetUserBlocks(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	// Get the tenant ID from the request.
	tenantID := vars["tenant_id"]
	if tenantID == "" {
		http.Error(w, "no tenant_id specified in the request", http.StatusBadRequest)
		return
	}

	// Get all user's blocks.
	metas, _, err := s.blocksScanner.GetBlocks(tenantID, math.MinInt64, math.MaxInt64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Export blocks.
	blocks := make([]Block, 0, len(metas))
	for _, meta := range metas {
		blocks = append(blocks, createBlockFromMeta(meta))
	}

	util.WriteJSONResponse(w, APIResponse{
		Data: GetUserBlocksResponse{
			TenantID: tenantID,
			Blocks:   blocks,
		},
	})
}

func (s *StorageAPI) GetUserBlock(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	// Get the tenant ID from the request.
	tenantID := vars["tenant_id"]
	if tenantID == "" {
		http.Error(w, "no tenant ID specified in the request", http.StatusBadRequest)
		return
	}

	// Get the block ID from the request.
	blockID := vars["block_id"]
	if blockID == "" {
		http.Error(w, "no block ID specified in the request", http.StatusBadRequest)
		return
	}

	// Get all user's blocks.
	metas, _, err := s.blocksScanner.GetBlocks(tenantID, math.MinInt64, math.MaxInt64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Look for the requested block.
	var found *querier.BlockMeta
	for _, meta := range metas {
		if meta.ULID.String() == blockID {
			found = meta
			break
		}
	}

	if found == nil {
		http.Error(w, "block not found", http.StatusBadRequest)
		return
	}

	// TODO Get extended statistics.

	util.WriteJSONResponse(w, APIResponse{
		Data: GetUserBlockResponse{
			TenantID: tenantID,
			Block:    createBlockFromMeta(found),
		},
	})
}
