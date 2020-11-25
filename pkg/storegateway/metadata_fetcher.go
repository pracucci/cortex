package storegateway

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util"
)

// MetadataFetcher discovers blocks using the bucket index and fetch meta.json
// for new blocks.
type MetadataFetcher struct {
	userID      string
	bkt         objstore.Bucket
	concurrency int
	logger      log.Logger
	strategy    ShardingStrategy

	// Keep all known block's meta.json in-memory. The full meta.json is required
	// by the Thanos BucketStore.
	//metasMx sync.Mutex
	//metas   map[ulid.ULID]*metadata.Meta
}

// TODO track metrics
func NewMetadataFetcher(userID string, bkt objstore.Bucket, concurrency int, strategy ShardingStrategy, logger log.Logger) *MetadataFetcher {
	return &MetadataFetcher{
		userID:      userID,
		bkt:         bkt,
		concurrency: concurrency,
		strategy:    strategy,
		logger:      util.WithUserID(userID, logger),
		//metas:       map[ulid.ULID]*metadata.Meta{},
	}
}

// Fetch implements metadata.MetadataFetcher.
func (f *MetadataFetcher) Fetch(ctx context.Context) (metas map[ulid.ULID]*metadata.Meta, partial map[ulid.ULID]error, err error) {
	// Check whether the user belongs to the shard.
	if len(f.strategy.FilterUsers(ctx, []string{f.userID})) != 1 {
		return nil, nil, nil
	}

	// Fetch the bucket index.
	idx, err := bucketindex.ReadIndex(ctx, f.bkt, f.userID, f.logger)
	if errors.Is(err, bucketindex.ErrIndexNotFound) || errors.Is(err, bucketindex.ErrIndexCorrupted) {
		// TODO test me: this is important because if a tenant doesn't have the bucketindex, we don't want the entire store-gateway to fail at startup
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	// Build block metas out of the index.
	metas = make(map[ulid.ULID]*metadata.Meta, len(idx.Blocks))
	for _, b := range idx.Blocks {
		m := b.ThanosMeta(f.userID)
		metas[b.ID] = &m
	}

	// TODO implement all the metadata filters we need

	return metas, nil, nil
}

/*
// Fetch implements metadata.MetadataFetcher.
func (f *MetadataFetcher) Fetch(ctx context.Context) (metas map[ulid.ULID]*metadata.Meta, partial map[ulid.ULID]error, err error) {
	// Check whether the user belongs to the shard.
	if len(f.strategy.FilterUsers(ctx, []string{f.userID})) != 1 {
		return nil, nil, nil
	}

	// Fetch the bucket index.
	idx, err := bucketindex.ReadIndex(ctx, f.bkt, f.userID, f.logger)
	if errors.Is(err, bucketindex.ErrIndexNotFound) || errors.Is(err, bucketindex.ErrIndexCorrupted) {
		// TODO test me: this is important because if a tenant doesn't have the bucketindex, we don't want the entire store-gateway to fail at startup
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	// Look for new blocks whose meta.json needs to be fetched.
	var toFetch []ulid.ULID
	toKeep := map[ulid.ULID]struct{}{}

	f.metasMx.Lock()
	for _, b := range idx.Blocks {
		if _, ok := f.metas[b.ID]; ok {
			toKeep[b.ID] = struct{}{}
		} else {
			toFetch = append(toFetch, b.ID)
		}
	}

	// Delete in-memory metas of blocks no more existing.
	for id := range f.metas {
		if _, ok := toKeep[id]; !ok {
			delete(f.metas, id)
		}
	}
	f.metasMx.Unlock()

	// Fetch meta.json of new blocks.
	err = concurrency.ForEachBlock(ctx, toFetch, f.concurrency, func(ctx context.Context, blockID ulid.ULID) error {
		bkt := bucketclient.NewUserBucketClient(f.userID, f.bkt)
		reader, err := bkt.Get(ctx, path.Join(blockID.String(), metadata.MetaFilename))
		if err != nil {
			return errors.Wrapf(err, "fetch meta.json for %s", blockID.String())
		}

		meta, err := metadata.Read(reader)
		if err != nil {
			return errors.Wrapf(err, "fetch meta.json for %s", blockID.String())
		}

		f.metasMx.Lock()
		f.metas[blockID] = meta
		f.metasMx.Unlock()

		return nil
	})

	if err != nil {
		return nil, nil, errors.Wrap(err, "fetch meta.json for newly discovered blocks")
	}

	// TODO implement all the metadata filters we need

	// Make a copy of the metas map before returning.
	f.metasMx.Lock()
	out := make(map[ulid.ULID]*metadata.Meta, len(f.metas))
	for k, v := range f.metas {
		out[k] = v
	}
	f.metasMx.Unlock()

	return out, nil, nil
}
*/

// UpdateOnChange implements metadata.MetadataFetcher.
func (f *MetadataFetcher) UpdateOnChange(func([]metadata.Meta, error)) {
	// Not supported.
}
