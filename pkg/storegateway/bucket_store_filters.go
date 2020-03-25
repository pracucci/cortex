package storegateway

import (
	"context"

	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"

	"github.com/cortexproject/cortex/pkg/ring"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
)

const (
	shardExcludedMeta = "shard-excluded"
)

// RingShardedMetaFilter represents struct that allows sharding using the ring.
// Not go-routine safe.
type RingShardedMetaFilter struct {
	r          *ring.Ring
	instanceID string
}

// NewRingShardedMetaFilter creates RingShardedMetaFilter.
func NewRingShardedMetaFilter(r *ring.Ring, instanceID string) *RingShardedMetaFilter {
	return &RingShardedMetaFilter{
		r:          r,
		instanceID: instanceID,
	}
}

// Filter filters out blocks not included within the current shard.
func (f *RingShardedMetaFilter) Filter(_ context.Context, metas map[ulid.ULID]*metadata.Meta, synced *extprom.TxGaugeVec, _ bool) error {
	ringTokens := f.r.GetOwnedTokens(ring.BlocksSync)

	for id, _ := range metas {
		actualID, err := ringTokens.GetOwner(cortex_tsdb.HashBlockID(id))
		if err != nil {
			return err
		}

		if actualID != f.instanceID {
			synced.WithLabelValues(shardExcludedMeta).Inc()
			delete(metas, id)
		}
	}

	return nil
}
