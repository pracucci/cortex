package bucketindex

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/cortexproject/cortex/pkg/storage/backend/filesystem"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketclient"
)

func TestWriter_GenerateIndex_NoTenantInTheBucket(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()

	storageDir, err := ioutil.TempDir(os.TempDir(), "")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(storageDir))
	}()

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	for _, oldIdx := range []*Index{nil, {}} {
		w := NewWriter(objstore.BucketWithMetrics("test", bkt, nil), userID, log.NewNopLogger())
		idx, err := w.GenerateIndex(ctx, oldIdx)

		require.NoError(t, err)
		assert.Equal(t, IndexVersion1, idx.Version)
		assert.InDelta(t, time.Now().Unix(), idx.UpdatedAt, 2)
		assert.Len(t, idx.Blocks, 0)
		assert.Len(t, idx.BlockDeletionMarks, 0)
	}
}

// TODO test is flaky because Index.Blocks and Index.BlockDeletionMarks ordering is not guaranteed.
func TestWriter_GenerateIndex(t *testing.T) {
	const userID = "user-1"

	ctx := context.Background()

	storageDir, err := ioutil.TempDir(os.TempDir(), "")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(storageDir))
	}()

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	// Generate the initial index.
	bkt = BucketWithMarkersIndex(bkt)
	block1 := mockStorageBlock(t, bkt, userID, 10, 20)
	block2 := mockStorageBlock(t, bkt, userID, 20, 30)
	block2Mark := mockStorageDeletionMark(t, bkt, userID, block2)

	w := NewWriter(objstore.BucketWithMetrics("test", bkt, nil), userID, log.NewNopLogger())
	idx, err := w.GenerateIndex(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, IndexVersion1, idx.Version)
	assert.InDelta(t, time.Now().Unix(), idx.UpdatedAt, 2)
	require.Len(t, idx.Blocks, 2)
	require.Len(t, idx.BlockDeletionMarks, 1)
	for i, b := range []tsdb.BlockMeta{block1, block2} {
		assert.Equal(t, b.ULID, idx.Blocks[i].ID)
		assert.Equal(t, b.MinTime, idx.Blocks[i].MinTime)
		assert.Equal(t, b.MaxTime, idx.Blocks[i].MaxTime)
	}
	for i, m := range []*metadata.DeletionMark{block2Mark} {
		assert.Equal(t, m.ID, idx.BlockDeletionMarks[i].ID)
		assert.Equal(t, m.DeletionTime, idx.BlockDeletionMarks[i].DeletionTime)
	}

	// Create new blocks, and generate a new index.
	block3 := mockStorageBlock(t, bkt, userID, 30, 40)
	block4 := mockStorageBlock(t, bkt, userID, 40, 50)
	block4Mark := mockStorageDeletionMark(t, bkt, userID, block4)

	idx, err = w.GenerateIndex(ctx, idx)
	require.NoError(t, err)
	assert.Equal(t, IndexVersion1, idx.Version)
	assert.InDelta(t, time.Now().Unix(), idx.UpdatedAt, 2)
	require.Len(t, idx.Blocks, 4)
	require.Len(t, idx.BlockDeletionMarks, 2)
	for i, b := range []tsdb.BlockMeta{block1, block2, block3, block4} {
		assert.Equal(t, b.ULID, idx.Blocks[i].ID)
		assert.Equal(t, b.MinTime, idx.Blocks[i].MinTime)
		assert.Equal(t, b.MaxTime, idx.Blocks[i].MaxTime)
	}
	for i, m := range []*metadata.DeletionMark{block2Mark, block4Mark} {
		assert.Equal(t, m.ID, idx.BlockDeletionMarks[i].ID)
		assert.Equal(t, m.DeletionTime, idx.BlockDeletionMarks[i].DeletionTime)
	}

	// Hard delete a block and generate a new index.
	require.NoError(t, block.Delete(ctx, log.NewNopLogger(), bucketclient.NewUserBucketClient(userID, bkt), block2.ULID))

	idx, err = w.GenerateIndex(ctx, idx)
	require.NoError(t, err)
	assert.Equal(t, IndexVersion1, idx.Version)
	assert.InDelta(t, time.Now().Unix(), idx.UpdatedAt, 2)
	require.Len(t, idx.Blocks, 3)
	require.Len(t, idx.BlockDeletionMarks, 1)
	for i, b := range []tsdb.BlockMeta{block1, block3, block4} {
		assert.Equal(t, b.ULID, idx.Blocks[i].ID)
		assert.Equal(t, b.MinTime, idx.Blocks[i].MinTime)
		assert.Equal(t, b.MaxTime, idx.Blocks[i].MaxTime)
	}
	for i, m := range []*metadata.DeletionMark{block4Mark} {
		assert.Equal(t, m.ID, idx.BlockDeletionMarks[i].ID)
		assert.Equal(t, m.DeletionTime, idx.BlockDeletionMarks[i].DeletionTime)
	}
}

// TODO copied from blocks_scanner_test.go
func mockStorageBlock(t *testing.T, bucket objstore.Bucket, userID string, minT, maxT int64) tsdb.BlockMeta {
	// Generate a block ID whose timestamp matches the maxT (for simplicity we assume it
	// has been compacted and shipped in zero time, even if not realistic).
	id := ulid.MustNew(uint64(maxT), rand.Reader)

	meta := tsdb.BlockMeta{
		Version: 1,
		ULID:    id,
		MinTime: minT,
		MaxTime: maxT,
		Compaction: tsdb.BlockMetaCompaction{
			Level:   1,
			Sources: []ulid.ULID{id},
		},
	}

	metaContent, err := json.Marshal(meta)
	if err != nil {
		panic("failed to marshal mocked block meta")
	}

	metaContentReader := strings.NewReader(string(metaContent))
	metaPath := fmt.Sprintf("%s/%s/meta.json", userID, id.String())
	require.NoError(t, bucket.Upload(context.Background(), metaPath, metaContentReader))

	return meta
}

// TODO copied from blocks_scanner_test.go
func mockStorageDeletionMark(t *testing.T, bucket objstore.Bucket, userID string, meta tsdb.BlockMeta) *metadata.DeletionMark {
	mark := metadata.DeletionMark{
		ID:           meta.ULID,
		DeletionTime: time.Now().Add(-time.Minute).Unix(),
		Version:      metadata.DeletionMarkVersion1,
	}

	markContent, err := json.Marshal(mark)
	if err != nil {
		panic("failed to marshal mocked block meta")
	}

	markContentReader := strings.NewReader(string(markContent))
	markPath := fmt.Sprintf("%s/%s/%s", userID, meta.ULID.String(), metadata.DeletionMarkFilename)
	require.NoError(t, bucket.Upload(context.Background(), markPath, markContentReader))

	return &mark
}
