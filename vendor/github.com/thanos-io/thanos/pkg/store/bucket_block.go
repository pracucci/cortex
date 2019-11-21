package store

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/pool"
	"github.com/thanos-io/thanos/pkg/runutil"
)

// bucketBlock represents a block that is located in a bucket. It holds intermediate
// state for the block on local disk.
type bucketBlock struct {
	logger     log.Logger
	bucket     objstore.BucketReader
	meta       *metadata.Meta
	dir        string
	indexCache indexCache
	chunkPool  *pool.BytesPool

	indexVersion int
	symbols      []string
	lvals        map[string][]string
	postings     map[labels.Label]index.Range

	id        ulid.ULID
	chunkObjs []string

	pendingReaders sync.WaitGroup

	partitioner partitioner

	labels labels.Labels
}

func newBucketBlock(
	ctx context.Context,
	logger log.Logger,
	bkt objstore.BucketReader,
	id ulid.ULID,
	dir string,
	indexCache indexCache,
	chunkPool *pool.BytesPool,
	p partitioner,
) (b *bucketBlock, err error) {
	b = &bucketBlock{
		logger:      logger,
		bucket:      bkt,
		id:          id,
		indexCache:  indexCache,
		chunkPool:   chunkPool,
		dir:         dir,
		partitioner: p,
	}
	err, meta := loadMeta(ctx, logger, bkt, dir, id)
	if err != nil {
		return nil, errors.Wrap(err, "load meta")
	}
	b.meta = meta

	if err = b.loadIndexCacheFile(ctx); err != nil {
		return nil, errors.Wrap(err, "load index cache")
	}
	// Get object handles for all chunk files.
	err = bkt.Iter(ctx, path.Join(id.String(), block.ChunksDirname), func(n string) error {
		b.chunkObjs = append(b.chunkObjs, n)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "list chunk files")
	}
	return b, nil
}

func (b *bucketBlock) indexFilename() string {
	return path.Join(b.id.String(), block.IndexFilename)
}

func (b *bucketBlock) indexCacheFilename() string {
	return path.Join(b.id.String(), block.IndexCacheFilename)
}

func (b *bucketBlock) loadIndexCacheFile(ctx context.Context) (err error) {
	cachefn := filepath.Join(b.dir, block.IndexCacheFilename)
	if err = b.loadIndexCacheFileFromFile(ctx, cachefn); err == nil {
		return nil
	}
	if !os.IsNotExist(errors.Cause(err)) {
		return errors.Wrap(err, "read index cache")
	}

	// Try to download index cache file from object store.
	if err = objstore.DownloadFile(ctx, b.logger, b.bucket, b.indexCacheFilename(), cachefn); err == nil {
		return b.loadIndexCacheFileFromFile(ctx, cachefn)
	}

	if !b.bucket.IsObjNotFoundErr(errors.Cause(err)) {
		return errors.Wrap(err, "download index cache file")
	}

	// No cache exists on disk yet, build it from the downloaded index and retry.
	fn := filepath.Join(b.dir, block.IndexFilename)

	if err := objstore.DownloadFile(ctx, b.logger, b.bucket, b.indexFilename(), fn); err != nil {
		return errors.Wrap(err, "download index file")
	}

	defer func() {
		if rerr := os.Remove(fn); rerr != nil {
			level.Error(b.logger).Log("msg", "failed to remove temp index file", "path", fn, "err", rerr)
		}
	}()

	if err := block.WriteIndexCache(b.logger, fn, cachefn); err != nil {
		return errors.Wrap(err, "write index cache")
	}

	return errors.Wrap(b.loadIndexCacheFileFromFile(ctx, cachefn), "read index cache")
}

func (b *bucketBlock) loadIndexCacheFileFromFile(ctx context.Context, cache string) (err error) {
	b.indexVersion, b.symbols, b.lvals, b.postings, err = block.ReadIndexCache(b.logger, cache)
	return err
}

func (b *bucketBlock) readIndexRange(ctx context.Context, off, length int64) ([]byte, error) {
	r, err := b.bucket.GetRange(ctx, b.indexFilename(), off, length)
	if err != nil {
		return nil, errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(b.logger, r, "readIndexRange close range reader")

	c, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "read range")
	}
	return c, nil
}

func (b *bucketBlock) readChunkRange(ctx context.Context, seq int, off, length int64) (*[]byte, error) {
	c, err := b.chunkPool.Get(int(length))
	if err != nil {
		return nil, errors.Wrap(err, "allocate chunk bytes")
	}
	buf := bytes.NewBuffer(*c)

	r, err := b.bucket.GetRange(ctx, b.chunkObjs[seq], off, length)
	if err != nil {
		b.chunkPool.Put(c)
		return nil, errors.Wrap(err, "get range reader")
	}
	defer runutil.CloseWithLogOnErr(b.logger, r, "readChunkRange close range reader")

	if _, err = io.Copy(buf, r); err != nil {
		b.chunkPool.Put(c)
		return nil, errors.Wrap(err, "read range")
	}
	internalBuf := buf.Bytes()
	return &internalBuf, nil
}

func (b *bucketBlock) indexReader(ctx context.Context) *bucketIndexReader {
	b.pendingReaders.Add(1)
	return newBucketIndexReader(ctx, b.logger, b, b.indexCache)
}

func (b *bucketBlock) chunkReader(ctx context.Context) *bucketChunkReader {
	b.pendingReaders.Add(1)
	return newBucketChunkReader(ctx, b)
}

// Close waits for all pending readers to finish and then closes all underlying resources.
func (b *bucketBlock) Close() error {
	b.pendingReaders.Wait()
	return nil
}
