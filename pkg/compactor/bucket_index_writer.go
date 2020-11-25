package compactor

import (
	"context"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"

	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketindex"
	"github.com/cortexproject/cortex/pkg/util/concurrency"
	"github.com/cortexproject/cortex/pkg/util/services"
)

type BucketIndexWriterConfig struct {
	UpdateInterval    time.Duration
	UpdateConcurrency int
}

type BucketIndexWriter struct {
	services.Service

	cfg          BucketIndexWriterConfig
	logger       log.Logger
	bucketClient objstore.InstrumentedBucket
	usersScanner *cortex_tsdb.UsersScanner
}

// TODO track metrics
func NewBucketIndexWriter(cfg BucketIndexWriterConfig, bucketClient objstore.InstrumentedBucket, usersScanner *cortex_tsdb.UsersScanner, logger log.Logger) *BucketIndexWriter {
	w := &BucketIndexWriter{
		cfg:          cfg,
		bucketClient: bucketClient,
		logger:       logger,
		usersScanner: usersScanner,
	}

	w.Service = services.NewTimerService(cfg.UpdateInterval, w.starting, w.ticker, nil)

	return w
}

func (w *BucketIndexWriter) starting(ctx context.Context) error {
	// Ensure all bucket indexes are updated at startup.
	w.runUpdate(ctx)

	return nil
}

func (w *BucketIndexWriter) ticker(ctx context.Context) error {
	w.runUpdate(ctx)

	return nil
}

func (w *BucketIndexWriter) runUpdate(ctx context.Context) {
	level.Info(w.logger).Log("msg", "started updating bucket indexes")
	// TODO w.runsStarted.Inc()

	if err := w.updateUsers(ctx); err == nil {
		level.Info(w.logger).Log("msg", "successfully completed bucket indexes update")
		// TODO w.runsCompleted.Inc()
		// TODO w.runsLastSuccess.SetToCurrentTime()
	} else if errors.Is(err, context.Canceled) {
		level.Info(w.logger).Log("msg", "canceled bucket indexes update", "err", err)
		return
	} else {
		level.Error(w.logger).Log("msg", "failed to update bucket indexes", "err", err.Error())
		// TODO w.runsFailed.Inc()
	}
}

func (w *BucketIndexWriter) updateUsers(ctx context.Context) error {
	users, err := w.usersScanner.ScanUsers(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to discover users from bucket")
	}

	return concurrency.ForEachUser(ctx, users, w.cfg.UpdateConcurrency, func(ctx context.Context, userID string) error {
		return errors.Wrapf(w.updateUser(ctx, userID), "failed to delete user blocks (user: %s)", userID)
	})
}

func (w *BucketIndexWriter) updateUser(ctx context.Context, userID string) error {
	// Read the index.
	idx, err := bucketindex.ReadIndex(ctx, w.bucketClient, userID, w.logger)
	if err != nil && !errors.Is(err, bucketindex.ErrIndexNotFound) && !errors.Is(err, bucketindex.ErrIndexCorrupted) {
		return err
	}

	// Write the update index.
	writer := bucketindex.NewWriter(w.bucketClient, userID, w.logger)
	_, err = writer.WriteIndex(ctx, idx)
	return err
}
