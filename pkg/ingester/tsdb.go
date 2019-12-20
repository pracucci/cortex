package ingester

import (
	"context"
	"sync"
	"time"

	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/tsdb"
	lbls "github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/shipper"
)

// usersDB holds the TSDB for all users.
type usersDB struct {
	lock  sync.RWMutex
	users map[string]*userDB

	// Bucket used to store TSDB blocks.
	bucket objstore.Bucket

	// Channel and wait group used to signal when the DBs should be closed,
	// and wait for it.
	quit     chan struct{}
	shippers sync.WaitGroup
}

// usersDB holds the TSDB for a single user
// TODO(pracucci) remove me if - at the end - will just hold the TSDB
type userDB struct {
	db *tsdb.DB
}

func newUsersDB(bucket objstore.Bucket) *usersDB {
	return &usersDB{
		users:  map[string]*userDB{},
		bucket: bucket,
		quit:   make(chan struct{}),
	}
}

// getUserDB returns the TSDB for the input user if the DB has already been created,
// nil otherwise.
// TODO(pracucci) test me
func (u *usersDB) getUserTSDB(userID string) *tsdb.DB {
	u.lock.RLock()
	defer u.lock.RUnlock()

	if db, ok := u.users[userID]; ok {
		return db.db
	}

	return nil
}

// createUserDB creates the DB for the input user, ensuring the DB doesn't already
// exist.
// TODO(pracucci) test me
func (u *usersDB) createUserTSDB(userID string, config cortex_tsdb.Config) (*tsdb.DB, error) {
	u.lock.Lock()
	defer u.lock.Unlock()

	// Check again for DB in the event it was created in-between locks
	if db, ok := u.users[userID]; ok {
		return db.db, nil
	}

	udir := config.BlocksDir(userID)

	// Create a new user database
	db, err := tsdb.Open(udir, util.Logger, nil, &tsdb.Options{
		RetentionDuration: uint64(config.Retention / time.Millisecond),
		BlockRanges:       config.BlockRanges.ToMillisecondRanges(),
		NoLockfile:        true,
	})
	if err != nil {
		return nil, err
	}

	// Thanos shipper requires at least 1 external label to be set. For this reason,
	// we set the tenant ID as external label and we'll filter it out when reading
	// the series from the storage.
	l := lbls.Labels{
		{
			Name:  cortex_tsdb.TenantIDExternalLabel,
			Value: userID,
		},
	}

	// Create a new shipper for this database
	s := shipper.New(util.Logger, nil, udir, &Bucket{userID, u.bucket}, func() lbls.Labels { return l }, metadata.ReceiveSource)
	u.shippers.Add(1)
	go func() {
		defer u.shippers.Done()
		runutil.Repeat(config.ShipInterval, u.quit, func() error {
			if uploaded, err := s.Sync(context.Background()); err != nil {
				level.Warn(util.Logger).Log("err", err, "uploaded", uploaded)
			}
			return nil
		})
	}()

	u.users[userID] = &userDB{
		db: db,
	}

	return db, nil
}

// empty returns true if there are no DBs.
// TODO(pracucci) test me
func (u *usersDB) empty() bool {
	u.lock.RLock()
	defer u.lock.RUnlock()
	return len(u.users) == 0
}

// disableCompactions on all DBs. If there's any on-going compaction,
// it will wait until completed.
// TODO(pracucci) test me
func (u *usersDB) disableCompactions() {
	u.lock.RLock()
	wg := &sync.WaitGroup{}
	wg.Add(len(u.users))

	for _, user := range u.users {
		go func(db *tsdb.DB) {
			defer wg.Done()
			db.DisableCompactions()
		}(user.db)
	}

	u.lock.RUnlock()
	wg.Wait()
}

// close all DBs and wait until completed. If there are open TSDB readers (ie. on-going
// queries), it will wait until all readers have been released.
// TODO(pracucci) test me
func (u *usersDB) close() {
	u.lock.Lock()

	wg := &sync.WaitGroup{}
	wg.Add(len(u.users))

	// Concurrently close all users TSDB
	for userID, user := range u.users {
		userID := userID

		go func(db *tsdb.DB) {
			defer wg.Done()

			if err := db.Close(); err != nil {
				level.Warn(util.Logger).Log("msg", "unable to close TSDB", "err", err, "user", userID)
				return
			}

			// Now that the TSDB has been closed, we should remove it from the
			// set of open ones. This lock acquisition doesn't deadlock with the
			// outer one, because the outer one is released as soon as all go
			// routines are started.
			u.lock.Lock()
			delete(u.users, userID)
			u.lock.Unlock()
		}(user.db)
	}

	// Wait until all Close() completed
	u.lock.Unlock()
	wg.Wait()

	// Finally, we can signal the shippers to stop and wait for it
	close(u.quit)
	u.shippers.Wait()
}
