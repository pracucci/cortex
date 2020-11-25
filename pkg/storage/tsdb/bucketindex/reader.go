package bucketindex

import (
	"context"
	"encoding/json"
	"io/ioutil"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/cortexproject/cortex/pkg/storage/tsdb/bucketclient"
)

var (
	ErrIndexNotFound  = errors.New("bucket index not found")
	ErrIndexCorrupted = errors.New("bucket index corrupted")
)

func ReadIndex(ctx context.Context, bkt objstore.Bucket, userID string, logger log.Logger) (*Index, error) {
	bkt = bucketclient.NewUserBucketClient(userID, bkt)

	// Get the bucket index.
	reader, err := bkt.Get(ctx, IndexFilename)
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return nil, ErrIndexNotFound
		}
		return nil, errors.Wrapf(err, "read bucket index for %s", userID)
	}
	defer runutil.CloseWithLogOnErr(logger, reader, "close bucket index reader")

	// Read all the content.
	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "read bucket index for %s", userID)
	}

	// Deserialize it.
	index := &Index{}
	if err := json.Unmarshal(content, index); err != nil {
		return nil, errors.Wrapf(ErrIndexCorrupted, "unmarshal bucket index for %s", userID)
	}

	return index, nil
}
