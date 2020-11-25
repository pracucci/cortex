package bucketindex

import (
	"testing"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
)

func TestMarkersIndexBucket_isBlockDeletionMark(t *testing.T) {
	block1 := ulid.MustNew(1, nil)

	tests := []struct {
		name       string
		expectedOk bool
		expectedID ulid.ULID
	}{
		{
			name:       "",
			expectedOk: false,
		}, {
			name:       "deletion-mark.json",
			expectedOk: false,
		}, {
			name:       block1.String() + "/index",
			expectedOk: false,
		}, {
			name:       block1.String() + "/deletion-mark.json",
			expectedOk: true,
			expectedID: block1,
		}, {
			name:       "/path/to/" + block1.String() + "/deletion-mark.json",
			expectedOk: true,
			expectedID: block1,
		},
	}

	b := BucketWithMarkersIndex(nil)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actualID, actualOk := b.isBlockDeletionMark(tc.name)
			assert.Equal(t, tc.expectedOk, actualOk)
			assert.Equal(t, tc.expectedID, actualID)
		})
	}
}
