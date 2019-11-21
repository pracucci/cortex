package store

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketDiscovery_getMissingBlocksRequests(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		mint          time.Time
		maxt          time.Time
		maxBlockRange time.Duration
		blocksRanges  TimeRanges
		expected      []blocksListRequest
	}{
		"single day": {
			mint:          mustParseTime(time.RFC3339, "2019-11-10T03:15:00Z"),
			maxt:          mustParseTime(time.RFC3339, "2019-11-10T05:30:00Z"),
			maxBlockRange: 2 * time.Hour,
			expected: []blocksListRequest{
				{
					timeRange:    TimeRange{toMillis(mustParseTime(time.RFC3339, "2019-11-10T00:00:00Z")), toMillis(mustParseTime(time.RFC3339, "2019-11-11T00:00:00Z")) - 1},
					bucketPrefix: "mint-2019-11-10T",
				},
			},
		},
		"multiple non continuous days": {
			mint:          mustParseTime(time.RFC3339, "2019-11-10T03:15:00Z"),
			maxt:          mustParseTime(time.RFC3339, "2019-11-14T05:30:00Z"),
			blocksRanges:  TimeRanges{{toMillis(mustParseTime(time.RFC3339, "2019-11-10T08:00:00Z")), toMillis(mustParseTime(time.RFC3339, "2019-11-14T02:00:00Z"))}},
			maxBlockRange: 2 * time.Hour,
			expected: []blocksListRequest{
				{
					timeRange:    TimeRange{toMillis(mustParseTime(time.RFC3339, "2019-11-10T00:00:00Z")), toMillis(mustParseTime(time.RFC3339, "2019-11-11T00:00:00Z")) - 1},
					bucketPrefix: "mint-2019-11-10T",
				},
				{
					timeRange:    TimeRange{toMillis(mustParseTime(time.RFC3339, "2019-11-14T00:00:00Z")), toMillis(mustParseTime(time.RFC3339, "2019-11-15T00:00:00Z")) - 1},
					bucketPrefix: "mint-2019-11-14T",
				},
			},
		},
		"multiple days starting with the same digit": {
			mint:          mustParseTime(time.RFC3339, "2019-11-10T03:15:00Z"),
			maxt:          mustParseTime(time.RFC3339, "2019-11-12T05:30:00Z"),
			maxBlockRange: 2 * time.Hour,
			expected: []blocksListRequest{
				{
					timeRange:    TimeRange{toMillis(mustParseTime(time.RFC3339, "2019-11-10T00:00:00Z")), toMillis(mustParseTime(time.RFC3339, "2019-11-13T00:00:00Z")) - 1},
					bucketPrefix: "mint-2019-11-1",
				},
			},
		},
		"multiple days starting with a different digit": {
			mint:          mustParseTime(time.RFC3339, "2019-11-08T03:15:00Z"),
			maxt:          mustParseTime(time.RFC3339, "2019-11-12T05:30:00Z"),
			maxBlockRange: 2 * time.Hour,
			expected: []blocksListRequest{
				{
					timeRange:    TimeRange{toMillis(mustParseTime(time.RFC3339, "2019-11-08T00:00:00Z")), toMillis(mustParseTime(time.RFC3339, "2019-11-13T00:00:00Z")) - 1},
					bucketPrefix: "mint-2019-11-",
				},
			},
		},
		"multiple days spanning across two months": {
			mint:          mustParseTime(time.RFC3339, "2019-11-08T03:15:00Z"),
			maxt:          mustParseTime(time.RFC3339, "2019-12-12T05:30:00Z"),
			maxBlockRange: 2 * time.Hour,
			expected: []blocksListRequest{
				{
					timeRange:    TimeRange{toMillis(mustParseTime(time.RFC3339, "2019-11-08T00:00:00Z")), toMillis(mustParseTime(time.RFC3339, "2019-12-13T00:00:00Z")) - 1},
					bucketPrefix: "mint-2019-1",
				},
			},
		},
		"multiple months spanning across two years": {
			mint:          mustParseTime(time.RFC3339, "2019-11-08T03:15:00Z"),
			maxt:          mustParseTime(time.RFC3339, "2020-01-02T05:30:00Z"),
			maxBlockRange: 2 * time.Hour,
			expected: []blocksListRequest{
				{
					timeRange:    TimeRange{toMillis(mustParseTime(time.RFC3339, "2019-11-08T00:00:00Z")), toMillis(mustParseTime(time.RFC3339, "2020-01-03T00:00:00Z")) - 1},
					bucketPrefix: "mint-20",
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			discovery := NewBucketDiscovery(nil, testData.maxBlockRange, nil)
			discovery.blocksRanges = testData.blocksRanges
			actual := discovery.getMissingBlocksRequests(toMillis(testData.mint), toMillis(testData.maxt))

			assert.Equal(t, testData.expected, actual)
		})
	}
}
