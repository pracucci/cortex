package distributor

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/cortexproject/cortex/pkg/ring"
)

func TestSubRingCache_GetSet(t *testing.T) {
	c := newSubringCache()

	now := time.Now()
	assert.Nil(t, c.userSubring(now, "user1"))
	assert.Nil(t, c.userSubring(now, "user2"))

	c.setUserSubring(now, "user1", mockRing(5))
	assert.NotNil(t, c.userSubring(now, "user1"))
	assert.Nil(t, c.userSubring(now, "user2"))
	assert.Equal(t, 5, c.userSubring(now, "user1").IngesterCount())

	c.setUserSubring(now, "user2", mockRing(3))
	assert.NotNil(t, c.userSubring(now, "user1"))
	assert.NotNil(t, c.userSubring(now, "user2"))
	assert.Equal(t, 3, c.userSubring(now, "user2").IngesterCount())

	// Overwrite a value with a new one
	c.setUserSubring(now, "user1", mockRing(10))
	assert.Equal(t, 10, c.userSubring(now, "user1").IngesterCount())
	assert.Equal(t, 3, c.userSubring(now, "user2").IngesterCount())

	c.setUserSubring(now, "user2", nil)
	assert.Equal(t, 10, c.userSubring(now, "user1").IngesterCount())
	assert.Nil(t, c.userSubring(now, "user2"))
}

func TestSubRingCache_Purge(t *testing.T) {
	c := newSubringCache()

	now := time.Now()

	const max = 10

	for i := 0; i < max; i++ {
		c.setUserSubring(now.Add(time.Duration(-i)*time.Minute), fmt.Sprintf("user-%d", i), mockRing(i))
	}

	for i := max; i >= 0; i-- {
		// Entries *older* than (but not equal to) timestamp are removed.
		alive := c.purge(now.Add(time.Duration(-i) * time.Minute))

		exp := i + 1
		if exp > max {
			exp = max
		}

		assert.Equal(t, exp, alive)
	}

	for i := 0; i < max; i++ {
		if i == 0 {
			assert.NotNil(t, c.userSubring(now, fmt.Sprintf("user-%d", i)))
		} else {
			assert.Nil(t, c.userSubring(now, fmt.Sprintf("user-%d", i)))
		}
	}
}

type mockRing int

func (m mockRing) Describe(descs chan<- *prometheus.Desc) {}

func (m mockRing) Collect(metrics chan<- prometheus.Metric) {}

func (m mockRing) Get(key uint32, op ring.Operation, buf []ring.IngesterDesc) (ring.ReplicationSet, error) {
	return ring.ReplicationSet{}, nil
}

func (m mockRing) GetAll(op ring.Operation) (ring.ReplicationSet, error) {
	return ring.ReplicationSet{}, nil
}

func (m mockRing) ReplicationFactor() int {
	return 3
}

func (m mockRing) IngesterCount() int {
	return int(m)
}

func (m mockRing) ShuffleShard(identifier string, size int, cachedRing ring.ReadRing) ring.ReadRing {
	return m
}

func (m mockRing) HasInstance(instanceID string) bool {
	return false
}
