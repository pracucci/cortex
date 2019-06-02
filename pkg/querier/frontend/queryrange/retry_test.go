package queryrange

import (
	"context"
	fmt "fmt"
	"sync/atomic"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/require"
)

func TestRetry(t *testing.T) {
	try := int32(0)

	h := NewRetryMiddleware(log.NewNopLogger(), 5).Wrap(
		HandlerFunc(func(_ context.Context, req *Request) (*APIResponse, error) {
			if atomic.AddInt32(&try, 1) == 5 {
				return &APIResponse{Status: "Hello World"}, nil
			}
			return nil, fmt.Errorf("fail")
		}),
	)

	resp, err := h.Do(nil, nil)
	require.NoError(t, err)
	require.Equal(t, &APIResponse{Status: "Hello World"}, resp)
}
