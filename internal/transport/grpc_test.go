package transport

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGetOrCreateClient(t *testing.T) {
	callCount := 0
	mockFactory := func(conn grpc.ClientConnInterface) any {
		callCount++
		return struct{}{}
	}

	cc := NewClientCache(mockFactory, grpc.WithTransportCredentials(insecure.NewCredentials()))

	// First invocation for an address should be a cache miss and a client should be created.
	addr1 := "127.0.0.1:8080"
	c, err := cc.GetOrCreate(addr1)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, 1, callCount)

	// Second invocation for an address should be a cache hit and a client should not be created.
	c, err = cc.GetOrCreate(addr1)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, 1, callCount)
}
