package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalKey(t *testing.T) {
	key := "key"
	version := uint64(1)

	ck, err := NewMetadataKey(key)
	require.NoError(t, err)
	require.Equal(t, key, ck.Key())
	require.True(t, ck.IsMetadataKey())
	require.Zero(t, ck.Version())

	ck, err = NewDataKey(key, version)
	require.NoError(t, err)
	require.Equal(t, key, ck.Key())
	require.False(t, ck.IsMetadataKey())
	require.Equal(t, version, ck.Version())
}
