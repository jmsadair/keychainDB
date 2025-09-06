package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKey(t *testing.T) {
	key := "key"
	version := uint64(1)

	k := newMetadataKey(key)
	require.Equal(t, key, k.clientKey())
	require.True(t, k.isMetadata())
	require.False(t, k.isCommitted())
	require.False(t, k.isDirty())
	require.Zero(t, k.version())

	k = newCommittedKey(key, version)
	require.Equal(t, key, k.clientKey())
	require.False(t, k.isMetadata())
	require.True(t, k.isCommitted())
	require.False(t, k.isDirty())
	require.Equal(t, version, k.version())

	k = newDirtyKey(key, version)
	require.Equal(t, key, k.clientKey())
	require.False(t, k.isMetadata())
	require.False(t, k.isCommitted())
	require.True(t, k.isDirty())
	require.Equal(t, version, k.version())
}
