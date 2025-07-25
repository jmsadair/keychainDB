package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewKey(t *testing.T) {
	key := "key"
	version := uint64(1)

	k := NewMetadataKey(key)
	require.Equal(t, key, k.ClientKey())
	require.True(t, k.IsMetadata())
	require.False(t, k.IsCommitted())
	require.False(t, k.IsDirty())

	k = NewCommittedKey(key)
	require.Equal(t, key, k.ClientKey())
	require.False(t, k.IsMetadata())
	require.True(t, k.IsCommitted())
	require.False(t, k.IsDirty())

	k = NewDirtyKey(key, version)
	require.Equal(t, key, k.ClientKey())
	require.False(t, k.IsMetadata())
	require.False(t, k.IsCommitted())
	require.True(t, k.IsDirty())
}
