package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewInternalKey(t *testing.T) {
	key := "key"
	version := uint64(1)
	isMetadataKey := false

	ik := NewInternalKey(key, isMetadataKey, version)
	require.Equal(t, key, ik.Key)
	require.Equal(t, isMetadataKey, ik.IsMetadataKey)
	require.Equal(t, version, ik.Version)
}

func TestNewInternalKeyBytes(t *testing.T) {
	key := "key"
	version := uint64(1)
	isMetadataKey := false
	ik := NewInternalKey(key, isMetadataKey, version)

	b, err := ik.Bytes()
	require.NoError(t, err)
	ikFromBytes, err := NewInternalKeyFromBytes(b)
	require.NoError(t, err)
	require.Equal(t, key, ikFromBytes.Key)
	require.Equal(t, isMetadataKey, ikFromBytes.IsMetadataKey)
	require.Equal(t, version, ikFromBytes.Version)

	version = 0
	isMetadataKey = true
	ik.Version = version
	ik.IsMetadataKey = isMetadataKey
	b, err = ik.Bytes()
	require.NoError(t, err)
	ikFromBytes, err = NewInternalKeyFromBytes(b)
	require.NoError(t, err)
	require.Equal(t, key, ikFromBytes.Key)
	require.Equal(t, isMetadataKey, ikFromBytes.IsMetadataKey)
	require.Equal(t, version, ikFromBytes.Version)
}
