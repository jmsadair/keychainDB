package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommitThenRead(t *testing.T) {
	store, err := NewPersistantStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key := "key"
	value := []byte("value")
	version, err := store.CommittedWriteNewVersion(key, value)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)

	readValue, err := store.CommittedRead(key)
	require.NoError(t, err)
	require.Equal(t, value, readValue)
}

func TestSingleUncommittedWriteThenRead(t *testing.T) {
	store, err := NewPersistantStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key := "key"
	value := []byte("value")
	version, err := store.UncommittedWriteNewVersion(key, value)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)

	_, err = store.CommittedRead(key)
	require.ErrorIs(t, err, ErrDirtyRead)
}

func TestMultipleUncommittedWriteThenRead(t *testing.T) {
	store, err := NewPersistantStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key := "key"
	value1 := []byte("value1")
	version, err := store.UncommittedWriteNewVersion(key, value1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
	value2 := []byte("value2")
	version, err = store.UncommittedWriteNewVersion(key, value2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), version)

	_, err = store.CommittedRead(key)
	require.ErrorIs(t, err, ErrDirtyRead)
}

func TestUncommittedWriteCommitThenRead(t *testing.T) {
	store, err := NewPersistantStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key := "key"
	value := []byte("value")
	version, err := store.UncommittedWriteNewVersion(key, value)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)

	err = store.CommitVersion(key, version)
	require.NoError(t, err)
	readValue, err := store.CommittedRead(key)
	require.NoError(t, err)
	require.Equal(t, string(value), string(readValue))
}

func TestMultipleUncommittedWriteCommitThenRead(t *testing.T) {
	store, err := NewPersistantStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key := "key"
	value1 := []byte("value1")
	version, err := store.UncommittedWriteNewVersion(key, value1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
	value2 := []byte("value2")
	version, err = store.UncommittedWriteNewVersion(key, value2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), version)

	err = store.CommitVersion(key, version)
	require.NoError(t, err)
	readValue, err := store.CommittedRead(key)
	require.NoError(t, err)
	require.Equal(t, string(value2), string(readValue))
}

func TestInterleavedUncommittedWriteCommitsReads(t *testing.T) {
	store, err := NewPersistantStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key := "key"
	value1 := []byte("value1")
	version1, err := store.UncommittedWriteNewVersion(key, value1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version1)

	value2 := []byte("value2")
	version2, err := store.UncommittedWriteNewVersion(key, value2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), version2)

	err = store.CommitVersion(key, version1)
	require.NoError(t, err)
	_, err = store.CommittedRead(key)
	require.ErrorIs(t, err, ErrDirtyRead)

	value3 := []byte("value3")
	version3, err := store.UncommittedWriteNewVersion(key, value3)
	require.NoError(t, err)
	require.Equal(t, uint64(3), version3)

	_, err = store.CommittedRead(key)
	require.ErrorIs(t, err, ErrDirtyRead)

	err = store.CommitVersion(key, version3)
	require.NoError(t, err)
	readValue, err := store.CommittedRead(key)
	require.NoError(t, err)
	require.Equal(t, string(value3), string(readValue))
}

func TestSendKeyValuePairsCommitted(t *testing.T) {
	store, err := NewPersistantStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key1 := "key1"
	value1 := []byte("value1")
	key2 := "key2"
	value2 := []byte("value2")
	key3 := "key3"
	value3 := []byte("value3")
	key4 := "key4"
	value4 := []byte("value4")

	version1, err := store.CommittedWriteNewVersion(key1, value1)
	require.NoError(t, err)
	version2, err := store.CommittedWriteNewVersion(key2, value2)
	require.NoError(t, err)
	version3, err := store.CommittedWriteNewVersion(key3, value3)
	require.NoError(t, err)

	// Ensure the committed key filter does not include dirty keys.
	_, err = store.UncommittedWriteNewVersion(key4, value4)
	require.NoError(t, err)
	value5 := []byte("value5")
	_, err = store.UncommittedWriteNewVersion(key3, value5)
	require.NoError(t, err)

	sentKvPairs := []KeyValuePair{}
	sendFunc := func(ctx context.Context, kvPairs []KeyValuePair) error {
		sentKvPairs = append(sentKvPairs, kvPairs...)
		return nil
	}

	require.NoError(t, store.SendKeys(context.TODO(), sendFunc, CommittedKeys))
	require.Len(t, sentKvPairs, 3)
	keys := []string{key1, key2, key3}
	values := [][]byte{value1, value2, value3}
	versions := []uint64{version1, version2, version3}
	for i := 0; i < len(sentKvPairs); i++ {
		kvPair := sentKvPairs[i]
		require.Equal(t, keys[i], kvPair.Key)
		require.Equal(t, values[i], kvPair.Value)
		require.Equal(t, versions[i], kvPair.Version)
		require.True(t, kvPair.Committed)
	}
}

func TestSendKeyValuePairsDirty(t *testing.T) {
	store, err := NewPersistantStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key1 := "key1"
	value1 := []byte("value1")
	value2 := []byte("value2")
	value3 := []byte("value3")
	version1, err := store.UncommittedWriteNewVersion(key1, value1)
	require.NoError(t, err)
	version2, err := store.UncommittedWriteNewVersion(key1, value2)
	require.NoError(t, err)
	version3, err := store.UncommittedWriteNewVersion(key1, value3)
	require.NoError(t, err)

	// Ensure the dirty key filter does not include committed keys.
	key2 := "key2"
	value4 := []byte("value4")
	_, err = store.CommittedWriteNewVersion(key2, value4)
	require.NoError(t, err)

	sentKvPairs := []KeyValuePair{}
	sendFunc := func(ctx context.Context, kvPairs []KeyValuePair) error {
		sentKvPairs = append(sentKvPairs, kvPairs...)
		return nil
	}

	require.NoError(t, store.SendKeys(context.TODO(), sendFunc, DirtyKeys))
	require.Len(t, sentKvPairs, 3)
	values := [][]byte{value1, value2, value3}
	versions := []uint64{version1, version2, version3}
	for i := 0; i < len(sentKvPairs); i++ {
		kvPair := sentKvPairs[i]
		require.Equal(t, key1, kvPair.Key)
		require.Equal(t, values[i], kvPair.Value)
		require.Equal(t, versions[i], kvPair.Version)
		require.False(t, kvPair.Committed)
	}
}

func TestSendKeyValuePairsAll(t *testing.T) {
	store, err := NewPersistantStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key1 := "key1"
	value1 := []byte("value1")
	key2 := "key2"
	value2 := []byte("value2")
	key3 := "key3"
	value3 := []byte("value3")
	key4 := "key4"
	value4 := []byte("value4")

	version1, err := store.CommittedWriteNewVersion(key1, value1)
	require.NoError(t, err)
	version2, err := store.CommittedWriteNewVersion(key2, value2)
	require.NoError(t, err)
	version3, err := store.CommittedWriteNewVersion(key3, value3)
	require.NoError(t, err)
	version4, err := store.UncommittedWriteNewVersion(key4, value4)
	require.NoError(t, err)

	sentKvPairs := []KeyValuePair{}
	sendFunc := func(ctx context.Context, kvPairs []KeyValuePair) error {
		sentKvPairs = append(sentKvPairs, kvPairs...)
		return nil
	}

	require.NoError(t, store.SendKeys(context.TODO(), sendFunc, AllKeys))
	require.Len(t, sentKvPairs, 4)
	keys := []string{key1, key2, key3, key4}
	values := [][]byte{value1, value2, value3, value4}
	versions := []uint64{version1, version2, version3, version4}
	committed := []bool{true, true, true, false}
	for i := 0; i < len(sentKvPairs); i++ {
		kvPair := sentKvPairs[i]
		require.Equal(t, keys[i], kvPair.Key)
		require.Equal(t, values[i], kvPair.Value)
		require.Equal(t, versions[i], kvPair.Version)
		require.Equal(t, committed[i], kvPair.Committed)
	}
}
