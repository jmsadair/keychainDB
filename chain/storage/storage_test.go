package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommitThenRead(t *testing.T) {
	store, err := NewPersistentStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key := "key-1"
	value := []byte("value-1")
	version, err := store.CommittedWriteNewVersion(key, value)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)

	readValue, err := store.CommittedRead(key)
	require.NoError(t, err)
	require.Equal(t, value, readValue)
}

func TestSingleUncommittedWriteThenRead(t *testing.T) {
	store, err := NewPersistentStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key := "key-1"
	value := []byte("value-1")
	version, err := store.UncommittedWriteNewVersion(key, value)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)

	_, err = store.CommittedRead(key)
	require.ErrorIs(t, err, ErrUncommittedRead)
}

func TestMultipleUncommittedWriteThenRead(t *testing.T) {
	store, err := NewPersistentStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key := "key-1"
	value1 := []byte("value-1")
	version, err := store.UncommittedWriteNewVersion(key, value1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
	value2 := []byte("value-2")
	version, err = store.UncommittedWriteNewVersion(key, value2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), version)

	_, err = store.CommittedRead(key)
	require.ErrorIs(t, err, ErrUncommittedRead)
}

func TestUncommittedWriteCommitThenRead(t *testing.T) {
	store, err := NewPersistentStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key := "key-1"
	value := []byte("value-1")
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
	store, err := NewPersistentStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key := "key-1"
	value1 := []byte("value-1")
	version, err := store.UncommittedWriteNewVersion(key, value1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
	value2 := []byte("value-2")
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
	store, err := NewPersistentStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key := "key-1"
	value1 := []byte("value-1")
	version1, err := store.UncommittedWriteNewVersion(key, value1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version1)

	value2 := []byte("value-2")
	version2, err := store.UncommittedWriteNewVersion(key, value2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), version2)

	err = store.CommitVersion(key, version1)
	require.NoError(t, err)
	_, err = store.CommittedRead(key)
	require.ErrorIs(t, err, ErrUncommittedRead)

	value3 := []byte("value-3")
	version3, err := store.UncommittedWriteNewVersion(key, value3)
	require.NoError(t, err)
	require.Equal(t, uint64(3), version3)

	_, err = store.CommittedRead(key)
	require.ErrorIs(t, err, ErrUncommittedRead)

	err = store.CommitVersion(key, version3)
	require.NoError(t, err)
	readValue, err := store.CommittedRead(key)
	require.NoError(t, err)
	require.Equal(t, string(value3), string(readValue))
}

func TestSendKeyValuePairsCommitted(t *testing.T) {
	store, err := NewPersistentStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key1 := "key-1"
	value1 := []byte("value-1")
	key2 := "key-2"
	value2 := []byte("value-2")
	key3 := "key-3"
	value3 := []byte("value-3")
	key4 := "key-4"
	value4 := []byte("value-4")

	version1, err := store.CommittedWriteNewVersion(key1, value1)
	require.NoError(t, err)
	version2, err := store.CommittedWriteNewVersion(key2, value2)
	require.NoError(t, err)
	version3, err := store.CommittedWriteNewVersion(key3, value3)
	require.NoError(t, err)

	// Ensure the committed key filter does not include dirty keys.
	_, err = store.UncommittedWriteNewVersion(key4, value4)
	require.NoError(t, err)
	value5 := []byte("value-5")
	_, err = store.UncommittedWriteNewVersion(key3, value5)
	require.NoError(t, err)

	sentKvPairs := []KeyValuePair{}
	sendFunc := func(ctx context.Context, kvPairs []KeyValuePair) error {
		sentKvPairs = append(sentKvPairs, kvPairs...)
		return nil
	}

	require.NoError(t, store.SendKeyValuePairs(context.TODO(), sendFunc, CommittedKeys))
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
	store, err := NewPersistentStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key1 := "key-1"
	value1 := []byte("value-1")
	value2 := []byte("value-2")
	value3 := []byte("value-3")
	version1, err := store.UncommittedWriteNewVersion(key1, value1)
	require.NoError(t, err)
	version2, err := store.UncommittedWriteNewVersion(key1, value2)
	require.NoError(t, err)
	version3, err := store.UncommittedWriteNewVersion(key1, value3)
	require.NoError(t, err)

	// Ensure the dirty key filter does not include committed keys.
	key2 := "key-2"
	value4 := []byte("value-4")
	_, err = store.CommittedWriteNewVersion(key2, value4)
	require.NoError(t, err)

	sentKvPairs := []KeyValuePair{}
	sendFunc := func(ctx context.Context, kvPairs []KeyValuePair) error {
		sentKvPairs = append(sentKvPairs, kvPairs...)
		return nil
	}

	require.NoError(t, store.SendKeyValuePairs(context.TODO(), sendFunc, DirtyKeys))
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
	store, err := NewPersistentStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	key1 := "key-1"
	value1 := []byte("value-1")
	key2 := "key-2"
	value2 := []byte("value-2")
	key3 := "key-3"
	value3 := []byte("value-3")
	key4 := "key-4"
	value4 := []byte("value-4")

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

	require.NoError(t, store.SendKeyValuePairs(context.TODO(), sendFunc, AllKeys))
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

func TestCommitAll(t *testing.T) {
	store, err := NewPersistentStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	// Add multiple key-value pairs to storage, but do not commit any of them.
	numKvPairs := 100
	numVersions := 3
	expectedKvPairs := make(map[string][]byte, numKvPairs)
	for i := range numKvPairs {
		key := fmt.Sprintf("key-%d", i)
		var value []byte
		for j := 1; j <= numVersions; j++ {
			value = fmt.Appendf(nil, "value-%d-%d", i, j)
			store.UncommittedWrite(key, value, uint64(j))
		}
		expectedKvPairs[key] = value
	}

	// Commit all keys.
	onCommit := func(ctx context.Context, key string, version uint64) error {
		return nil
	}
	require.NoError(t, store.CommitAll(context.TODO(), onCommit))

	// Ensure that the only the latest version of the committed keys exists.
	actualKvPairs := make(map[string][]byte, numKvPairs)
	recordCommittedKeys := func(ctx context.Context, kvPairs []KeyValuePair) error {
		for _, kvPair := range kvPairs {
			if !kvPair.Committed {
				return errors.New("all keys should have been committed")
			}
			actualKvPairs[kvPair.Key] = kvPair.Value
		}
		return nil
	}
	require.NoError(t, store.SendKeyValuePairs(context.TODO(), recordCommittedKeys, AllKeys))
	require.Len(t, actualKvPairs, len(expectedKvPairs))
	for expectedKey, expectedValue := range expectedKvPairs {
		actualValue, ok := actualKvPairs[expectedKey]
		require.True(t, ok)
		require.Equal(t, expectedValue, actualValue)
	}
}
