package storage

import (
	"context"
	"fmt"
	"slices"
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

func TestSendKeyValuePairs(t *testing.T) {
	store, err := NewPersistantStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	expectedDirtyKeys := make([]string, 25)
	expectedCommittedKeys := make([]string, 25)
	expectedAllKeys := make([]string, 50)
	version := uint64(1)
	for i := range 50 {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Appendf([]byte{}, "value%d", i)
		expectedAllKeys[i] = key
		if i < 25 {
			err := store.CommittedWrite(key, value, version)
			require.NoError(t, err)
			expectedCommittedKeys[i] = key
			continue
		}
		err := store.UncommittedWrite(key, value, version)
		require.NoError(t, err)
		expectedDirtyKeys[i-25] = key
	}

	var actualKeys []string
	send := func(ctx context.Context, kvPairs []KeyValuePair) error {
		for _, kvPair := range kvPairs {
			actualKeys = append(actualKeys, kvPair.Key)
		}
		return nil
	}

	err = store.SendKeys(context.TODO(), send, AllKeys)
	require.NoError(t, err)
	slices.Sort(actualKeys)
	slices.Sort(expectedAllKeys)
	require.Equal(t, expectedAllKeys, actualKeys)

	actualKeys = []string{}
	err = store.SendKeys(context.TODO(), send, CommittedKeys)
	require.NoError(t, err)
	slices.Sort(actualKeys)
	slices.Sort(expectedCommittedKeys)
	require.Equal(t, expectedCommittedKeys, actualKeys)

	actualKeys = []string{}
	err = store.SendKeys(context.TODO(), send, DirtyKeys)
	require.NoError(t, err)
	slices.Sort(actualKeys)
	slices.Sort(expectedDirtyKeys)
	require.Equal(t, expectedDirtyKeys, actualKeys)
}
