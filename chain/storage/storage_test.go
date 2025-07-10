package storage

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSingleUncommittedWriteThenRead(t *testing.T) {
	store, err := NewPersistantStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	// Perform a single uncommitted write for a key.
	key := "key"
	value := []byte("value")
	version, err := store.UncommittedWriteNewVersion(key, value)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)

	// A committed read should fail since there are writes that have not been committed.
	_, err = store.CommittedRead(key)
	require.ErrorIs(t, err, ErrDirtyRead)
}

func TestMultipleUncommittedWriteThenRead(t *testing.T) {
	store, err := NewPersistantStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	// Perform a multiple uncommitted writes for the same key.
	key := "key"
	value1 := []byte("value1")
	version, err := store.UncommittedWriteNewVersion(key, value1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
	value2 := []byte("value2")
	version, err = store.UncommittedWriteNewVersion(key, value2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), version)

	// A committed read should fail since there are writes that have not been committed.
	_, err = store.CommittedRead(key)
	require.ErrorIs(t, err, ErrDirtyRead)
}

func TestUncommittedWriteCommitThenRead(t *testing.T) {
	store, err := NewPersistantStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	// Perform a single uncommitted write for a key.
	key := "key"
	value := []byte("value")
	version, err := store.UncommittedWriteNewVersion(key, value)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)

	// Commit the latest version. Since all writes for the key are committed, the read should succeed.
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

	// Perform two uncommitted writes with different values to the same key.
	key := "key"
	value1 := []byte("value1")
	version, err := store.UncommittedWriteNewVersion(key, value1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
	value2 := []byte("value2")
	version, err = store.UncommittedWriteNewVersion(key, value2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), version)

	// Commit the latest version. Since all writes for the key are committed, the read should succeed.
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

	// Perform an uncommitted write for the key.
	key := "key"
	value1 := []byte("value1")
	version1, err := store.UncommittedWriteNewVersion(key, value1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version1)

	// Perform another uncommitted write on the same key.
	value2 := []byte("value2")
	version2, err := store.UncommittedWriteNewVersion(key, value2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), version2)

	// Commit the first write. A committed read should still fail following this commit because the
	// second write has yet to be committed.
	err = store.CommitVersion(key, version1)
	require.NoError(t, err)
	_, err = store.CommittedRead(key)
	require.ErrorIs(t, err, ErrDirtyRead)

	// Perform another uncommitted write on the same key.
	value3 := []byte("value3")
	version3, err := store.UncommittedWriteNewVersion(key, value3)
	require.NoError(t, err)
	require.Equal(t, uint64(3), version3)

	// A committed read should still fail since the second and third writes have yet to be committed.
	_, err = store.CommittedRead(key)
	require.ErrorIs(t, err, ErrDirtyRead)

	// Commit the last write. With all writes committed, a committed read should succeed.
	err = store.CommitVersion(key, version3)
	require.NoError(t, err)
	readValue, err := store.CommittedRead(key)
	require.NoError(t, err)
	require.Equal(t, string(value3), string(readValue))
}

func TestSendKeys(t *testing.T) {
	store, err := NewPersistantStorage(t.TempDir())
	require.NoError(t, err)
	defer store.Close()

	// Perform some uncommitted and committed writes.
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
	send := func(keys []string) error {
		actualKeys = append(actualKeys, keys...)
		return nil
	}

	// Check that all keys are listed correctly.
	err = store.SendKeys(send, AllKeys)
	require.NoError(t, err)
	slices.Sort(actualKeys)
	slices.Sort(expectedAllKeys)
	require.Equal(t, expectedAllKeys, actualKeys)

	// Check that committed keys are listed correctly.
	actualKeys = []string{}
	err = store.SendKeys(send, CommittedKeys)
	require.NoError(t, err)
	slices.Sort(actualKeys)
	slices.Sort(expectedCommittedKeys)
	require.Equal(t, expectedCommittedKeys, actualKeys)

	// Check that dirty keys are listed correctly.
	actualKeys = []string{}
	err = store.SendKeys(send, DirtyKeys)
	require.NoError(t, err)
	slices.Sort(actualKeys)
	slices.Sort(expectedDirtyKeys)
	require.Equal(t, expectedDirtyKeys, actualKeys)
}
