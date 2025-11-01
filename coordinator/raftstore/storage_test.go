package logstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

func TestStoreGetLog(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewPersistentStorage(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	storedLog := &raft.Log{}
	logToStore := &raft.Log{
		Index:      1,
		Term:       1,
		Type:       raft.LogCommand,
		Data:       []byte("data"),
		Extensions: []byte("extensions"),
		AppendedAt: time.Now().UTC(),
	}

	require.NoError(t, store.StoreLog(logToStore))
	require.NoError(t, store.GetLog(logToStore.Index, storedLog))
	require.NotNil(t, storedLog)
	require.Equal(t, *logToStore, *storedLog)
}

func TestSetGet(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewPersistentStorage(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	key := []byte("key")
	value := []byte("value")

	require.NoError(t, store.Set(key, value))
	storedValue, err := store.Get(key)
	require.NoError(t, err)
	require.Equal(t, value, storedValue)

	missingKey := []byte("missing")
	storedValue, err = store.Get(missingKey)
	require.NoError(t, err)
	require.Equal(t, []byte{}, storedValue)
}

func TestSetGetUint64(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewPersistentStorage(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	key := []byte("key")
	value := uint64(256)

	require.NoError(t, store.SetUint64(key, value))
	storedValue, err := store.GetUint64(key)
	require.NoError(t, err)
	require.Equal(t, value, storedValue)
}

func TestFirstIndexLastIndex(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewPersistentStorage(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	logToStore := &raft.Log{Index: 1, Term: 1}
	require.NoError(t, store.StoreLog(logToStore))
	firstIndex, err := store.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), firstIndex)
	lastIndex, err := store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), lastIndex)

	logToStore = &raft.Log{Index: 2, Term: 1}
	require.NoError(t, store.StoreLog(logToStore))
	firstIndex, err = store.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), firstIndex)
	lastIndex, err = store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), lastIndex)

	logToStore = &raft.Log{Index: 4, Term: 1}
	require.NoError(t, store.StoreLog(logToStore))
	firstIndex, err = store.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), firstIndex)
	lastIndex, err = store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(4), lastIndex)
}

func TestStoreGetLogs(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewPersistentStorage(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	numLogs := 100
	logsToStore := make([]*raft.Log, 0, numLogs)
	for i := range numLogs {
		log := &raft.Log{
			Index:      uint64(i),
			Term:       uint64(i),
			Data:       fmt.Appendf(nil, "data-%d", i),
			Type:       raft.LogCommand,
			Extensions: fmt.Appendf(nil, "extensions-%d", i),
			AppendedAt: time.Now().UTC(),
		}
		logsToStore = append(logsToStore, log)
	}

	require.NoError(t, store.StoreLogs(logsToStore))
	for i := range numLogs {
		logToStore := logsToStore[i]
		storedLog := &raft.Log{}
		require.NoError(t, store.GetLog(logToStore.Index, storedLog))
		require.Equal(t, *logToStore, *storedLog)
	}
}

func TestSetGetMultiple(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewPersistentStorage(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	numKeys := 100
	keyValuePairs := make(map[string][]byte, numKeys)
	for i := range numKeys {
		key := fmt.Appendf(nil, "key-%d", i)
		value := fmt.Appendf(nil, "value-%d", i)
		keyValuePairs[string(key)] = value
		require.NoError(t, store.Set(key, value))
	}

	for key, value := range keyValuePairs {
		storedValue, err := store.Get([]byte(key))
		require.NoError(t, err)
		require.Equal(t, value, storedValue)
	}
}

func TestSetGetUint64Multiple(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewPersistentStorage(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	numKeys := 100
	keyValuePairs := make(map[string]uint64, numKeys)
	for i := range numKeys {
		key := fmt.Appendf(nil, "key-%d", i)
		value := uint64(i)
		keyValuePairs[string(key)] = value
		require.NoError(t, store.SetUint64(key, value))
	}

	for key, value := range keyValuePairs {
		storedValue, err := store.GetUint64([]byte(key))
		require.NoError(t, err)
		require.Equal(t, value, storedValue)
	}
}

func TestDeleteRange(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewPersistentStorage(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	logToStore1 := &raft.Log{Index: 1, Term: 1, Data: []byte("data-1")}
	require.NoError(t, store.StoreLog(logToStore1))
	logToStore2 := &raft.Log{Index: 2, Term: 2, Data: []byte("data-2")}
	require.NoError(t, store.StoreLog(logToStore2))
	logToStore3 := &raft.Log{Index: 3, Term: 3, Data: []byte("data-3")}
	require.NoError(t, store.StoreLog(logToStore3))

	// Delete a single log.
	require.NoError(t, store.DeleteRange(logToStore3.Index, logToStore3.Index))
	storedLog := &raft.Log{}
	firstIndex, err := store.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, logToStore1.Index, firstIndex)
	lastIndex, err := store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, logToStore2.Index, lastIndex)
	require.NoError(t, store.GetLog(logToStore1.Index, storedLog))
	require.Equal(t, *logToStore1, *storedLog)
	require.NoError(t, store.GetLog(logToStore2.Index, storedLog))
	require.Equal(t, *logToStore2, *storedLog)
	require.Error(t, store.GetLog(logToStore3.Index, storedLog))

	// Delete multiple logs.
	require.NoError(t, store.DeleteRange(logToStore1.Index, logToStore2.Index))
	firstIndex, err = store.FirstIndex()
	require.NoError(t, err)
	require.Zero(t, firstIndex)
	lastIndex, err = store.LastIndex()
	require.NoError(t, err)
	require.Zero(t, lastIndex)
	require.Error(t, store.GetLog(logToStore1.Index, storedLog))
	require.Error(t, store.GetLog(logToStore2.Index, storedLog))
	require.Error(t, store.GetLog(logToStore3.Index, storedLog))

	logToStore4 := &raft.Log{Index: 8, Term: 4, Data: []byte("data-4")}
	require.NoError(t, store.StoreLog(logToStore4))
	logToStore5 := &raft.Log{Index: 17, Term: 5, Data: []byte("data-5")}
	require.NoError(t, store.StoreLog(logToStore5))
	logToStore6 := &raft.Log{Index: 26, Term: 6, Data: []byte("data-6")}
	require.NoError(t, store.StoreLog(logToStore6))
	logToStore7 := &raft.Log{Index: 29, Term: 6, Data: []byte("data-7")}
	require.NoError(t, store.StoreLog(logToStore7))

	// Delete non-contiguous range of logs.
	require.NoError(t, store.DeleteRange(logToStore5.Index-1, logToStore7.Index+1))
	firstIndex, err = store.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, logToStore4.Index, firstIndex)
	lastIndex, err = store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, logToStore4.Index, lastIndex)
	require.NoError(t, store.GetLog(logToStore4.Index, storedLog))
	require.Equal(t, *logToStore4, *storedLog)
	require.Error(t, store.GetLog(logToStore1.Index, storedLog))
	require.Error(t, store.GetLog(logToStore2.Index, storedLog))
	require.Error(t, store.GetLog(logToStore3.Index, storedLog))
	require.Error(t, store.GetLog(logToStore6.Index, storedLog))
	require.Error(t, store.GetLog(logToStore7.Index, storedLog))
}
