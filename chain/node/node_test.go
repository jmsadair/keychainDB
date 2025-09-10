package node

import (
	"context"
	"io"
	"testing"

	"github.com/jmsadair/keychain/chain/storage"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockKeyValueStreamReader struct {
	mock.Mock
}

func (mkvsr *mockKeyValueStreamReader) Receive() (*storage.KeyValuePair, error) {
	args := mkvsr.MethodCalled("Receive")
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*storage.KeyValuePair), args.Error(1)
}

type mockTransport struct {
	mock.Mock
}

func (mc *mockTransport) Write(ctx context.Context, address string, key string, value []byte, version uint64) error {
	args := mc.MethodCalled("Write", address, key, value, version)
	return args.Error(0)
}

func (mc *mockTransport) Read(ctx context.Context, address string, key string) ([]byte, error) {
	args := mc.MethodCalled("Read", address, key)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (mc *mockTransport) Commit(ctx context.Context, address string, key string, version uint64) error {
	args := mc.MethodCalled("Commit", address, key, version)
	return args.Error(0)
}

func (mc *mockTransport) Propagate(ctx context.Context, address string, keyFilter storage.KeyFilter) (KeyValueReceiveStream, error) {
	args := mc.MethodCalled("Propagate", address, keyFilter)
	return args.Get(0).(KeyValueReceiveStream), args.Error(1)
}

type mockStorage struct {
	mock.Mock
}

func (ms *mockStorage) CommittedWrite(key string, value []byte, version uint64) error {
	args := ms.MethodCalled("CommittedWrite", key, value, version)
	return args.Error(0)
}

func (ms *mockStorage) CommittedWriteNewVersion(key string, value []byte) (uint64, error) {
	args := ms.MethodCalled("CommittedWriteNewVersion", key, value)
	return args.Get(0).(uint64), args.Error(1)
}

func (ms *mockStorage) CommittedRead(key string) ([]byte, error) {
	args := ms.MethodCalled("CommittedRead", key)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (ms *mockStorage) CommitVersion(key string, version uint64) error {
	args := ms.MethodCalled("CommitVersion", key, version)
	return args.Error(0)
}

func (ms *mockStorage) UncommittedWrite(key string, value []byte, version uint64) error {
	args := ms.MethodCalled("UncommittedWrite", key, value, version)
	return args.Error(0)
}

func (ms *mockStorage) UncommittedWriteNewVersion(key string, value []byte) (uint64, error) {
	args := ms.MethodCalled("UncommittedWriteNewVersion", key, value)
	return args.Get(0).(uint64), args.Error(1)
}

func (ms *mockStorage) SendKeyValuePairs(ctx context.Context, sendFunc func(context.Context, []storage.KeyValuePair) error, keyFilter storage.KeyFilter) error {
	args := ms.MethodCalled("SendKeyValuePairs", sendFunc, keyFilter)
	return args.Error(0)
}

func (ms *mockStorage) CommitAll(ctx context.Context, onCommit func(ctx context.Context, key string, version uint64) error) error {
	args := ms.MethodCalled("CommitAll")
	return args.Error(0)
}

func TestInitiateReplicatedWrite(t *testing.T) {
	address1 := "127.0.0.1:8080"
	address2 := "127.0.0.2:8080"

	transport := new(mockTransport)
	store := new(mockStorage)
	node := NewChainNode("member-1", address1, store, transport)

	key := "key"
	value := []byte("value")
	version := uint64(1)

	err := node.InitiateReplicatedWrite(context.TODO(), key, value)
	require.ErrorIs(t, err, ErrNotMemberOfChain)

	member1 := &ChainMember{ID: "member-1", Address: address1}
	config, err := NewConfiguration([]*ChainMember{member1})
	require.NoError(t, err)
	node.state.Load().Config = config
	store.On("CommittedWriteNewVersion", key, value).Return(version, nil).Once()
	err = node.InitiateReplicatedWrite(context.TODO(), key, value)
	require.NoError(t, err)
	store.AssertExpectations(t)

	member2 := &ChainMember{ID: "member-2", Address: address2}
	config, err = NewConfiguration([]*ChainMember{member1, member2})
	require.NoError(t, err)
	node.state.Load().Config = config
	store.On("UncommittedWriteNewVersion", key, value).Return(version, nil).Once()
	transport.On("Write", address2, key, value, version).Return(nil).Once()
	err = node.InitiateReplicatedWrite(context.TODO(), key, value)
	require.NoError(t, err)
	store.AssertExpectations(t)
	transport.AssertExpectations(t)

	config, err = NewConfiguration([]*ChainMember{member2, member1})
	require.NoError(t, err)
	node.state.Load().Config = config
	err = node.InitiateReplicatedWrite(context.TODO(), key, value)
	require.ErrorIs(t, err, ErrNotHead)
}

func TestWriteWithVersion(t *testing.T) {
	address1 := "127.0.0.1:8080"
	address2 := "127.0.0.2:8080"
	address3 := "127.0.0.3:8080"

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode("member-1", address1, store, transport)

	key := "key"
	value := []byte("value")
	version := uint64(1)

	err := node.WriteWithVersion(context.TODO(), key, value, version)
	require.ErrorIs(t, err, ErrNotMemberOfChain)

	member1 := &ChainMember{ID: "member-1", Address: address1}
	member2 := &ChainMember{ID: "member-2", Address: address2}
	member3 := &ChainMember{ID: "member-3", Address: address3}

	config, err := NewConfiguration([]*ChainMember{member2, member1})
	require.NoError(t, err)
	node.state.Load().Config = config
	store.On("CommittedWrite", key, value, version).Return(nil).Once()
	err = node.WriteWithVersion(context.TODO(), key, value, version)
	require.NoError(t, err)
	store.AssertExpectations(t)

	config, err = NewConfiguration([]*ChainMember{member2, member1, member3})
	require.NoError(t, err)
	node.state.Load().Config = config
	store.On("UncommittedWrite", key, value, version).Return(nil).Once()
	transport.On("Write", address3, key, value, version).Return(nil).Once()
	err = node.WriteWithVersion(context.TODO(), key, value, version)
	require.NoError(t, err)
	store.AssertExpectations(t)
	transport.AssertExpectations(t)
}

func TestRead(t *testing.T) {
	address1 := "127.0.0.1:8080"
	address2 := "127.0.0.2:8080"
	address3 := "127.0.0.3:8080"

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode("member-1", address1, store, transport)

	key := "key"
	value := []byte("value")

	_, err := node.Read(context.TODO(), key)
	require.ErrorIs(t, err, ErrNotMemberOfChain)

	member1 := &ChainMember{ID: "member-1", Address: address1}
	member2 := &ChainMember{ID: "member-2", Address: address2}
	member3 := &ChainMember{ID: "member-3", Address: address3}

	config, err := NewConfiguration([]*ChainMember{member1, member2, member3})
	require.NoError(t, err)
	node.state.Load().Config = config
	store.On("CommittedRead", key).Return(value, nil).Once()
	readValue, err := node.Read(context.TODO(), key)
	require.NoError(t, err)
	require.Equal(t, value, readValue)
	store.AssertExpectations(t)

	store.On("CommittedRead", key).Return(nil, storage.ErrDirtyRead).Once()
	transport.On("Read", address3, key).Return(value, nil).Once()
	readValue, err = node.Read(context.TODO(), key)
	require.NoError(t, err)
	require.Equal(t, value, readValue)
	store.AssertExpectations(t)
	transport.AssertExpectations(t)
}

func TestCommit(t *testing.T) {
	address1 := "127.0.0.1:8080"

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode("member-1", address1, store, transport)

	key := "key"
	version := uint64(1)

	err := node.Commit(context.TODO(), key, version)
	require.ErrorIs(t, err, ErrNotMemberOfChain)

	member1 := &ChainMember{ID: "member-1", Address: address1}
	config, err := NewConfiguration([]*ChainMember{member1})
	require.NoError(t, err)
	node.state.Load().Config = config
	store.On("CommitVersion", key, version).Return(nil).Once()
	err = node.Commit(context.TODO(), key, version)
	store.AssertExpectations(t)
	require.NoError(t, err)
	require.Len(t, node.onCommitCh, 1)
	commitMsg := <-node.onCommitCh
	require.Equal(t, key, commitMsg.key)
	require.Equal(t, version, commitMsg.version)
}

func TestRequestPropagation(t *testing.T) {
	address1 := "127.0.0.1:8080"
	address2 := "127.0.0.2:8080"

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode("member-1", address1, store, transport)

	stream := new(mockKeyValueStreamReader)
	kv1 := storage.KeyValuePair{Key: "key-1", Value: []byte("value-1"), Committed: false}
	kv2 := storage.KeyValuePair{Key: "key-2", Value: []byte("value-2"), Committed: true}

	stream.On("Receive").Return(&kv1, nil).Once()
	stream.On("Receive").Return(&kv2, nil).Once()
	stream.On("Receive").Return(nil, io.EOF).Once()
	transport.On("Propagate", address2, storage.AllKeys).Return(stream, nil).Once()
	store.On("UncommittedWrite", kv1.Key, kv1.Value, kv1.Version).Return(nil).Once()
	store.On("CommittedWrite", kv2.Key, kv2.Value, kv2.Version).Return(nil).Once()
	require.NoError(t, node.requestPropagation(context.TODO(), address2, storage.AllKeys, false))
	stream.AssertExpectations(t)
	transport.AssertExpectations(t)
	store.AssertExpectations(t)

	transport.On("Propagate", address2, storage.AllKeys).Return(stream, nil).Once()
	stream.On("Receive").Return(&kv1, nil).Once()
	stream.On("Receive").Return(&kv2, nil).Once()
	stream.On("Receive").Return(nil, io.EOF).Once()
	store.On("CommittedWrite", kv1.Key, kv1.Value, kv1.Version).Return(nil).Once()
	store.On("CommittedWrite", kv2.Key, kv2.Value, kv2.Version).Return(nil).Once()
	require.NoError(t, node.requestPropagation(context.TODO(), address2, storage.AllKeys, true))
	stream.AssertExpectations(t)
	transport.AssertExpectations(t)
	store.AssertExpectations(t)
}

func TestOnNewPredecessor(t *testing.T) {
	address1 := "127.0.0.1:8080"
	address2 := "127.0.0.2:8080"
	address3 := "127.0.0.3:8080"

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode("member-1", address1, store, transport)

	stream := new(mockKeyValueStreamReader)
	kv1 := storage.KeyValuePair{Key: "key-1", Value: []byte("value-1"), Committed: false}
	kv2 := storage.KeyValuePair{Key: "key-2", Value: []byte("value-2"), Committed: true}

	// Node becomes new member of existing chain.
	// It should request all key-value pairs from its predecessor to sync and then signal syncing is complete.
	// It is the tail so it should commit any key-value pairs it receives immediately.
	member1 := &ChainMember{ID: "member-1", Address: address1}
	member2 := &ChainMember{ID: "member-2", Address: address2}
	member3 := &ChainMember{ID: "member-3", Address: address3}

	node.syncCompleteCh = make(chan any, 1)
	config, err := NewConfiguration([]*ChainMember{member2, member1})
	require.NoError(t, err)
	stream.On("Receive").Return(&kv1, nil).Once()
	stream.On("Receive").Return(&kv2, nil).Once()
	stream.On("Receive").Return(nil, io.EOF).Once()
	transport.On("Propagate", address2, storage.AllKeys).Return(stream, nil).Once()
	store.On("CommittedWrite", kv1.Key, kv1.Value, kv1.Version).Return(nil).Once()
	store.On("CommittedWrite", kv2.Key, kv2.Value, kv2.Version).Return(nil).Once()
	node.onNewPredecessor(context.TODO(), config, true)
	require.Len(t, node.syncCompleteCh, 1)
	node.syncCompleteCh = make(chan any)
	stream.AssertExpectations(t)
	transport.AssertExpectations(t)
	store.AssertExpectations(t)

	// Node is an existing member of the chain and gets a new predecessor.
	// It should request only the dirty key-value pairs from its predecessor in case its previous
	// predecessor failed and did not finish sending them. It is the tail so it should commit
	// any key-value pairs it receives immediately.
	config, err = NewConfiguration([]*ChainMember{member3, member1})
	require.NoError(t, err)
	stream.On("Receive").Return(&kv1, nil).Once()
	stream.On("Receive").Return(&kv2, nil).Once()
	stream.On("Receive").Return(nil, io.EOF).Once()
	transport.On("Propagate", address3, storage.DirtyKeys).Return(stream, nil).Once()
	store.On("CommittedWrite", kv1.Key, kv1.Value, kv1.Version).Return(nil).Once()
	store.On("CommittedWrite", kv2.Key, kv2.Value, kv2.Version).Return(nil).Once()
	node.onNewPredecessor(context.TODO(), config, false)
	stream.AssertExpectations(t)
	transport.AssertExpectations(t)
	store.AssertExpectations(t)
}

func TestOnNewSuccessor(t *testing.T) {
	address1 := "127.0.0.1:8080"
	address2 := "127.0.0.2:8080"
	address3 := "127.0.0.3:8080"

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode("member-1", address1, store, transport)

	stream := new(mockKeyValueStreamReader)
	kv1 := storage.KeyValuePair{Key: "key-1", Value: []byte("value-1"), Committed: false}
	kv2 := storage.KeyValuePair{Key: "key-2", Value: []byte("value-2"), Committed: true}

	// Node becomes new member of existing chain.
	// It should request all key-value pairs from its successor to sync and then signal syncing is complete.
	// It is not the tail, so it should only commit key-value pairs that it knows are committed.
	member1 := &ChainMember{ID: "member-1", Address: address1}
	member2 := &ChainMember{ID: "member-2", Address: address2}
	member3 := &ChainMember{ID: "member-3", Address: address3}

	node.syncCompleteCh = make(chan any, 1)
	config, err := NewConfiguration([]*ChainMember{member1, member2})
	require.NoError(t, err)
	stream.On("Receive").Return(&kv1, nil).Once()
	stream.On("Receive").Return(&kv2, nil).Once()
	stream.On("Receive").Return(nil, io.EOF).Once()
	transport.On("Propagate", address2, storage.AllKeys).Return(stream, nil).Once()
	store.On("UncommittedWrite", kv1.Key, kv1.Value, kv1.Version).Return(nil).Once()
	store.On("CommittedWrite", kv2.Key, kv2.Value, kv2.Version).Return(nil).Once()
	node.onNewSuccessor(context.TODO(), config, true)
	require.Len(t, node.syncCompleteCh, 1)
	node.syncCompleteCh = make(chan any)
	stream.AssertExpectations(t)
	transport.AssertExpectations(t)
	store.AssertExpectations(t)

	// Node is an existing member of the chain and gets a new successor.
	// It should request only the committed key-value pairs from its predecessor in case its previous
	// successor failed without sending an acknowledgement of the commit.
	config, err = NewConfiguration([]*ChainMember{member1, member3})
	require.NoError(t, err)
	stream.On("Receive").Return(&kv2, nil).Once()
	stream.On("Receive").Return(nil, io.EOF).Once()
	transport.On("Propagate", address3, storage.CommittedKeys).Return(stream, nil).Once()
	store.On("CommittedWrite", kv2.Key, kv2.Value, kv2.Version).Return(nil).Once()
	node.onNewSuccessor(context.TODO(), config, false)
	stream.AssertExpectations(t)
	transport.AssertExpectations(t)
	store.AssertExpectations(t)

	// Node is an existing member of the chain and becomes the new tail.
	// It should immediately commit all of its dirty key-value pairs.
	config, err = NewConfiguration([]*ChainMember{member1})
	require.NoError(t, err)
	store.On("CommitAll").Return(nil).Once()
	node.onNewSuccessor(context.TODO(), config, false)
	stream.AssertExpectations(t)
	transport.AssertExpectations(t)
	store.AssertExpectations(t)
}
