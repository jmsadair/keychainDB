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

func (m *mockKeyValueStreamReader) Receive() (*storage.KeyValuePair, error) {
	args := m.MethodCalled("Receive")
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*storage.KeyValuePair), args.Error(1)
}

type mockTransport struct {
	mock.Mock
}

func (m *mockTransport) Write(ctx context.Context, address string, request *WriteRequest, response *WriteResponse) error {
	args := m.MethodCalled("Write", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		*response = *resp.(*WriteResponse)
	}
	return args.Error(1)
}

func (m *mockTransport) Read(ctx context.Context, address string, request *ReadRequest, response *ReadResponse) error {
	args := m.MethodCalled("Read", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		*response = *resp.(*ReadResponse)
	}
	return args.Error(1)
}

func (m *mockTransport) Commit(ctx context.Context, address string, request *CommitRequest, response *CommitResponse) error {
	args := m.MethodCalled("Commit", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		*response = *resp.(*CommitResponse)
	}
	return args.Error(1)
}

func (m *mockTransport) Propagate(ctx context.Context, address string, request *PropagateRequest) (KeyValueReceiveStream, error) {
	args := m.MethodCalled("Propagate", ctx, address, request)
	return args.Get(0).(KeyValueReceiveStream), args.Error(1)
}

type mockStorage struct {
	mock.Mock
}

func (m *mockStorage) CommittedWrite(key string, value []byte, version uint64) error {
	return m.MethodCalled("CommittedWrite", key, value, version).Error(0)
}

func (m *mockStorage) CommittedWriteNewVersion(key string, value []byte) (uint64, error) {
	args := m.MethodCalled("CommittedWriteNewVersion", key, value)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *mockStorage) CommittedRead(key string) ([]byte, error) {
	args := m.MethodCalled("CommittedRead", key)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (m *mockStorage) CommitVersion(key string, version uint64) error {
	return m.MethodCalled("CommitVersion", key, version).Error(0)
}

func (m *mockStorage) UncommittedWrite(key string, value []byte, version uint64) error {
	return m.MethodCalled("UncommittedWrite", key, value, version).Error(0)
}

func (m *mockStorage) UncommittedWriteNewVersion(key string, value []byte) (uint64, error) {
	args := m.MethodCalled("UncommittedWriteNewVersion", key, value)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *mockStorage) SendKeyValuePairs(ctx context.Context, sendFunc func(context.Context, []storage.KeyValuePair) error, keyFilter storage.KeyFilter) error {
	return m.MethodCalled("SendKeyValuePairs", ctx, sendFunc, keyFilter).Error(0)
}

func (m *mockStorage) CommitAll(ctx context.Context, onCommit func(ctx context.Context, key string, version uint64) error) error {
	return m.MethodCalled("CommitAll", ctx, onCommit).Error(0)
}

func TestInitiateReplicatedWrite(t *testing.T) {
	member1 := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	member2 := &ChainMember{ID: "member-2", Address: "127.0.0.2:8080"}

	transport := new(mockTransport)
	store := new(mockStorage)
	node := NewChainNode(member1.ID, member1.Address, store, transport)

	// Node is not a member of a chain.
	key := "key"
	value := []byte("value")
	version := uint64(1)
	err := node.InitiateReplicatedWrite(context.Background(), key, value, 0)
	require.ErrorIs(t, err, ErrNotMemberOfChain)

	// Node is the the sole member of the chain - no forwarding is necessary.
	config := NewConfiguration([]*ChainMember{member1}, 0)
	node.state.Load().Config = config
	store.On("CommittedWriteNewVersion", key, value).Return(version, nil).Once()
	err = node.InitiateReplicatedWrite(context.Background(), key, value, 0)
	require.NoError(t, err)
	store.AssertExpectations(t)

	// Node is head of a chain with multiple members - forwarding is necessary.
	config = NewConfiguration([]*ChainMember{member1, member2}, 0)
	node.state.Load().Config = config
	store.On("UncommittedWriteNewVersion", key, value).Return(version, nil).Once()
	transport.On("Write", mock.Anything, member2.Address, &WriteRequest{
		Key:     key,
		Value:   value,
		Version: version,
	}).Return(&WriteResponse{}, nil).Once()
	err = node.InitiateReplicatedWrite(context.Background(), key, value, 0)
	require.NoError(t, err)
	store.AssertExpectations(t)
	transport.AssertExpectations(t)

	// Node is not the head.
	config = NewConfiguration([]*ChainMember{member2, member1}, 0)
	node.state.Load().Config = config
	err = node.InitiateReplicatedWrite(context.Background(), key, value, 0)
	require.ErrorIs(t, err, ErrNotHead)

	// There is a mismatch between the node configuration version and the request configuration version.
	config = NewConfiguration([]*ChainMember{member1}, 1)
	node.state.Load().Config = config
	err = node.InitiateReplicatedWrite(context.Background(), key, value, 0)
	require.ErrorIs(t, err, ErrInvalidConfigVersion)
}

func TestWriteWithVersion(t *testing.T) {
	member1 := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	member2 := &ChainMember{ID: "member-2", Address: "127.0.0.2:8080"}
	member3 := &ChainMember{ID: "member-3", Address: "127.0.0.3:8080"}

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode(member1.ID, member1.Address, store, transport)

	// Node is not a member of a chain.
	key := "key"
	value := []byte("value")
	version := uint64(1)
	req := &WriteRequest{Key: key, Value: value, Version: version}
	err := node.WriteWithVersion(context.Background(), req, &WriteResponse{})
	require.ErrorIs(t, err, ErrNotMemberOfChain)

	// Node is the the tail of the chain. It should immediately commit the key-value pair.
	config := NewConfiguration([]*ChainMember{member2, member1}, 0)
	node.state.Load().Config = config
	store.On("CommittedWrite", key, value, version).Return(nil).Once()
	err = node.WriteWithVersion(context.Background(), req, &WriteResponse{})
	require.NoError(t, err)
	store.AssertExpectations(t)

	// Node is not he head of the chain. It should forward the write without committing.
	config = NewConfiguration([]*ChainMember{member2, member1, member3}, 0)
	node.state.Load().Config = config
	store.On("UncommittedWrite", key, value, version).Return(nil).Once()
	transport.On("Write", mock.Anything, member3.Address, &WriteRequest{
		Key:     key,
		Value:   value,
		Version: version,
	}).Return(&WriteResponse{}, nil).Once()
	err = node.WriteWithVersion(context.Background(), req, &WriteResponse{})
	require.NoError(t, err)
	store.AssertExpectations(t)
	transport.AssertExpectations(t)

	// There is a mismatch between the node configuration version and the request configuration version.
	config = NewConfiguration([]*ChainMember{member2, member1, member3}, 1)
	node.state.Load().Config = config
	err = node.WriteWithVersion(context.Background(), req, &WriteResponse{})
	require.ErrorIs(t, err, ErrInvalidConfigVersion)
}

func TestRead(t *testing.T) {
	member1 := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	member2 := &ChainMember{ID: "member-2", Address: "127.0.0.2:8080"}
	member3 := &ChainMember{ID: "member-3", Address: "127.0.0.3:8080"}

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode(member1.ID, member1.Address, store, transport)

	// Node is not a member of a chain.
	key := "key"
	value := []byte("value")
	req := &ReadRequest{Key: key}
	var resp ReadResponse
	err := node.Read(context.Background(), req, &resp)
	require.ErrorIs(t, err, ErrNotMemberOfChain)

	// Node is not the tail of the chain, but the key-value pair is committed.
	config := NewConfiguration([]*ChainMember{member1, member2, member3}, 0)
	node.state.Load().Config = config
	store.On("CommittedRead", key).Return(value, nil).Once()
	err = node.Read(context.Background(), req, &resp)
	require.NoError(t, err)
	require.Equal(t, value, resp.Value)
	store.AssertExpectations(t)

	// Node is not the tail of the chain and the key-value pair is dirty.
	// It should forward the read to the tail.
	store.On("CommittedRead", key).Return(nil, storage.ErrDirtyRead).Once()
	transport.On("Read", mock.Anything, member3.Address, req).Return(&ReadResponse{Value: value}, nil).Once()
	err = node.Read(context.Background(), req, &resp)
	require.NoError(t, err)
	require.Equal(t, value, resp.Value)
	store.AssertExpectations(t)
	transport.AssertExpectations(t)

	// There is a mismatch between the node configuration version and the request configuration version.
	config = NewConfiguration([]*ChainMember{member1, member2, member3}, 1)
	node.state.Load().Config = config
	err = node.Read(context.Background(), req, &resp)
	require.ErrorIs(t, err, ErrInvalidConfigVersion)
}

func TestCommit(t *testing.T) {
	member1 := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode(member1.ID, member1.Address, store, transport)

	// Node is not a member of a chain.
	key := "key"
	version := uint64(1)
	req := &CommitRequest{Key: key, Version: version}
	resp := &CommitResponse{}
	err := node.Commit(context.Background(), req, resp)
	require.ErrorIs(t, err, ErrNotMemberOfChain)

	// Once a key-value pair is committed, the node should notify the background routine
	// which will inform the predecessors of the commit.
	config := NewConfiguration([]*ChainMember{member1}, 0)
	node.state.Load().Config = config
	store.On("CommitVersion", key, version).Return(nil).Once()
	err = node.Commit(context.Background(), req, resp)
	require.NoError(t, err)
	store.AssertExpectations(t)
	require.Len(t, node.onCommitCh, 1)
	commitMsg := <-node.onCommitCh
	require.Equal(t, key, commitMsg.key)
	require.Equal(t, version, commitMsg.version)

	// There is a mismatch between the node configuration version and the request configuration version.
	config = NewConfiguration([]*ChainMember{member1}, 1)
	node.state.Load().Config = config
	err = node.Commit(context.Background(), req, resp)
	require.ErrorIs(t, err, ErrInvalidConfigVersion)
}

func TestRequestPropagation(t *testing.T) {
	member1 := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	member2 := &ChainMember{ID: "member-2", Address: "127.0.0.2:8080"}

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode(member1.ID, member1.Address, store, transport)

	stream := new(mockKeyValueStreamReader)
	kv1 := storage.KeyValuePair{Key: "key-1", Value: []byte("value-1"), Committed: false}
	kv2 := storage.KeyValuePair{Key: "key-2", Value: []byte("value-2"), Committed: true}

	// All keys are propagated. Dirty keys should not be committed since the isTail flag is false.
	stream.On("Receive").Return(&kv1, nil).Once()
	stream.On("Receive").Return(&kv2, nil).Once()
	stream.On("Receive").Return(nil, io.EOF).Once()
	transport.On("Propagate", mock.Anything, member2.Address, &PropagateRequest{KeyFilter: storage.AllKeys}).Return(stream, nil).Once()
	store.On("UncommittedWrite", kv1.Key, kv1.Value, kv1.Version).Return(nil).Once()
	store.On("CommittedWrite", kv2.Key, kv2.Value, kv2.Version).Return(nil).Once()
	require.NoError(t, node.requestPropagation(context.Background(), member2.Address, storage.AllKeys, false))
	stream.AssertExpectations(t)
	transport.AssertExpectations(t)
	store.AssertExpectations(t)

	// All keys are propagated. Dirty keys should be committed since the isTail flag is true.
	stream.On("Receive").Return(&kv1, nil).Once()
	stream.On("Receive").Return(&kv2, nil).Once()
	stream.On("Receive").Return(nil, io.EOF).Once()
	transport.On("Propagate", mock.Anything, member2.Address, &PropagateRequest{KeyFilter: storage.AllKeys}).Return(stream, nil).Once()
	store.On("CommittedWrite", kv1.Key, kv1.Value, kv1.Version).Return(nil).Once()
	store.On("CommittedWrite", kv2.Key, kv2.Value, kv2.Version).Return(nil).Once()
	require.NoError(t, node.requestPropagation(context.Background(), member2.Address, storage.AllKeys, true))
	stream.AssertExpectations(t)
	transport.AssertExpectations(t)
	store.AssertExpectations(t)
}

func TestOnNewPredecessor(t *testing.T) {
	member1 := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	member2 := &ChainMember{ID: "member-2", Address: "127.0.0.2:8080"}
	member3 := &ChainMember{ID: "member-3", Address: "127.0.0.3:8080"}

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode(member1.ID, member1.Address, store, transport)

	stream := new(mockKeyValueStreamReader)
	kv1 := storage.KeyValuePair{Key: "key-1", Value: []byte("value-1"), Committed: false}
	kv2 := storage.KeyValuePair{Key: "key-2", Value: []byte("value-2"), Committed: true}

	// Node becomes new member of existing chain.
	// It should request all key-value pairs from its predecessor to sync and then signal syncing is complete.
	// It is the tail so it should commit any key-value pairs it receives immediately.
	node.syncCompleteCh = make(chan any, 1)
	config := NewConfiguration([]*ChainMember{member2, member1}, 0)
	stream.On("Receive").Return(&kv1, nil).Once()
	stream.On("Receive").Return(&kv2, nil).Once()
	stream.On("Receive").Return(nil, io.EOF).Once()
	transport.On("Propagate", mock.Anything, member2.Address, &PropagateRequest{KeyFilter: storage.AllKeys}).Return(stream, nil).Once()
	store.On("CommittedWrite", kv1.Key, kv1.Value, kv1.Version).Return(nil).Once()
	store.On("CommittedWrite", kv2.Key, kv2.Value, kv2.Version).Return(nil).Once()
	node.onNewPredecessor(context.Background(), config, true)
	require.Len(t, node.syncCompleteCh, 1)
	node.syncCompleteCh = make(chan any)
	stream.AssertExpectations(t)
	transport.AssertExpectations(t)
	store.AssertExpectations(t)

	// Node is an existing member of the chain and gets a new predecessor.
	// It should request only the dirty key-value pairs from its predecessor in case its previous
	// predecessor failed and did not finish sending them. It is the tail so it should commit
	// any key-value pairs it receives immediately.
	config = NewConfiguration([]*ChainMember{member3, member1}, 0)
	stream.On("Receive").Return(&kv1, nil).Once()
	stream.On("Receive").Return(&kv2, nil).Once()
	stream.On("Receive").Return(nil, io.EOF).Once()
	transport.On("Propagate", mock.Anything, member3.Address, &PropagateRequest{KeyFilter: storage.DirtyKeys}).Return(stream, nil).Once()
	store.On("CommittedWrite", kv1.Key, kv1.Value, kv1.Version).Return(nil).Once()
	store.On("CommittedWrite", kv2.Key, kv2.Value, kv2.Version).Return(nil).Once()
	node.onNewPredecessor(context.Background(), config, false)
	stream.AssertExpectations(t)
	transport.AssertExpectations(t)
	store.AssertExpectations(t)
}

func TestOnNewSuccessor(t *testing.T) {
	member1 := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	member2 := &ChainMember{ID: "member-2", Address: "127.0.0.2:8080"}
	member3 := &ChainMember{ID: "member-3", Address: "127.0.0.3:8080"}

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode(member1.ID, member1.Address, store, transport)

	stream := new(mockKeyValueStreamReader)
	kv1 := storage.KeyValuePair{Key: "key-1", Value: []byte("value-1"), Committed: false}
	kv2 := storage.KeyValuePair{Key: "key-2", Value: []byte("value-2"), Committed: true}

	// Node becomes new member of existing chain.
	// It should request all key-value pairs from its successor to sync and then signal syncing is complete.
	// It is not the tail, so it should only commit key-value pairs that it knows are committed.
	node.syncCompleteCh = make(chan any, 1)
	config := NewConfiguration([]*ChainMember{member1, member2}, 0)
	stream.On("Receive").Return(&kv1, nil).Once()
	stream.On("Receive").Return(&kv2, nil).Once()
	stream.On("Receive").Return(nil, io.EOF).Once()
	transport.On("Propagate", mock.Anything, member2.Address, &PropagateRequest{KeyFilter: storage.AllKeys}).Return(stream, nil).Once()
	store.On("UncommittedWrite", kv1.Key, kv1.Value, kv1.Version).Return(nil).Once()
	store.On("CommittedWrite", kv2.Key, kv2.Value, kv2.Version).Return(nil).Once()
	node.onNewSuccessor(context.Background(), config, true)
	require.Len(t, node.syncCompleteCh, 1)
	node.syncCompleteCh = make(chan any)
	stream.AssertExpectations(t)
	transport.AssertExpectations(t)
	store.AssertExpectations(t)

	// Node is an existing member of the chain and gets a new successor.
	// It should request only the committed key-value pairs from its predecessor in case its previous
	// successor failed without sending an acknowledgement of the commit.
	config = NewConfiguration([]*ChainMember{member1, member3}, 0)
	stream.On("Receive").Return(&kv2, nil).Once()
	stream.On("Receive").Return(nil, io.EOF).Once()
	transport.On("Propagate", mock.Anything, member3.Address, &PropagateRequest{KeyFilter: storage.CommittedKeys}).Return(stream, nil).Once()
	store.On("CommittedWrite", kv2.Key, kv2.Value, kv2.Version).Return(nil).Once()
	node.onNewSuccessor(context.Background(), config, false)
	stream.AssertExpectations(t)
	transport.AssertExpectations(t)
	store.AssertExpectations(t)

	// Node is an existing member of the chain and becomes the new tail.
	// It should immediately commit all of its dirty key-value pairs.
	config = NewConfiguration([]*ChainMember{member1}, 0)
	store.On("CommitAll", mock.Anything, mock.Anything).Return(nil).Once()
	node.onNewSuccessor(context.Background(), config, false)
	store.AssertExpectations(t)
}

func TestPing(t *testing.T) {
	member1 := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	member2 := &ChainMember{ID: "member-2", Address: "127.0.0.2:8080"}
	member3 := &ChainMember{ID: "member-3", Address: "127.0.0.3:8080"}

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode(member1.ID, member1.Address, store, transport)

	version := uint64(3)
	status := Syncing
	config := NewConfiguration([]*ChainMember{member1, member2, member3}, version)
	node.state.Load().Config = config
	node.state.Load().Status = status

	req := &PingRequest{}
	var resp PingResponse
	node.Ping(req, &resp)
	require.Equal(t, version, resp.Version)
	require.Equal(t, Syncing, resp.Status)
}
