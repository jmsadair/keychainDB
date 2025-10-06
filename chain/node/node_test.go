package node

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/jmsadair/keychain/chain/storage"
	pb "github.com/jmsadair/keychain/proto/chain"
	storagepb "github.com/jmsadair/keychain/proto/storage"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockClientStream struct {
	grpc.ClientStream
	mock.Mock
}

func (m *mockClientStream) Recv() (*storagepb.KeyValuePair, error) {
	args := m.MethodCalled("Recv")
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*storagepb.KeyValuePair), args.Error(1)
}

type mockTransport struct {
	mock.Mock
}

func (m *mockTransport) Write(ctx context.Context, address string, request *pb.WriteRequest) (*pb.WriteResponse, error) {
	args := m.MethodCalled("Write", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*pb.WriteResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockTransport) Read(ctx context.Context, address string, request *pb.ReadRequest) (*pb.ReadResponse, error) {
	args := m.MethodCalled("Read", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*pb.ReadResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockTransport) Commit(ctx context.Context, address string, request *pb.CommitRequest) (*pb.CommitResponse, error) {
	args := m.MethodCalled("Commit", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*pb.CommitResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockTransport) Propagate(ctx context.Context, address string, request *pb.PropagateRequest) (grpc.ServerStreamingClient[storagepb.KeyValuePair], error) {
	args := m.MethodCalled("Propagate", ctx, address, request)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(grpc.ServerStreamingClient[storagepb.KeyValuePair]), args.Error(1)
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

func TestReplicate(t *testing.T) {
	member1 := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	member2 := &ChainMember{ID: "member-2", Address: "127.0.0.2:8080"}

	transport := new(mockTransport)
	store := new(mockStorage)
	node := NewChainNode(member1.ID, member1.Address, store, transport, slog.Default())

	// Node is not a member of a chain.
	request := &pb.ReplicateRequest{Key: "key", Value: []byte("value")}
	version := uint64(1)
	_, err := node.Replicate(context.Background(), request)
	require.ErrorIs(t, err, ErrNotMemberOfChain)

	// Node is the the sole member of the chain - no forwarding is necessary.
	config := NewConfiguration([]*ChainMember{member1}, 0)
	state := node.state.Load()
	state.Config = config
	store.On("CommittedWriteNewVersion", request.Key, request.Value).Return(version, nil).Once()
	_, err = node.Replicate(context.Background(), request)
	require.NoError(t, err)
	store.AssertExpectations(t)

	// Node is head of a chain with multiple members - forwarding is necessary.
	config = NewConfiguration([]*ChainMember{member1, member2}, 0)
	state = node.state.Load()
	state.Config = config
	store.On("UncommittedWriteNewVersion", request.Key, request.Value).Return(version, nil).Once()
	transport.On("Write", mock.Anything, member2.Address, &pb.WriteRequest{
		Key:           request.Key,
		Value:         request.Value,
		Version:       version,
		ConfigVersion: 0,
	}).Return(&pb.WriteResponse{}, nil).Once()
	_, err = node.Replicate(context.Background(), request)
	require.NoError(t, err)
	store.AssertExpectations(t)
	transport.AssertExpectations(t)

	// Node is not the head.
	config = NewConfiguration([]*ChainMember{member2, member1}, 0)
	state = node.state.Load()
	state.Config = config
	_, err = node.Replicate(context.Background(), request)
	require.ErrorIs(t, err, ErrNotHead)

	// There is a mismatch between the node configuration version and the request configuration version.
	config = NewConfiguration([]*ChainMember{member1}, 1)
	state = node.state.Load()
	state.Config = config
	_, err = node.Replicate(context.Background(), request)
	require.ErrorIs(t, err, ErrInvalidConfigVersion)
}

func TestWriteWithVersion(t *testing.T) {
	member1 := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	member2 := &ChainMember{ID: "member-2", Address: "127.0.0.2:8080"}
	member3 := &ChainMember{ID: "member-3", Address: "127.0.0.3:8080"}

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode(member1.ID, member1.Address, store, transport, slog.Default())

	// Node is not a member of a chain.
	key := "key"
	value := []byte("value")
	version := uint64(1)
	req := &pb.WriteRequest{Key: key, Value: value, Version: version}
	_, err := node.WriteWithVersion(context.Background(), req)
	require.ErrorIs(t, err, ErrNotMemberOfChain)

	// Node is the the tail of the chain. It should immediately commit the key-value pair.
	config := NewConfiguration([]*ChainMember{member2, member1}, 0)
	state := node.state.Load()
	state.Config = config
	store.On("CommittedWrite", key, value, version).Return(nil).Once()
	_, err = node.WriteWithVersion(context.Background(), req)
	require.NoError(t, err)
	store.AssertExpectations(t)

	// Node is not he head of the chain. It should forward the write without committing.
	config = NewConfiguration([]*ChainMember{member2, member1, member3}, 0)
	state = node.state.Load()
	state.Config = config
	store.On("UncommittedWrite", key, value, version).Return(nil).Once()
	transport.On("Write", mock.Anything, member3.Address, &pb.WriteRequest{
		Key:     key,
		Value:   value,
		Version: version,
	}).Return(&pb.WriteResponse{}, nil).Once()
	_, err = node.WriteWithVersion(context.Background(), req)
	require.NoError(t, err)
	store.AssertExpectations(t)
	transport.AssertExpectations(t)

	// There is a mismatch between the node configuration version and the request configuration version.
	config = NewConfiguration([]*ChainMember{member2, member1, member3}, 1)
	state = node.state.Load()
	state.Config = config
	_, err = node.WriteWithVersion(context.Background(), req)
	require.ErrorIs(t, err, ErrInvalidConfigVersion)
}

func TestRead(t *testing.T) {
	member1 := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	member2 := &ChainMember{ID: "member-2", Address: "127.0.0.2:8080"}
	member3 := &ChainMember{ID: "member-3", Address: "127.0.0.3:8080"}

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode(member1.ID, member1.Address, store, transport, slog.Default())

	// Node is not a member of a chain.
	key := "key"
	value := []byte("value")
	req := &pb.ReadRequest{Key: key}
	_, err := node.Read(context.Background(), req)
	require.ErrorIs(t, err, ErrNotMemberOfChain)

	// Node is not the tail of the chain, but the key-value pair is committed.
	config := NewConfiguration([]*ChainMember{member1, member2, member3}, 0)
	state := node.state.Load()
	state.Config = config
	store.On("CommittedRead", key).Return(value, nil).Once()
	resp, err := node.Read(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, value, resp.Value)
	store.AssertExpectations(t)

	// Node is not the tail of the chain and the key-value pair is dirty.
	// It should forward the read to the tail.
	forwardedReq := &pb.ReadRequest{Key: key, ConfigVersion: 0, Forwarded: true}
	store.On("CommittedRead", key).Return(nil, storage.ErrDirtyRead).Once()
	transport.On("Read", mock.Anything, member3.Address, forwardedReq).Return(&pb.ReadResponse{Value: value}, nil).Once()
	resp, err = node.Read(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, value, resp.Value)
	store.AssertExpectations(t)
	transport.AssertExpectations(t)

	// There is a mismatch between the node configuration version and the request configuration version.
	config = NewConfiguration([]*ChainMember{member1, member2, member3}, 1)
	state = node.state.Load()
	state.Config = config
	_, err = node.Read(context.Background(), req)
	require.ErrorIs(t, err, ErrInvalidConfigVersion)
}

func TestCommit(t *testing.T) {
	member1 := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode(member1.ID, member1.Address, store, transport, slog.Default())

	// Node is not a member of a chain.
	key := "key"
	version := uint64(1)
	req := &pb.CommitRequest{Key: key, Version: version}
	_, err := node.Commit(context.Background(), req)
	require.ErrorIs(t, err, ErrNotMemberOfChain)

	// Once a key-value pair is committed, the node should notify the background routine
	// which will inform the predecessors of the commit.
	config := NewConfiguration([]*ChainMember{member1}, 0)
	state := node.state.Load()
	state.Config = config
	store.On("CommitVersion", key, version).Return(nil).Once()
	_, err = node.Commit(context.Background(), req)
	require.NoError(t, err)
	store.AssertExpectations(t)
	require.Len(t, node.onCommitCh, 1)
	commitMsg := <-node.onCommitCh
	require.Equal(t, key, commitMsg.key)
	require.Equal(t, version, commitMsg.version)

	// There is a mismatch between the node configuration version and the request configuration version.
	config = NewConfiguration([]*ChainMember{member1}, 1)
	state = node.state.Load()
	state.Config = config
	_, err = node.Commit(context.Background(), req)
	require.ErrorIs(t, err, ErrInvalidConfigVersion)
}

func TestRequestPropagation(t *testing.T) {
	member1 := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	member2 := &ChainMember{ID: "member-2", Address: "127.0.0.2:8080"}

	store := new(mockStorage)
	transport := new(mockTransport)
	node := NewChainNode(member1.ID, member1.Address, store, transport, slog.Default())

	stream := new(mockClientStream)
	kv1 := storage.KeyValuePair{Key: "key-1", Value: []byte("value-1"), Committed: false}
	kv2 := storage.KeyValuePair{Key: "key-2", Value: []byte("value-2"), Committed: true}

	// All keys are propagated. Dirty keys should not be committed since this node is not the tail.
	config := NewConfiguration([]*ChainMember{member1, member2}, 0)
	state := node.state.Load()
	state.Config = config
	stream.On("Recv").Return(kv1.Proto(), nil).Once()
	stream.On("Recv").Return(kv2.Proto(), nil).Once()
	stream.On("Recv").Return(nil, io.EOF).Once()
	transport.On("Propagate", mock.Anything, member2.Address, &pb.PropagateRequest{KeyType: storage.AllKeys.Proto(), ConfigVersion: 0}).Return(stream, nil).Once()
	store.On("UncommittedWrite", kv1.Key, kv1.Value, kv1.Version).Return(nil).Once()
	store.On("CommittedWrite", kv2.Key, kv2.Value, kv2.Version).Return(nil).Once()
	require.NoError(t, node.requestPropagation(context.Background(), member2, storage.AllKeys, config))
	stream.AssertExpectations(t)
	transport.AssertExpectations(t)
	store.AssertExpectations(t)

	// All keys are propagated. Dirty keys should be committed since this node is the tail.
	config = NewConfiguration([]*ChainMember{member2, member1}, 0)
	state = node.state.Load()
	state.Config = config
	stream.On("Recv").Return(kv1.Proto(), nil).Once()
	stream.On("Recv").Return(kv2.Proto(), nil).Once()
	stream.On("Recv").Return(nil, io.EOF).Once()
	transport.On("Propagate", mock.Anything, member2.Address, &pb.PropagateRequest{KeyType: storage.AllKeys.Proto(), ConfigVersion: 0}).Return(stream, nil).Once()
	store.On("CommittedWrite", kv1.Key, kv1.Value, kv1.Version).Return(nil).Once()
	store.On("CommittedWrite", kv2.Key, kv2.Value, kv2.Version).Return(nil).Once()
	require.NoError(t, node.requestPropagation(context.Background(), member2, storage.AllKeys, config))
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
	node := NewChainNode(member1.ID, member1.Address, store, transport, slog.Default())

	stream := new(mockClientStream)
	kv1 := storage.KeyValuePair{Key: "key-1", Value: []byte("value-1"), Committed: false}
	kv2 := storage.KeyValuePair{Key: "key-2", Value: []byte("value-2"), Committed: true}

	// Node becomes new member of existing chain.
	// It should request all key-value pairs from its predecessor to sync and then signal syncing is complete.
	// It is the tail so it should commit any key-value pairs it receives immediately.
	node.syncCompleteCh = make(chan any, 1)
	config := NewConfiguration([]*ChainMember{member2, member1}, 0)
	stream.On("Recv").Return(kv1.Proto(), nil).Once()
	stream.On("Recv").Return(kv2.Proto(), nil).Once()
	stream.On("Recv").Return(nil, io.EOF).Once()
	transport.On("Propagate", mock.Anything, member2.Address, &pb.PropagateRequest{KeyType: storage.AllKeys.Proto(), ConfigVersion: 0}).Return(stream, nil).Once()
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
	stream.On("Recv").Return(kv1.Proto(), nil).Once()
	stream.On("Recv").Return(kv2.Proto(), nil).Once()
	stream.On("Recv").Return(nil, io.EOF).Once()
	transport.On("Propagate", mock.Anything, member3.Address, &pb.PropagateRequest{KeyType: storage.DirtyKeys.Proto(), ConfigVersion: 0}).Return(stream, nil).Once()
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
	node := NewChainNode(member1.ID, member1.Address, store, transport, slog.Default())

	stream := new(mockClientStream)
	kv1 := storage.KeyValuePair{Key: "key-1", Value: []byte("value-1"), Committed: false}
	kv2 := storage.KeyValuePair{Key: "key-2", Value: []byte("value-2"), Committed: true}

	// Node becomes new member of existing chain.
	// It should request all key-value pairs from its successor to sync and then signal syncing is complete.
	// It is not the tail, so it should only commit key-value pairs that it knows are committed.
	node.syncCompleteCh = make(chan any, 1)
	config := NewConfiguration([]*ChainMember{member1, member2}, 0)
	stream.On("Recv").Return(kv1.Proto(), nil).Once()
	stream.On("Recv").Return(kv2.Proto(), nil).Once()
	stream.On("Recv").Return(nil, io.EOF).Once()
	transport.On("Propagate", mock.Anything, member2.Address, &pb.PropagateRequest{KeyType: storage.AllKeys.Proto(), ConfigVersion: 0}).Return(stream, nil).Once()
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
	stream.On("Recv").Return(kv2.Proto(), nil).Once()
	stream.On("Recv").Return(nil, io.EOF).Once()
	transport.On("Propagate", mock.Anything, member3.Address, &pb.PropagateRequest{KeyType: storage.CommittedKeys.Proto(), ConfigVersion: 0}).Return(stream, nil).Once()
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
	node := NewChainNode(member1.ID, member1.Address, store, transport, slog.Default())

	version := uint64(3)
	status := Syncing
	config := NewConfiguration([]*ChainMember{member1, member2, member3}, version)
	state := node.state.Load()
	state.Config = config
	state.Status = status

	req := &pb.PingRequest{}
	resp := node.Ping(req)
	require.Equal(t, version, resp.ConfigVersion)
	require.Equal(t, int32(Syncing), resp.Status)
}
