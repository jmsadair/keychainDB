package chain

import (
	"context"
	"net"
	"testing"

	"github.com/jmsadair/zebraos/internal/storage"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockClient struct {
	mock.Mock
}

func (mc *mockClient) Write(ctx context.Context, address net.Addr, key string, value []byte, version uint64) error {
	args := mc.MethodCalled("Write", address, key, value, version)
	return args.Error(0)
}

func (mc *mockClient) Read(ctx context.Context, address net.Addr, key string) ([]byte, error) {
	args := mc.MethodCalled("Read", address, key)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]byte), args.Error(1)
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

func TestInitiateReplicatedWrite(t *testing.T) {
	address1, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	address2, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)

	client := new(mockClient)
	store := new(mockStorage)
	node := NewChainNode(address1, store, client)

	key := "key"
	value := []byte("value")
	version := uint64(1)

	err = node.InitiateReplicatedWrite(context.TODO(), key, value)
	require.ErrorIs(t, err, ErrNotMemberOfChain)

	chain := ChainID("chain")
	membership, err := NewChainMetadata(chain, []net.Addr{address1})
	require.NoError(t, err)
	node.membership.Store(membership)
	store.On("CommittedWriteNewVersion", key, value).Return(version, nil)
	err = node.InitiateReplicatedWrite(context.TODO(), key, value)
	require.NoError(t, err)
	store.AssertExpectations(t)
	store.ExpectedCalls = nil

	membership, err = NewChainMetadata(chain, []net.Addr{address1, address2})
	require.NoError(t, err)
	node.membership.Store(membership)
	store.On("UncommittedWriteNewVersion", key, value).Return(version, nil)
	store.On("CommitVersion", key, version).Return(nil)
	client.On("Write", address2, key, value, version).Return(nil)
	err = node.InitiateReplicatedWrite(context.TODO(), key, value)
	require.NoError(t, err)
	store.AssertExpectations(t)
	client.AssertExpectations(t)
	client.ExpectedCalls = nil
	store.ExpectedCalls = nil

	membership, err = NewChainMetadata(chain, []net.Addr{address2, address1})
	require.NoError(t, err)
	node.membership.Store(membership)
	err = node.InitiateReplicatedWrite(context.TODO(), key, value)
	require.ErrorIs(t, err, ErrNotHead)
}

func TestWriteWithVersion(t *testing.T) {
	address1, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	address2, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	address3, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)

	store := new(mockStorage)
	client := new(mockClient)
	node := NewChainNode(address1, store, client)

	key := "key"
	value := []byte("value")
	version := uint64(1)

	err = node.WriteWithVersion(context.TODO(), key, value, version)
	require.ErrorIs(t, err, ErrNotMemberOfChain)

	chain := ChainID("chain")
	membership, err := NewChainMetadata(chain, []net.Addr{address2, address1})
	require.NoError(t, err)
	node.membership.Store(membership)
	store.On("CommittedWrite", key, value, version).Return(nil)
	err = node.WriteWithVersion(context.TODO(), key, value, version)
	require.NoError(t, err)
	store.AssertExpectations(t)
	store.ExpectedCalls = nil

	membership, err = NewChainMetadata(chain, []net.Addr{address2, address1, address3})
	require.NoError(t, err)
	node.membership.Store(membership)
	store.On("UncommittedWrite", key, value, version).Return(nil)
	store.On("CommitVersion", key, version).Return(nil)
	client.On("Write", address3, key, value, version).Return(nil)
	err = node.WriteWithVersion(context.TODO(), key, value, version)
	require.NoError(t, err)
	store.AssertExpectations(t)
	client.AssertExpectations(t)
	store.ExpectedCalls = nil
	client.ExpectedCalls = nil
}

func TestRead(t *testing.T) {
	address1, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	address2, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	address3, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)

	store := new(mockStorage)
	client := new(mockClient)
	node := NewChainNode(address1, store, client)

	key := "key"
	value := []byte("value")

	_, err = node.Read(context.TODO(), key)
	require.ErrorIs(t, err, ErrNotMemberOfChain)

	chain := ChainID("chain")
	membership, err := NewChainMetadata(chain, []net.Addr{address1, address2, address3})
	require.NoError(t, err)
	node.membership.Store(membership)
	store.On("CommittedRead", key).Return(value, nil)
	readValue, err := node.Read(context.TODO(), key)
	require.NoError(t, err)
	require.Equal(t, value, readValue)
	store.AssertExpectations(t)
	store.ExpectedCalls = nil

	store.On("CommittedRead", key).Return(nil, storage.ErrDirtyRead)
	client.On("Read", address3, key).Return(value, nil)
	readValue, err = node.Read(context.TODO(), key)
	require.NoError(t, err)
	require.Equal(t, value, readValue)
	store.AssertExpectations(t)
	client.AssertExpectations(t)
	store.ExpectedCalls = nil
	client.ExpectedCalls = nil
}
