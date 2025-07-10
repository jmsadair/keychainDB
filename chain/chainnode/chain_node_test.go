package chainnode

import (
	"net"
	"testing"

	"github.com/jmsadair/zebraos/chain/storage"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockClient struct {
	mock.Mock
}

func newMockClient() *mockClient {
	return &mockClient{}
}

func (mc *mockClient) Write(address net.Addr, key string, value []byte, version uint64) error {
	args := mc.MethodCalled("Write", address, key, value, version)
	return args.Error(0)
}

func (mc *mockClient) Read(address net.Addr, key string) ([]byte, error) {
	args := mc.MethodCalled("Read", address, key)
	return args.Get(0).([]byte), args.Error(1)
}

type mockStorage struct {
	mock.Mock
}

func newMockStorage() *mockStorage {
	return &mockStorage{}
}

func (ms *mockStorage) CommittedWrite(key string, value []byte, version uint64) error {
	args := ms.MethodCalled("CommittedWrite", key, value, version)
	return args.Error(0)
}

func (ms *mockStorage) CommittedRead(key string) ([]byte, error) {
	args := ms.MethodCalled("CommittedRead", key)
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

func (ms *mockStorage) SendKeys(sendFunc func([]string) error, keyFilter storage.KeyFilter) error {
	args := ms.MethodCalled("SendKeys", sendFunc, keyFilter)
	return args.Error(0)
}

func TestNewChainNode(t *testing.T) {
	address, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)

	store := newMockStorage()
	client := newMockClient()
	node := NewChainNode(address, store, client)

	require.Equal(t, address.String(), node.address.String())
}
