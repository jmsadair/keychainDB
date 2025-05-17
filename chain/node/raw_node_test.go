package node

import (
	"net"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockStorage struct {
	mock.Mock
}

func newMockStorage() *mockStorage {
	return &mockStorage{}
}

func (ms *mockStorage) Set(key string, value []byte) error {
	args := ms.MethodCalled("Set", key, value)
	return args.Error(0)
}

func (ms *mockStorage) Get(key string) ([]byte, error) {
	args := ms.MethodCalled("Get", key)
	return args.Get(0).([]byte), args.Error(1)
}

func TestNewRawNode(t *testing.T) {
	address, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	predecessor, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	successor, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)
	storage := newMockStorage()
	node := newRawNode(address, predecessor, successor, storage)

	require.Equal(t, address.String(), node.address.String())
	require.Equal(t, predecessor.String(), node.predecessor().String())
	require.Equal(t, successor.String(), node.successor().String())
}

func TestSetGet(t *testing.T) {
	address, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	predecessor, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	successor, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)
	storage := newMockStorage()
	node := newRawNode(address, predecessor, successor, storage)

	key := "key"
	value := []byte("value")
	storage.On("Set", key, value).Return(nil)
	storage.On("Get", key).Return(value, nil)
	err = node.set(key, value)
	require.NoError(t, err)
	returnedValue, err := node.get(key)
	require.Equal(t, value, returnedValue)
	require.NoError(t, err)
	storage.AssertCalled(t, "Set", key, value)
	storage.AssertNumberOfCalls(t, "Set", 1)
	storage.AssertCalled(t, "Get", key)
	storage.AssertNumberOfCalls(t, "Get", 1)
}
