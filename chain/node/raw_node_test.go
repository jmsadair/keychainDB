package node

import (
	"net"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockClient struct {
	mock.Mock
}

func newClientMock() *mockClient {
	return &mockClient{}
}

func (mc *mockClient) Put(address net.Addr, key string, value []byte) error {
	args := mc.MethodCalled("Put", address, key, value)
	return args.Error(0)
}

func (mc *mockClient) Get(address net.Addr, key string) ([]byte, error) {
	args := mc.MethodCalled("Get", address, key)
	return args.Get(0).([]byte), args.Error(1)
}

func (mc *mockClient) Delete(address net.Addr, key string) error {
	args := mc.MethodCalled("Delete", address, key)
	return args.Error(0)
}

type mockStorage struct {
	mock.Mock
}

func newMockStorage() *mockStorage {
	return &mockStorage{}
}

func (ms *mockStorage) Put(key string, value []byte) error {
	args := ms.MethodCalled("Put", key, value)
	return args.Error(0)
}

func (ms *mockStorage) Get(key string) ([]byte, error) {
	args := ms.MethodCalled("Get", key)
	return args.Get(0).([]byte), args.Error(1)
}

func (ms *mockStorage) Delete(key string) error {
	args := ms.MethodCalled("Delete", key)
	return args.Error(0)
}

func TestNewRawNode(t *testing.T) {
	address, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	predecessor, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	successor, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)
	storage := newMockStorage()
	client := newClientMock()
	node := newRawNode(address, predecessor, successor, storage, client)

	require.Equal(t, address.String(), node.address.String())
	require.Equal(t, predecessor.String(), node.predecessor().String())
	require.Equal(t, successor.String(), node.successor().String())
}

func TestSetPredecessor(t *testing.T) {
	address, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	predecessor, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	successor, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)
	storage := newMockStorage()
	client := newClientMock()
	node := newRawNode(address, predecessor, successor, storage, client)

	require.Equal(t, predecessor.String(), node.predecessor().String())
	newPredecessor, err := net.ResolveTCPAddr("tcp", "127.0.0.4:8080")
	require.NoError(t, err)
	node.setPredecessor(newPredecessor)
	require.Equal(t, newPredecessor.String(), node.predecessor().String())
}

func TestSetSuccessor(t *testing.T) {
	address, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	predecessor, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	successor, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)
	storage := newMockStorage()
	client := newClientMock()
	node := newRawNode(address, predecessor, successor, storage, client)

	require.Equal(t, successor.String(), node.successor().String())
	newSuccessor, err := net.ResolveTCPAddr("tcp", "127.0.0.4:8080")
	require.NoError(t, err)
	node.setSuccessor(newSuccessor)
	require.Equal(t, newSuccessor.String(), node.successor().String())
}

func TestPutGetDelete(t *testing.T) {
	address, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	predecessor, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	successor, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)
	storage := newMockStorage()
	client := newClientMock()
	node := newRawNode(address, predecessor, successor, storage, client)

	key := "key"
	value := []byte("value")

	storage.On("Put", key, value).Return(nil)
	client.On("Put", successor, key, value).Return(nil)
	err = node.put(key, value)
	require.NoError(t, err)
	storage.AssertCalled(t, "Put", key, value)
	storage.AssertNumberOfCalls(t, "Put", 1)

	storage.On("Get", key).Return(value, nil)
	returnedValue, err := node.get(key)
	require.NoError(t, err)
	require.Equal(t, value, returnedValue)
	storage.AssertCalled(t, "Get", key)
	storage.AssertNumberOfCalls(t, "Get", 1)

	storage.On("Delete", key).Return(nil)
	client.On("Delete", successor, key).Return(nil)
	err = node.delete(key)
	require.NoError(t, err)
	storage.AssertCalled(t, "Delete", key)
	storage.AssertNumberOfCalls(t, "Delete", 1)
}
