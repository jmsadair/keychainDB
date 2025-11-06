package node

import (
	"context"

	"github.com/jmsadair/keychainDB/chain/storage"
	pb "github.com/jmsadair/keychainDB/proto/chain"
	storagepb "github.com/jmsadair/keychainDB/proto/storage"
	"github.com/stretchr/testify/mock"
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

type mockChainClient struct {
	mock.Mock
}

func (m *mockChainClient) Write(ctx context.Context, address string, request *pb.WriteRequest) (*pb.WriteResponse, error) {
	args := m.MethodCalled("Write", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*pb.WriteResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockChainClient) Read(ctx context.Context, address string, request *pb.ReadRequest) (*pb.ReadResponse, error) {
	args := m.MethodCalled("Read", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*pb.ReadResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockChainClient) Commit(ctx context.Context, address string, request *pb.CommitRequest) (*pb.CommitResponse, error) {
	args := m.MethodCalled("Commit", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*pb.CommitResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockChainClient) Propagate(ctx context.Context, address string, request *pb.PropagateRequest) (pb.ChainService_PropagateClient, error) {
	args := m.MethodCalled("Propagate", ctx, address, request)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(pb.ChainService_PropagateClient), args.Error(1)
}

func (m *mockChainClient) Close() error {
	args := m.MethodCalled("Close")
	return args.Error(0)
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
