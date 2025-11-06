package node

import (
	"context"

	chainpb "github.com/jmsadair/keychainDB/proto/chain"
	coordinatorpb "github.com/jmsadair/keychainDB/proto/coordinator"
	"github.com/stretchr/testify/mock"
)

type mockCoordinatorClient struct {
	mock.Mock
}

func (m *mockCoordinatorClient) GetMembers(
	ctx context.Context,
	target string,
	request *coordinatorpb.GetMembersRequest,
) (*coordinatorpb.GetMembersResponse, error) {
	args := m.MethodCalled("GetMembers", ctx, target, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*coordinatorpb.GetMembersResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockCoordinatorClient) Close() error {
	args := m.MethodCalled("Close")
	return args.Error(0)
}

type mockChainClient struct {
	mock.Mock
}

func (m *mockChainClient) Read(ctx context.Context, target string, request *chainpb.ReadRequest) (*chainpb.ReadResponse, error) {
	args := m.MethodCalled("Read", ctx, target, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*chainpb.ReadResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockChainClient) Replicate(ctx context.Context, target string, request *chainpb.ReplicateRequest) (*chainpb.ReplicateResponse, error) {
	args := m.MethodCalled("Replicate", ctx, target, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*chainpb.ReplicateResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockChainClient) Close() error {
	args := m.MethodCalled("Close")
	return args.Error(0)
}
