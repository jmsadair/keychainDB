package node

import (
	"context"
	"errors"
	"testing"

	chainnode "github.com/jmsadair/keychain/chain/node"
	chainpb "github.com/jmsadair/keychain/proto/chain"
	coordinatorpb "github.com/jmsadair/keychain/proto/coordinator"
	proxypb "github.com/jmsadair/keychain/proto/proxy"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockCoordinatorClient struct {
	mock.Mock
}

func (m *mockCoordinatorClient) ReadChainConfiguration(
	ctx context.Context,
	target string,
	request *coordinatorpb.ReadChainConfigurationRequest,
) (*coordinatorpb.ReadChainConfigurationResponse, error) {
	args := m.MethodCalled("ReadChainConfiguration", ctx, target, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*coordinatorpb.ReadChainConfigurationResponse), nil
	}
	return nil, args.Error(1)
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

func TestSetValue(t *testing.T) {
	members := []string{"coordinator-0.cluster.local", "coordinator-1.cluster.local", "coordinator-2.cluster.local"}
	chainClient := new(mockChainClient)
	coordinatorClient := new(mockCoordinatorClient)
	p := NewProxy(members, coordinatorClient, chainClient)

	ctx := context.Background()
	key := "key"
	value := []byte("value")
	head := chainnode.ChainMember{Address: "node-1.chain.local", ID: "node-1"}
	tail := chainnode.ChainMember{Address: "node-2.chain.local", ID: "node-2"}
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{&head, &tail}, 0)

	chainClient.On(
		"Replicate",
		mock.Anything,
		head.Address,
		&chainpb.ReplicateRequest{Key: key, Value: value},
	).Return(&chainpb.ReplicateResponse{}, nil).Once()

	coordinatorClient.On(
		"ReadChainConfiguration",
		mock.Anything,
		members[0],
		&coordinatorpb.ReadChainConfigurationRequest{},
	).Return(nil, errors.New("not leader")).Once()
	coordinatorClient.On(
		"ReadChainConfiguration",
		mock.Anything,
		members[1],
		&coordinatorpb.ReadChainConfigurationRequest{},
	).Return(&coordinatorpb.ReadChainConfigurationResponse{Configuration: config.Proto()}, nil).Once()
	coordinatorClient.On(
		"ReadChainConfiguration",
		mock.Anything,
		members[2],
		&coordinatorpb.ReadChainConfigurationRequest{},
	).Return(nil, errors.New("not leader")).Once()

	// Proxy initially does not have a chain configuration cached.
	// It should read it from the coordinator and then set the key-value pair on the head of the chain.
	req := &proxypb.SetRequest{Key: key, Value: value}
	resp, err := p.Set(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	chainClient.AssertExpectations(t)
	coordinatorClient.AssertExpectations(t)

	chainClient.On(
		"Replicate",
		mock.Anything,
		head.Address,
		&chainpb.ReplicateRequest{Key: key, Value: value},
	).Return(&chainpb.ReplicateResponse{}, nil).Once()

	// Next request should use the cached chain configuration.
	resp, err = p.Set(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	chainClient.AssertExpectations(t)
}

func TestGetValue(t *testing.T) {
	members := []string{"coordinator-0.cluster.local", "coordinator-1.cluster.local", "coordinator-2.cluster.local"}
	chainClient := new(mockChainClient)
	coordinatorClient := new(mockCoordinatorClient)
	p := NewProxy(members, coordinatorClient, chainClient)

	ctx := context.Background()
	key := "key"
	value := []byte("value")
	head := chainnode.ChainMember{Address: "node-1.chain.local", ID: "node-1"}
	tail := chainnode.ChainMember{Address: "node-2.chain.local", ID: "node-2"}
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{&head, &tail}, 0)

	chainClient.On(
		"Read",
		mock.Anything,
		tail.Address,
		&chainpb.ReadRequest{Key: key},
	).Return(&chainpb.ReadResponse{Value: value}, nil).Once()

	coordinatorClient.On(
		"ReadChainConfiguration",
		mock.Anything,
		members[0],
		&coordinatorpb.ReadChainConfigurationRequest{},
	).Return(nil, errors.New("not leader")).Once()
	coordinatorClient.On(
		"ReadChainConfiguration",
		mock.Anything,
		members[1],
		&coordinatorpb.ReadChainConfigurationRequest{},
	).Return(&coordinatorpb.ReadChainConfigurationResponse{Configuration: config.Proto()}, nil).Once()
	coordinatorClient.On(
		"ReadChainConfiguration",
		mock.Anything,
		members[2],
		&coordinatorpb.ReadChainConfigurationRequest{},
	).Return(nil, errors.New("not leader")).Once()

	// Proxy initially does not have a chain configuration cached.
	// It should read it from the coordinator and then get the key-value pair from the tail of the chain.
	req := &proxypb.GetRequest{Key: key}
	resp, err := p.Get(ctx, req)
	require.NoError(t, err)
	require.Equal(t, value, resp.GetValue())
	chainClient.AssertExpectations(t)
	coordinatorClient.AssertExpectations(t)

	chainClient.On(
		"Read",
		mock.Anything,
		tail.Address,
		&chainpb.ReadRequest{Key: key},
	).Return(&chainpb.ReadResponse{Value: value}, nil).Once()

	// Next request should use the cached chain configuration.
	resp, err = p.Get(ctx, req)
	require.NoError(t, err)
	require.Equal(t, value, resp.GetValue())
	chainClient.AssertExpectations(t)
}
