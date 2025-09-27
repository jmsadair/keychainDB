package node

import (
	"context"
	"errors"
	"testing"

	chainnode "github.com/jmsadair/keychain/chain/node"
	coordinatornode "github.com/jmsadair/keychain/coordinator/node"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockCoordinatorClient struct {
	mock.Mock
}

func (m *mockCoordinatorClient) ReadChainConfiguration(
	ctx context.Context,
	target string,
	request *coordinatornode.ReadChainConfigurationRequest,
	response *coordinatornode.ReadChainConfigurationResponse,
) error {
	args := m.MethodCalled("ReadChainConfiguration", ctx, target, request)
	if resp := args.Get(0); resp != nil {
		*response = *resp.(*coordinatornode.ReadChainConfigurationResponse)
	}
	return args.Error(1)
}

type mockChainClient struct {
	mock.Mock
}

func (m *mockChainClient) Read(
	ctx context.Context,
	target string,
	request *chainnode.ReadRequest,
	response *chainnode.ReadResponse,
) error {
	args := m.MethodCalled("Read", ctx, target, request)
	if resp := args.Get(0); resp != nil {
		*response = *resp.(*chainnode.ReadResponse)
	}
	return args.Error(1)
}

func (m *mockChainClient) Replicate(
	ctx context.Context,
	target string,
	request *chainnode.ReplicateRequest,
	response *chainnode.ReplicateResponse,
) error {
	args := m.MethodCalled("Replicate", ctx, target, request)
	if resp := args.Get(0); resp != nil {
		*response = *resp.(*chainnode.ReplicateResponse)
	}
	return args.Error(1)
}

func TestSetValue(t *testing.T) {
	members := []string{"coordinator-0.cluster.local", "coordinator-1.cluster.local", "coordinator-2.cluster.local"}
	chainClient := new(mockChainClient)
	coordinatorClient := new(mockCoordinatorClient)
	p := NewProxy(members, coordinatorClient, chainClient)

	key := "key"
	value := []byte("value")
	head := chainnode.ChainMember{Address: "node-1.chain.local", ID: "node-1"}
	tail := chainnode.ChainMember{Address: "node-2.chain.local", ID: "node-2"}
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{&head, &tail}, 0)

	chainClient.On(
		"Replicate",
		mock.Anything,
		head.Address,
		&chainnode.ReplicateRequest{Key: key, Value: value},
	).Return(&chainnode.ReplicateResponse{}, nil).Once()

	coordinatorClient.On(
		"ReadChainConfiguration",
		mock.Anything,
		members[0],
		&coordinatornode.ReadChainConfigurationRequest{},
	).Return(nil, errors.New("not leader")).Once()
	coordinatorClient.On(
		"ReadChainConfiguration",
		mock.Anything,
		members[1],
		&coordinatornode.ReadChainConfigurationRequest{},
	).Return(&coordinatornode.ReadChainConfigurationResponse{Configuration: config}, nil).Once()
	coordinatorClient.On(
		"ReadChainConfiguration",
		mock.Anything,
		members[2],
		&coordinatornode.ReadChainConfigurationRequest{},
	).Return(nil, errors.New("not leader")).Once()

	// Proxy initially does not have a chain configuration cached.
	// It should read it from the coordinator and then set the key-value pair on the head of the chain.
	require.NoError(t, p.SetValue(context.Background(), key, value))
	chainClient.AssertExpectations(t)
	coordinatorClient.AssertExpectations(t)

	chainClient.On(
		"Replicate",
		mock.Anything,
		head.Address,
		&chainnode.ReplicateRequest{Key: key, Value: value},
	).Return(&chainnode.ReplicateResponse{}, nil).Once()

	// Next request should use the cached chain configuration.
	require.NoError(t, p.SetValue(context.Background(), key, value))
	chainClient.AssertExpectations(t)
}

func TestGetValue(t *testing.T) {
	members := []string{"coordinator-0.cluster.local", "coordinator-1.cluster.local", "coordinator-2.cluster.local"}
	chainClient := new(mockChainClient)
	coordinatorClient := new(mockCoordinatorClient)
	p := NewProxy(members, coordinatorClient, chainClient)

	key := "key"
	value := []byte("value")
	head := chainnode.ChainMember{Address: "node-1.chain.local", ID: "node-1"}
	tail := chainnode.ChainMember{Address: "node-2.chain.local", ID: "node-2"}
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{&head, &tail}, 0)

	chainClient.On(
		"Read",
		mock.Anything,
		tail.Address,
		&chainnode.ReadRequest{Key: key},
	).Return(&chainnode.ReadResponse{Value: value}, nil).Once()

	coordinatorClient.On(
		"ReadChainConfiguration",
		mock.Anything,
		members[0],
		&coordinatornode.ReadChainConfigurationRequest{},
	).Return(nil, errors.New("not leader")).Once()
	coordinatorClient.On(
		"ReadChainConfiguration",
		mock.Anything,
		members[1],
		&coordinatornode.ReadChainConfigurationRequest{},
	).Return(&coordinatornode.ReadChainConfigurationResponse{Configuration: config}, nil).Once()
	coordinatorClient.On(
		"ReadChainConfiguration",
		mock.Anything,
		members[2],
		&coordinatornode.ReadChainConfigurationRequest{},
	).Return(nil, errors.New("not leader")).Once()

	// Proxy initially does not have a chain configuration cached.
	// It should read it from the coordinator and then get the key-value pair from the tail of the chain.
	v, err := p.GetValue(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, value, v)
	chainClient.AssertExpectations(t)
	coordinatorClient.AssertExpectations(t)

	chainClient.On(
		"Read",
		mock.Anything,
		tail.Address,
		&chainnode.ReadRequest{Key: key},
	).Return(&chainnode.ReadResponse{Value: value}, nil).Once()

	// Next request should use the cached chain configuration.
	v, err = p.GetValue(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, value, v)
	chainClient.AssertExpectations(t)
}
