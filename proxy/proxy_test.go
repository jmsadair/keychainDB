package proxy

import (
	"context"
	"log/slog"
	"testing"

	"github.com/jmsadair/keychainDB/chain"
	apipb "github.com/jmsadair/keychainDB/proto/api"
	chainpb "github.com/jmsadair/keychainDB/proto/chain"
	coordinatorpb "github.com/jmsadair/keychainDB/proto/coordinator"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSetValue(t *testing.T) {
	members := []string{"coordinator-0.cluster.local"}
	chainClient := new(mockChainClient)
	coordinatorClient := new(mockCoordinatorClient)
	log := slog.Default()
	p := NewProxy(members, coordinatorClient, chainClient, log)

	ctx := context.Background()
	key := "key"
	value := []byte("value")
	head := chain.ChainMember{Address: "node-1.chain.local", ID: "node-1"}
	tail := chain.ChainMember{Address: "node-2.chain.local", ID: "node-2"}
	config := chain.NewConfiguration([]*chain.ChainMember{&head, &tail}, 0)

	chainClient.On(
		"Replicate",
		mock.Anything,
		head.Address,
		&chainpb.ReplicateRequest{Key: key, Value: value},
	).Return(&chainpb.ReplicateResponse{}, nil).Once()
	coordinatorClient.On(
		"GetMembers",
		mock.Anything,
		members[0],
		&coordinatorpb.GetMembersRequest{},
	).Return(&coordinatorpb.GetMembersResponse{Configuration: config.Proto()}, nil).Once()

	// Proxy initially does not have a chain configuration cached.
	// It should read it from the coordinator and then set the key-value pair on the head of the chain.
	req := &apipb.SetRequest{Key: key, Value: value}
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
	members := []string{"coordinator-0.cluster.local"}
	chainClient := new(mockChainClient)
	coordinatorClient := new(mockCoordinatorClient)
	log := slog.Default()
	p := NewProxy(members, coordinatorClient, chainClient, log)

	ctx := context.Background()
	key := "key"
	value := []byte("value")
	head := chain.ChainMember{Address: "node-1.chain.local", ID: "node-1"}
	tail := chain.ChainMember{Address: "node-2.chain.local", ID: "node-2"}
	config := chain.NewConfiguration([]*chain.ChainMember{&head, &tail}, 0)

	chainClient.On(
		"Read",
		mock.Anything,
		tail.Address,
		&chainpb.ReadRequest{Key: key},
	).Return(&chainpb.ReadResponse{Value: value}, nil).Once()
	coordinatorClient.On(
		"GetMembers",
		mock.Anything,
		members[0],
		&coordinatorpb.GetMembersRequest{},
	).Return(&coordinatorpb.GetMembersResponse{Configuration: config.Proto()}, nil).Once()

	// Proxy initially does not have a chain configuration cached.
	// It should read it from the coordinator and then get the key-value pair from the tail of the chain.
	req := &apipb.GetRequest{Key: key}
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
