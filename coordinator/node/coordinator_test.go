package node

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	chainnode "github.com/jmsadair/keychainDB/chain/node"
	chainpb "github.com/jmsadair/keychainDB/proto/chain"
	pb "github.com/jmsadair/keychainDB/proto/coordinator"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockRaft struct {
	mock.Mock
}

func (m *mockRaft) JoinCluster(ctx context.Context, id, address string) error {
	return m.MethodCalled("JoinCluster", ctx, id, address).Error(0)
}

func (m *mockRaft) RemoveFromCluster(ctx context.Context, id string) error {
	return m.MethodCalled("RemoveFromCluster", ctx, id).Error(0)
}

func (m *mockRaft) AddMember(ctx context.Context, id, address string) (*chainnode.Configuration, error) {
	args := m.MethodCalled("AddMember", ctx, id, address)
	return args.Get(0).(*chainnode.Configuration), args.Error(1)
}

func (m *mockRaft) RemoveMember(ctx context.Context, id string) (*chainnode.Configuration, *chainnode.ChainMember, error) {
	args := m.MethodCalled("RemoveMember", ctx, id)
	return args.Get(0).(*chainnode.Configuration), args.Get(1).(*chainnode.ChainMember), args.Error(2)
}

func (m *mockRaft) GetMembers(ctx context.Context) (*chainnode.Configuration, error) {
	args := m.MethodCalled("GetMembers", ctx)
	return args.Get(0).(*chainnode.Configuration), args.Error(1)
}

func (m *mockRaft) LeaderCh() <-chan bool {
	return m.MethodCalled("LeaderCh").Get(0).(chan bool)
}

func (m *mockRaft) ChainConfiguration() *chainnode.Configuration {
	return m.MethodCalled("ChainConfiguration").Get(0).(*chainnode.Configuration)
}

func (m *mockRaft) ClusterStatus() (Status, error) {
	args := m.MethodCalled("ClusterStatus")
	return args.Get(0).(Status), args.Error(1)
}

func (m *mockRaft) Shutdown() error {
	args := m.MethodCalled("Shutdown")
	return args.Error(0)
}

type mockChainClient struct {
	mock.Mock
}

func (m *mockChainClient) Ping(ctx context.Context, address string, request *chainpb.PingRequest) (*chainpb.PingResponse, error) {
	args := m.MethodCalled("Ping", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*chainpb.PingResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockChainClient) UpdateConfiguration(ctx context.Context, address string, request *chainpb.UpdateConfigurationRequest) (*chainpb.UpdateConfigurationResponse, error) {
	args := m.MethodCalled("UpdateConfiguration", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*chainpb.UpdateConfigurationResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockChainClient) Close() error {
	args := m.MethodCalled("Close")
	return args.Error(0)
}

func TestNewCoordinator(t *testing.T) {
	chainClient := new(mockChainClient)
	consensus := new(mockRaft)
	id := "coordinator-1"
	addr := "127.0.0.1:9000"
	log := slog.Default()
	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(id, addr, chainClient, consensus, log)
	consensus.AssertExpectations(t)

	require.NotNil(t, coordinator)
	require.Equal(t, id, coordinator.ID)
	require.Equal(t, addr, coordinator.Address)
	require.NotNil(t, coordinator.memberStates)
	require.NotNil(t, coordinator.failedChainMemberCh)
	require.False(t, coordinator.isLeader)
}

func TestJoinCluster(t *testing.T) {
	chainClient := new(mockChainClient)
	consensus := new(mockRaft)
	id := "coordinator-1"
	addr := "127.0.0.1:9000"
	log := slog.Default()
	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(id, addr, chainClient, consensus, log)
	consensus.AssertExpectations(t)

	req := &pb.JoinClusterRequest{Id: "node-2", Address: "127.0.0.2:9000"}

	consensus.On("LeaderAddressAndID").Return(addr, id).Once()
	consensus.On("JoinCluster", mock.Anything, req.Id, req.Address).Return(nil)
	resp, err := coordinator.JoinCluster(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestRemoveFromCluster(t *testing.T) {
	chainClient := new(mockChainClient)
	consensus := new(mockRaft)
	id := "coordinator-1"
	addr := "127.0.0.1:9000"
	log := slog.Default()
	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(id, addr, chainClient, consensus, log)
	consensus.AssertExpectations(t)

	req := &pb.RemoveFromClusterRequest{Id: "coordinator-3"}

	consensus.On("LeaderAddressAndID").Return(addr, id).Once()
	consensus.On("RemoveFromCluster", mock.Anything, req.Id).Return(nil)
	resp, err := coordinator.RemoveFromCluster(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestClusterStatus(t *testing.T) {
	chainClient := new(mockChainClient)
	consensus := new(mockRaft)
	id := "coordinator-1"
	addr := "127.0.0.1:9000"
	log := slog.Default()
	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(id, addr, chainClient, consensus, log)
	consensus.AssertExpectations(t)

	req := &pb.ClusterStatusRequest{}

	consensus.On("LeaderAddressAndID").Return(addr, id).Once()
	consensus.On("ClusterStatus", mock.Anything).Return(Status{Leader: addr, Members: map[string]string{id: addr}}, nil)
	resp, err := coordinator.ClusterStatus(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestAddMember(t *testing.T) {
	chainClient := new(mockChainClient)
	consensus := new(mockRaft)
	id := "coordinator-1"
	addr := "127.0.0.1:9000"
	log := slog.Default()
	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(id, addr, chainClient, consensus, log)
	consensus.AssertExpectations(t)

	memberID := "member-1"
	memberAddr := "127.0.0.2:9000"
	member := &chainnode.ChainMember{ID: memberID, Address: memberAddr}
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{member}, 0)

	consensus.On("AddMember", mock.Anything, memberID, memberAddr).Return(config, nil).Once()
	chainClient.On("UpdateConfiguration", mock.Anything, memberAddr, mock.MatchedBy(func(r *chainpb.UpdateConfigurationRequest) bool {
		return config.Equal(chainnode.NewConfigurationFromProto(r.GetConfiguration()))
	})).Return(&chainpb.UpdateConfigurationResponse{}, nil).Once()
	req := &pb.AddMemberRequest{Id: memberID, Address: memberAddr}
	resp, err := coordinator.AddMember(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	chainClient.AssertExpectations(t)
	consensus.AssertExpectations(t)
}

func TestRemoveMember(t *testing.T) {
	chainClient := new(mockChainClient)
	consensus := new(mockRaft)
	id := "coordinator-1"
	addr := "127.0.0.1:9000"
	log := slog.Default()
	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(id, addr, chainClient, consensus, log)
	consensus.AssertExpectations(t)

	member1 := &chainnode.ChainMember{ID: "member-1", Address: "127.0.0.2:9000"}
	member2 := &chainnode.ChainMember{ID: "member-2", Address: "127.0.0.3:9000"}
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{member1}, 1)

	consensus.On("RemoveMember", mock.Anything, member2.ID).Return(config, member2, nil).Once()
	chainClient.On(
		"UpdateConfiguration",
		mock.Anything,
		member1.Address,
		&chainpb.UpdateConfigurationRequest{Configuration: config.Proto()},
	).Return(&chainpb.UpdateConfigurationResponse{}, nil).Once()
	chainClient.On(
		"UpdateConfiguration",
		mock.Anything,
		member2.Address,
		&chainpb.UpdateConfigurationRequest{Configuration: config.Proto()},
	).Return(&chainpb.UpdateConfigurationResponse{}, nil).Once()
	req := &pb.RemoveMemberRequest{Id: member2.ID}
	resp, err := coordinator.RemoveMember(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	consensus.AssertExpectations(t)
	chainClient.AssertExpectations(t)
}

func TestGetMembers(t *testing.T) {
	chainClient := new(mockChainClient)
	consensus := new(mockRaft)
	id := "coordinator-1"
	addr := "127.0.0.1:9000"
	log := slog.Default()
	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(id, addr, chainClient, consensus, log)
	consensus.AssertExpectations(t)

	member1 := &chainnode.ChainMember{ID: "member-1", Address: "127.0.0.1:9000"}
	member2 := &chainnode.ChainMember{ID: "member-2", Address: "127.0.0.2:9000"}

	expectedConfig := chainnode.NewConfiguration([]*chainnode.ChainMember{member1, member2}, 0)
	consensus.On("GetMembers", mock.Anything).Return(expectedConfig, nil).Once()
	req := &pb.GetMembersRequest{}
	resp, err := coordinator.GetMembers(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, expectedConfig, chainnode.NewConfigurationFromProto(resp.GetConfiguration()))
	consensus.AssertExpectations(t)
}

func TestOnHeartbeat(t *testing.T) {
	chainClient := new(mockChainClient)
	consensus := new(mockRaft)
	id := "coordinator-1"
	addr := "127.0.0.1:9000"
	log := slog.Default()
	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(id, addr, chainClient, consensus, log)
	consensus.AssertExpectations(t)

	member1 := &chainnode.ChainMember{ID: "member-1", Address: "127.0.0.2:9000"}
	member2 := &chainnode.ChainMember{ID: "member-2", Address: "127.0.0.3:9000"}
	member3 := &chainnode.ChainMember{ID: "member-3", Address: "127.0.0.4:9000"}
	version := uint64(1)
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{member1, member2, member3}, version)

	// This coordinator is the leader and all pings to chain members are successful.
	coordinator.isLeader = true
	consensus.On("ChainConfiguration").Return(config).Once()
	chainClient.On("Ping", mock.Anything, member1.Address, &chainpb.PingRequest{}).Return(&chainpb.PingResponse{ConfigVersion: version}, nil).Once()
	chainClient.On("Ping", mock.Anything, member2.Address, &chainpb.PingRequest{}).Return(&chainpb.PingResponse{ConfigVersion: version}, nil).Once()
	chainClient.On("Ping", mock.Anything, member3.Address, &chainpb.PingRequest{}).Return(&chainpb.PingResponse{ConfigVersion: version}, nil).Once()
	coordinator.onHeartbeat(context.Background())
	require.Contains(t, coordinator.memberStates, member1.ID)
	require.Contains(t, coordinator.memberStates, member2.ID)
	require.Contains(t, coordinator.memberStates, member3.ID)
	consensus.AssertExpectations(t)
	chainClient.AssertExpectations(t)

	// This coordinator is the leader and a ping to one of the chain members fails.
	coordinator.failedChainMemberCh = make(chan any, 1)
	consensus.On("ChainConfiguration").Return(config).Once()
	chainClient.On("Ping", mock.Anything, member1.Address, &chainpb.PingRequest{}).Return(&chainpb.PingResponse{ConfigVersion: version}, nil).Once()
	chainClient.On("Ping", mock.Anything, member2.Address, &chainpb.PingRequest{}).Return(nil, errors.New("ping RPC failed")).Once()
	chainClient.On("Ping", mock.Anything, member3.Address, &chainpb.PingRequest{}).Return(&chainpb.PingResponse{ConfigVersion: version}, nil).Once()
	coordinator.onHeartbeat(context.Background())
	require.Len(t, coordinator.failedChainMemberCh, 1)
	consensus.AssertExpectations(t)
	chainClient.AssertExpectations(t)

	// This coordinator is the leader and a chain members has an out-of-date configuration.
	coordinator.configSyncCh = make(chan any, 1)
	consensus.On("ChainConfiguration").Return(config).Once()
	chainClient.On("Ping", mock.Anything, member1.Address, &chainpb.PingRequest{}).Return(&chainpb.PingResponse{ConfigVersion: version}, nil).Once()
	chainClient.On("Ping", mock.Anything, member2.Address, &chainpb.PingRequest{}).Return(&chainpb.PingResponse{ConfigVersion: version}, nil).Once()
	chainClient.On("Ping", mock.Anything, member3.Address, &chainpb.PingRequest{}).Return(&chainpb.PingResponse{ConfigVersion: version - 1}, nil).Once()
	coordinator.onHeartbeat(context.Background())
	require.Len(t, coordinator.configSyncCh, 1)
	consensus.AssertExpectations(t)
	chainClient.AssertExpectations(t)

	// This coordinator is not the leader. It should not send heartbeats.
	coordinator.isLeader = false
	coordinator.memberStates = make(map[string]*memberState)
	coordinator.onHeartbeat(context.Background())
	require.Empty(t, coordinator.memberStates)
}

func TestOnFailedChainMember(t *testing.T) {
	chainClient := new(mockChainClient)
	consensus := new(mockRaft)
	id := "coordinator-1"
	addr := "127.0.0.1:9000"
	log := slog.Default()
	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(id, addr, chainClient, consensus, log)
	consensus.AssertExpectations(t)

	member1 := &chainnode.ChainMember{ID: "member-1", Address: "127.0.0.2:9000"}
	member2 := &chainnode.ChainMember{ID: "member-2", Address: "127.0.0.3:9000"}
	member3 := &chainnode.ChainMember{ID: "member-3", Address: "127.0.0.4:9000"}

	// This coordinator is the leader and all chain members have been recently contacted.
	// It should not attempt to remove any of them.
	coordinator.isLeader = true
	coordinator.memberStates = map[string]*memberState{
		member1.ID: {lastContact: time.Now()},
		member2.ID: {lastContact: time.Now()},
		member3.ID: {lastContact: time.Now()},
	}
	coordinator.onFailedChainMember(context.Background())

	// This coordinator is the leader and one of the chain members has not been contacted in a while.
	// It should attempt to remove the failed member.
	coordinator.memberStates = map[string]*memberState{
		member1.ID: {lastContact: time.Now()},
		member2.ID: {lastContact: time.Now().Add(-60 * time.Second)},
		member3.ID: {lastContact: time.Now()},
	}
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{member1, member3}, 0)
	consensus.On("RemoveMember", mock.Anything, member2.ID).Return(config, member2, nil)
	chainClient.On("UpdateConfiguration", mock.Anything, member1.Address, mock.MatchedBy(func(r *chainpb.UpdateConfigurationRequest) bool {
		return config.Equal(chainnode.NewConfigurationFromProto(r.GetConfiguration()))
	})).Return(&chainpb.UpdateConfigurationResponse{}, nil)
	chainClient.On("UpdateConfiguration", mock.Anything, member2.Address, mock.MatchedBy(func(r *chainpb.UpdateConfigurationRequest) bool {
		return config.Equal(chainnode.NewConfigurationFromProto(r.GetConfiguration()))
	})).Return(&chainpb.UpdateConfigurationResponse{}, errors.New("RPC failed"))
	chainClient.On("UpdateConfiguration", mock.Anything, member3.Address, mock.MatchedBy(func(r *chainpb.UpdateConfigurationRequest) bool {
		return config.Equal(chainnode.NewConfigurationFromProto(r.GetConfiguration()))
	})).Return(&chainpb.UpdateConfigurationResponse{}, nil)
	coordinator.onFailedChainMember(context.Background())
	consensus.AssertExpectations(t)
	chainClient.AssertExpectations(t)

	// This coordinator is not the leader. It should not attempt to remove chain members.
	coordinator.isLeader = false
	coordinator.onFailedChainMember(context.Background())
}

func TestOnLeadershipChange(t *testing.T) {
	chainClient := new(mockChainClient)
	consensus := new(mockRaft)
	id := "coordinator-1"
	addr := "127.0.0.1:9000"
	log := slog.Default()
	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(id, addr, chainClient, consensus, log)
	consensus.AssertExpectations(t)

	memberID := "member-1"
	coordinator.memberStates = map[string]*memberState{
		memberID: {lastContact: time.Now()},
	}
	coordinator.onLeadershipChange(true)
	require.True(t, coordinator.isLeader)
	require.Empty(t, coordinator.memberStates)

	coordinator.memberStates = map[string]*memberState{
		memberID: {lastContact: time.Now()},
	}
	coordinator.onLeadershipChange(false)
	require.False(t, coordinator.isLeader)
	require.Empty(t, coordinator.memberStates)
}

func TestOnConfigSync(t *testing.T) {
	chainClient := new(mockChainClient)
	consensus := new(mockRaft)
	id := "coordinator-1"
	addr := "127.0.0.1:9000"
	log := slog.Default()
	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(id, addr, chainClient, consensus, log)
	consensus.AssertExpectations(t)

	member1 := &chainnode.ChainMember{ID: "member-1", Address: "127.0.0.2:9000"}
	member2 := &chainnode.ChainMember{ID: "member-2", Address: "127.0.0.3:9000"}
	member3 := &chainnode.ChainMember{ID: "member-3", Address: "127.0.0.4:9000"}
	version := uint64(3)
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{member1, member2, member3}, version)

	// This coordinator is the leader and all chain members have the correct chain configuration version.
	// It should not attempt to update any of their configurations.
	coordinator.isLeader = true
	consensus.On("GetMembers", mock.Anything).Return(config, nil).Once()
	coordinator.memberStates = map[string]*memberState{
		member1.ID: {lastContact: time.Now(), configVersion: version},
		member2.ID: {lastContact: time.Now(), configVersion: version},
		member3.ID: {lastContact: time.Now(), configVersion: version},
	}
	coordinator.onConfigSync(context.Background())
	consensus.AssertExpectations(t)

	// This coordinator is the leader and one of the chain members has an out-of-date configuration.
	// It should attempt to update that member's configuration.
	consensus.On("GetMembers", mock.Anything).Return(config, nil).Once()
	chainClient.On("UpdateConfiguration", mock.Anything, member2.Address, mock.MatchedBy(func(r *chainpb.UpdateConfigurationRequest) bool {
		return config.Equal(chainnode.NewConfigurationFromProto(r.GetConfiguration()))
	})).Return(&chainpb.UpdateConfigurationResponse{}, nil)
	coordinator.memberStates = map[string]*memberState{
		member1.ID: {lastContact: time.Now(), configVersion: version},
		member2.ID: {lastContact: time.Now(), configVersion: version - 1},
		member3.ID: {lastContact: time.Now(), configVersion: version},
	}
	coordinator.onConfigSync(context.Background())
	consensus.AssertExpectations(t)
	chainClient.AssertExpectations(t)

	// This coordinator is not the leader. It should not attempt to update the configuration of any chain members.
	coordinator.isLeader = false
	coordinator.onFailedChainMember(context.Background())
}
