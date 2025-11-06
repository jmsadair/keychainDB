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

func TestNewCoordinator(t *testing.T) {
	chainClient := new(mockChainClient)
	coordinatorClient := new(mockCoordinatorClient)
	consensus := new(mockRaft)

	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator("coordinator-1", "127.0.0.1:9000", chainClient, coordinatorClient, consensus, slog.Default())

	require.NotNil(t, coordinator)
	require.Equal(t, "coordinator-1", coordinator.ID)
	require.Equal(t, "127.0.0.1:9000", coordinator.Address)
	require.NotNil(t, coordinator.memberStates)
	require.NotNil(t, coordinator.failedChainMemberCh)
	require.False(t, coordinator.isLeader)
	consensus.AssertExpectations(t)
}

func TestJoinCluster(t *testing.T) {
	chainClient := new(mockChainClient)
	coordinatorClient := new(mockCoordinatorClient)
	consensus := new(mockRaft)

	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator("coordinator-1", "127.0.0.1:9000", chainClient, coordinatorClient, consensus, slog.Default())

	req := &pb.JoinClusterRequest{Id: "node-2", Address: "127.0.0.2:9000"}

	// This node is the leader.
	consensus.On("LeaderWithID").Return(coordinator.Address, coordinator.ID).Once()
	consensus.On("JoinCluster", mock.Anything, req.Id, req.Address).Return(nil)
	resp, err := coordinator.JoinCluster(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	consensus.AssertExpectations(t)

	// This node is not the leader. The request should be forwarded.
	consensus.On("LeaderWithID").Return("127.0.0.3:9000", "coordinator-3").Once()
	coordinatorClient.On("JoinCluster", mock.Anything, "127.0.0.3:9000", req).Return(&pb.JoinClusterResponse{}, nil).Once()
	resp, err = coordinator.JoinCluster(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	coordinatorClient.AssertExpectations(t)
	consensus.AssertExpectations(t)
}

func TestRemoveFromCluster(t *testing.T) {
	chainClient := new(mockChainClient)
	coordinatorClient := new(mockCoordinatorClient)
	consensus := new(mockRaft)

	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator("coordinator-1", "127.0.0.1:9000", chainClient, coordinatorClient, consensus, slog.Default())

	req := &pb.RemoveFromClusterRequest{Id: "coordinator-2"}

	// This node is the leader.
	consensus.On("LeaderWithID").Return(coordinator.Address, coordinator.ID).Once()
	consensus.On("RemoveFromCluster", mock.Anything, req.Id).Return(nil)
	resp, err := coordinator.RemoveFromCluster(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	consensus.AssertExpectations(t)

	// This node is not the leader. The request should be forwarded.
	consensus.On("LeaderWithID").Return("127.0.0.3:9000", "coordinator-3").Once()
	coordinatorClient.On("RemoveFromCluster", mock.Anything, "127.0.0.3:9000", req).Return(&pb.RemoveFromClusterResponse{}, nil).Once()
	resp, err = coordinator.RemoveFromCluster(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	coordinatorClient.AssertExpectations(t)
	consensus.AssertExpectations(t)

}

func TestClusterStatus(t *testing.T) {
	chainClient := new(mockChainClient)
	coordinatorClient := new(mockCoordinatorClient)
	consensus := new(mockRaft)

	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator("coordinator-1", "127.0.0.1:9000", chainClient, coordinatorClient, consensus, slog.Default())

	req := &pb.ClusterStatusRequest{}

	// This node is the leader.
	consensus.On("LeaderWithID").Return(coordinator.Address, coordinator.ID).Once()
	consensus.On("ClusterStatus", mock.Anything).Return(
		Status{Leader: coordinator.Address, Members: map[string]string{coordinator.ID: coordinator.Address}}, nil,
	)
	resp, err := coordinator.ClusterStatus(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	consensus.AssertExpectations(t)

	// This node is not the leader. The request should be forwarded.
	expectedLeader := "coordinator-2"
	expectedMembers := map[string]string{coordinator.ID: coordinator.Address, "coordinator-2": "127.0.0.2:9000"}
	consensus.On("LeaderWithID").Return("127.0.0.2:9000", "coordinator-2").Once()
	coordinatorClient.On("ClusterStatus", mock.Anything, "127.0.0.2:9000", req).Return(
		&pb.ClusterStatusResponse{
			Leader:  "coordinator-2",
			Members: map[string]string{coordinator.ID: coordinator.Address, "coordinator-2": "127.0.0.2:9000"},
		}, nil,
	).Once()
	resp, err = coordinator.ClusterStatus(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, expectedLeader, resp.GetLeader())
	require.Equal(t, expectedMembers, resp.GetMembers())
	coordinatorClient.AssertExpectations(t)
	consensus.AssertExpectations(t)

}

func TestAddMember(t *testing.T) {
	chainClient := new(mockChainClient)
	coordinatorClient := new(mockCoordinatorClient)
	consensus := new(mockRaft)

	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator("coordinator-1", "127.0.0.1:9000", chainClient, coordinatorClient, consensus, slog.Default())

	member := &chainnode.ChainMember{ID: "member-1", Address: "127.0.0.2:9000"}
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{member}, 0)

	// This node is the leader.
	consensus.On("LeaderWithID").Return(coordinator.Address, coordinator.ID).Once()
	consensus.On("AddMember", mock.Anything, member.ID, member.Address).Return(config, nil).Once()
	chainClient.On("UpdateConfiguration", mock.Anything, member.Address, mock.MatchedBy(func(r *chainpb.UpdateConfigurationRequest) bool {
		return config.Equal(chainnode.NewConfigurationFromProto(r.GetConfiguration()))
	})).Return(&chainpb.UpdateConfigurationResponse{}, nil).Once()
	req := &pb.AddMemberRequest{Id: member.ID, Address: member.Address}
	resp, err := coordinator.AddMember(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	chainClient.AssertExpectations(t)
	consensus.AssertExpectations(t)

	// This node is not the leader. The request should be forwarded.
	consensus.On("LeaderWithID").Return("127.0.0.3:9000", "coordinator-3").Once()
	coordinatorClient.On("AddMember", mock.Anything, "127.0.0.3:9000", req).Return(&pb.AddMemberResponse{}, nil).Once()
	resp, err = coordinator.AddMember(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	coordinatorClient.AssertExpectations(t)
	consensus.AssertExpectations(t)
}

func TestRemoveMember(t *testing.T) {
	chainClient := new(mockChainClient)
	coordinatorClient := new(mockCoordinatorClient)
	consensus := new(mockRaft)

	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator("coordinator-1", "127.0.0.1:9000", chainClient, coordinatorClient, consensus, slog.Default())

	member1 := &chainnode.ChainMember{ID: "member-1", Address: "127.0.0.2:9000"}
	member2 := &chainnode.ChainMember{ID: "member-2", Address: "127.0.0.3:9000"}
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{member1}, 1)

	// This node is the leader.
	consensus.On("LeaderWithID").Return(coordinator.Address, coordinator.ID).Once()
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

	// This node is not the leader. The request should be forwarded.
	consensus.On("LeaderWithID").Return("127.0.0.3:9000", "coordinator-3").Once()
	coordinatorClient.On("RemoveMember", mock.Anything, "127.0.0.3:9000", req).Return(&pb.RemoveMemberResponse{}, nil).Once()
	resp, err = coordinator.RemoveMember(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	coordinatorClient.AssertExpectations(t)
	consensus.AssertExpectations(t)
}

func TestGetMembers(t *testing.T) {
	chainClient := new(mockChainClient)
	coordinatorClient := new(mockCoordinatorClient)
	consensus := new(mockRaft)

	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator("coordinator-1", "127.0.0.1:9000", chainClient, coordinatorClient, consensus, slog.Default())

	member1 := &chainnode.ChainMember{ID: "member-1", Address: "127.0.0.2:9000"}
	member2 := &chainnode.ChainMember{ID: "member-2", Address: "127.0.0.3:9000"}
	expectedConfig := chainnode.NewConfiguration([]*chainnode.ChainMember{member1, member2}, 0)

	// This node is the leader.
	consensus.On("LeaderWithID").Return(coordinator.Address, coordinator.ID).Once()
	consensus.On("GetMembers", mock.Anything).Return(expectedConfig, nil).Once()
	req := &pb.GetMembersRequest{}
	resp, err := coordinator.GetMembers(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, expectedConfig, chainnode.NewConfigurationFromProto(resp.GetConfiguration()))
	consensus.AssertExpectations(t)

	// This node is not the leader. The request should be forwarded.
	consensus.On("LeaderWithID").Return("127.0.0.4:9000", "coordinator-2").Once()
	coordinatorClient.On("GetMembers", mock.Anything, "127.0.0.4:9000", req).Return(
		&pb.GetMembersResponse{Configuration: expectedConfig.Proto()}, nil,
	).Once()
	resp, err = coordinator.GetMembers(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, expectedConfig, chainnode.NewConfigurationFromProto(resp.GetConfiguration()))
	coordinatorClient.AssertExpectations(t)
	consensus.AssertExpectations(t)
}

func TestOnHeartbeat(t *testing.T) {
	chainClient := new(mockChainClient)
	coordinatorClient := new(mockCoordinatorClient)
	consensus := new(mockRaft)

	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator("coordinator-1", "127.0.0.1:9000", chainClient, coordinatorClient, consensus, slog.Default())

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
	coordinatorClient := new(mockCoordinatorClient)
	consensus := new(mockRaft)

	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator("coordinator-1", "127.0.0.1:9000", chainClient, coordinatorClient, consensus, slog.Default())

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
	coordinatorClient := new(mockCoordinatorClient)
	consensus := new(mockRaft)

	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator("coordinator-1", "127.0.0.1:9000", chainClient, coordinatorClient, consensus, slog.Default())

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
	coordinatorClient := new(mockCoordinatorClient)
	consensus := new(mockRaft)

	consensus.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator("coordinator-1", "127.0.0.1:9000", chainClient, coordinatorClient, consensus, slog.Default())

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
