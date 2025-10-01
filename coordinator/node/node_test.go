package node

import (
	"context"
	"errors"
	"testing"
	"time"

	chainnode "github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/coordinator/raft"
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

func (m *mockRaft) AddChainMember(ctx context.Context, id, address string) (*chainnode.Configuration, error) {
	args := m.MethodCalled("AddChainMember", ctx, id, address)
	return args.Get(0).(*chainnode.Configuration), args.Error(1)
}

func (m *mockRaft) RemoveChainMember(ctx context.Context, id string) (*chainnode.Configuration, *chainnode.ChainMember, error) {
	args := m.MethodCalled("RemoveChainMember", ctx, id)
	return args.Get(0).(*chainnode.Configuration), args.Get(1).(*chainnode.ChainMember), args.Error(2)
}

func (m *mockRaft) ReadChainConfiguration(ctx context.Context) (*chainnode.Configuration, error) {
	args := m.MethodCalled("ReadChainConfiguration", ctx)
	return args.Get(0).(*chainnode.Configuration), args.Error(1)
}

func (m *mockRaft) LeaderCh() <-chan bool {
	return m.MethodCalled("LeaderCh").Get(0).(chan bool)
}

func (m *mockRaft) ChainConfiguration() *chainnode.Configuration {
	return m.MethodCalled("ChainConfiguration").Get(0).(*chainnode.Configuration)
}

func (m *mockRaft) ClusterStatus() (*raft.Status, error) {
	args := m.MethodCalled("ClusterStatus")
	return args.Get(0).(*raft.Status), args.Error(1)
}

func (m *mockRaft) Shutdown() error {
	args := m.MethodCalled("Shutdown")
	return args.Error(0)
}

type mockTransport struct {
	mock.Mock
}

func (m *mockTransport) Ping(ctx context.Context, address string, request *chainnode.PingRequest, response *chainnode.PingResponse) error {
	args := m.MethodCalled("Ping", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		*response = *resp.(*chainnode.PingResponse)
	}
	return args.Error(1)
}

func (m *mockTransport) UpdateConfiguration(ctx context.Context, address string, request *chainnode.UpdateConfigurationRequest, response *chainnode.UpdateConfigurationResponse) error {
	args := m.MethodCalled("UpdateConfiguration", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		*response = *resp.(*chainnode.UpdateConfigurationResponse)
	}
	return args.Error(1)
}

func TestNewCoordinator(t *testing.T) {
	tn := new(mockTransport)
	raft := new(mockRaft)
	addr := "127.0.0.1:9000"
	raft.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(addr, tn, raft)
	raft.AssertExpectations(t)

	require.NotNil(t, coordinator)
	require.Equal(t, addr, coordinator.Address)
	require.NotNil(t, coordinator.memberStates)
	require.NotNil(t, coordinator.failedChainMemberCh)
	require.False(t, coordinator.isLeader)
}

func TestAddMember(t *testing.T) {
	tn := new(mockTransport)
	raft := new(mockRaft)
	addr := "127.0.0.1:9000"
	raft.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(addr, tn, raft)
	raft.AssertExpectations(t)

	memberID := "member-1"
	memberAddr := "127.0.0.2:9000"
	member := &chainnode.ChainMember{ID: memberID, Address: memberAddr}
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{member}, 0)

	raft.On("AddChainMember", mock.Anything, memberID, memberAddr).Return(config, nil).Once()
	tn.On("UpdateConfiguration", mock.Anything, memberAddr, mock.MatchedBy(func(r *chainnode.UpdateConfigurationRequest) bool {
		return r.Configuration.Equal(config)
	})).Return(&chainnode.UpdateConfigurationResponse{}, nil).Once()
	req := &AddMemberRequest{ID: memberID, Address: memberAddr}
	var resp AddMemberResponse
	err := coordinator.AddMember(context.Background(), req, &resp)
	require.NoError(t, err)
	tn.AssertExpectations(t)
	raft.AssertExpectations(t)
}

func TestRemoveMember(t *testing.T) {
	tn := new(mockTransport)
	raft := new(mockRaft)
	addr := "127.0.0.1:9000"
	raft.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(addr, tn, raft)
	raft.AssertExpectations(t)

	member1 := &chainnode.ChainMember{ID: "member-1", Address: "127.0.0.2:9000"}
	member2 := &chainnode.ChainMember{ID: "member-2", Address: "127.0.0.3:9000"}
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{member1}, 1)

	raft.On("RemoveChainMember", mock.Anything, member2.ID).Return(config, member2, nil).Once()
	tn.On(
		"UpdateConfiguration",
		mock.Anything,
		member1.Address,
		&chainnode.UpdateConfigurationRequest{Configuration: config},
	).Return(&chainnode.UpdateConfigurationResponse{}, nil).Once()
	tn.On(
		"UpdateConfiguration",
		mock.Anything,
		member2.Address,
		&chainnode.UpdateConfigurationRequest{Configuration: config},
	).Return(&chainnode.UpdateConfigurationResponse{}, nil).Once()
	req := &RemoveMemberRequest{ID: member2.ID}
	var resp RemoveMemberResponse
	err := coordinator.RemoveMember(context.Background(), req, &resp)
	require.NoError(t, err)
	raft.AssertExpectations(t)
	tn.AssertExpectations(t)
}

func TestReadMembershipConfiguration(t *testing.T) {
	tn := new(mockTransport)
	raft := new(mockRaft)
	addr := "127.0.0.1:9000"
	raft.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(addr, tn, raft)
	raft.AssertExpectations(t)

	member1 := &chainnode.ChainMember{ID: "member-1", Address: "127.0.0.1:9000"}
	member2 := &chainnode.ChainMember{ID: "member-2", Address: "127.0.0.2:9000"}

	expectedConfig := chainnode.NewConfiguration([]*chainnode.ChainMember{member1, member2}, 0)
	raft.On("ReadChainConfiguration", mock.Anything).Return(expectedConfig, nil).Once()
	req := &ReadChainConfigurationRequest{}
	var resp ReadChainConfigurationResponse
	err := coordinator.ReadMembershipConfiguration(context.Background(), req, &resp)
	require.NoError(t, err)
	require.Equal(t, expectedConfig, resp.Configuration)
	raft.AssertExpectations(t)
}

func TestOnHeartbeat(t *testing.T) {
	tn := new(mockTransport)
	raft := new(mockRaft)
	addr := "127.0.0.1:9000"
	raft.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(addr, tn, raft)
	raft.AssertExpectations(t)

	member1 := &chainnode.ChainMember{ID: "member-1", Address: "127.0.0.2:9000"}
	member2 := &chainnode.ChainMember{ID: "member-2", Address: "127.0.0.3:9000"}
	member3 := &chainnode.ChainMember{ID: "member-3", Address: "127.0.0.4:9000"}
	version := uint64(1)
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{member1, member2, member3}, version)

	// This coordinator is the leader and all pings to chain members are successful.
	coordinator.isLeader = true
	raft.On("ChainConfiguration").Return(config).Once()
	tn.On("Ping", mock.Anything, member1.Address, &chainnode.PingRequest{}).Return(&chainnode.PingResponse{Version: version}, nil).Once()
	tn.On("Ping", mock.Anything, member2.Address, &chainnode.PingRequest{}).Return(&chainnode.PingResponse{Version: version}, nil).Once()
	tn.On("Ping", mock.Anything, member3.Address, &chainnode.PingRequest{}).Return(&chainnode.PingResponse{Version: version}, nil).Once()
	require.NoError(t, coordinator.onHeartbeat(context.Background()))
	require.Contains(t, coordinator.memberStates, member1.ID)
	require.Contains(t, coordinator.memberStates, member2.ID)
	require.Contains(t, coordinator.memberStates, member3.ID)
	raft.AssertExpectations(t)
	tn.AssertExpectations(t)

	// This coordinator is the leader and a ping to one of the chain members fails.
	coordinator.failedChainMemberCh = make(chan any, 1)
	raft.On("ChainConfiguration").Return(config).Once()
	tn.On("Ping", mock.Anything, member1.Address, &chainnode.PingRequest{}).Return(&chainnode.PingResponse{Version: version}, nil).Once()
	tn.On("Ping", mock.Anything, member2.Address, &chainnode.PingRequest{}).Return(nil, errors.New("ping RPC failed")).Once()
	tn.On("Ping", mock.Anything, member3.Address, &chainnode.PingRequest{}).Return(&chainnode.PingResponse{Version: version}, nil).Once()
	require.Error(t, coordinator.onHeartbeat(context.Background()))
	require.Len(t, coordinator.failedChainMemberCh, 1)
	raft.AssertExpectations(t)
	tn.AssertExpectations(t)

	// This coordinator is the leader and a chain members has an out-of-date configuration.
	coordinator.configSyncCh = make(chan any, 1)
	raft.On("ChainConfiguration").Return(config).Once()
	tn.On("Ping", mock.Anything, member1.Address, &chainnode.PingRequest{}).Return(&chainnode.PingResponse{Version: version}, nil).Once()
	tn.On("Ping", mock.Anything, member2.Address, &chainnode.PingRequest{}).Return(&chainnode.PingResponse{Version: version}, nil).Once()
	tn.On("Ping", mock.Anything, member3.Address, &chainnode.PingRequest{}).Return(&chainnode.PingResponse{Version: version - 1}, nil).Once()
	require.NoError(t, coordinator.onHeartbeat(context.Background()))
	require.Len(t, coordinator.configSyncCh, 1)
	raft.AssertExpectations(t)
	tn.AssertExpectations(t)

	// This coordinator is not the leader. It should not send heartbeats.
	coordinator.isLeader = false
	coordinator.memberStates = make(map[string]*memberState)
	require.NoError(t, coordinator.onHeartbeat(context.Background()))
	require.Empty(t, coordinator.memberStates)
}

func TestOnFailedChainMember(t *testing.T) {
	tn := new(mockTransport)
	raft := new(mockRaft)
	addr := "127.0.0.1:9000"
	raft.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(addr, tn, raft)
	raft.AssertExpectations(t)

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
	require.NoError(t, coordinator.onFailedChainMember(context.Background()))

	// This coordinator is the leader and one of the chain members has not been contacted in a while.
	// It should attempt to remove the failed member.
	coordinator.memberStates = map[string]*memberState{
		member1.ID: {lastContact: time.Now()},
		member2.ID: {lastContact: time.Now().Add(-60 * time.Second)},
		member3.ID: {lastContact: time.Now()},
	}
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{member1, member3}, 0)
	raft.On("RemoveChainMember", mock.Anything, member2.ID).Return(config, member2, nil)
	tn.On("UpdateConfiguration", mock.Anything, member1.Address, mock.MatchedBy(func(r *chainnode.UpdateConfigurationRequest) bool {
		return r.Configuration.Equal(config)
	})).Return(&chainnode.UpdateConfigurationResponse{}, nil)
	tn.On("UpdateConfiguration", mock.Anything, member2.Address, mock.MatchedBy(func(r *chainnode.UpdateConfigurationRequest) bool {
		return r.Configuration.Equal(config)
	})).Return(&chainnode.UpdateConfigurationResponse{}, errors.New("RPC failed"))
	tn.On("UpdateConfiguration", mock.Anything, member3.Address, mock.MatchedBy(func(r *chainnode.UpdateConfigurationRequest) bool {
		return r.Configuration.Equal(config)
	})).Return(&chainnode.UpdateConfigurationResponse{}, nil)
	require.NoError(t, coordinator.onFailedChainMember(context.Background()))
	raft.AssertExpectations(t)
	tn.AssertExpectations(t)

	// This coordinator is not the leader. It should not attempt to remove chain members.
	coordinator.isLeader = false
	require.NoError(t, coordinator.onFailedChainMember(context.Background()))
}

func TestOnLeadershipChange(t *testing.T) {
	tn := new(mockTransport)
	raft := new(mockRaft)
	addr := "127.0.0.1:9000"
	raft.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(addr, tn, raft)
	raft.AssertExpectations(t)

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
	tn := new(mockTransport)
	raft := new(mockRaft)
	addr := "127.0.0.1:9000"
	raft.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(addr, tn, raft)
	raft.AssertExpectations(t)

	member1 := &chainnode.ChainMember{ID: "member-1", Address: "127.0.0.2:9000"}
	member2 := &chainnode.ChainMember{ID: "member-2", Address: "127.0.0.3:9000"}
	member3 := &chainnode.ChainMember{ID: "member-3", Address: "127.0.0.4:9000"}
	version := uint64(3)
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{member1, member2, member3}, version)

	// This coordinator is the leader and all chain members have the correct chain configuration version.
	// It should not attempt to update any of their configurations.
	raft.On("ReadChainConfiguration", mock.Anything).Return(config, nil).Once()
	coordinator.isLeader = true
	coordinator.memberStates = map[string]*memberState{
		member1.ID: {lastContact: time.Now(), configVersion: version},
		member2.ID: {lastContact: time.Now(), configVersion: version},
		member3.ID: {lastContact: time.Now(), configVersion: version},
	}
	require.NoError(t, coordinator.onConfigSync(context.Background()))
	raft.AssertExpectations(t)

	// This coordinator is the leader and one of the chain members has an out-of-date configuration.
	// It should attempt to update that member's configuration.
	raft.On("ReadChainConfiguration", mock.Anything).Return(config, nil).Once()
	tn.On("UpdateConfiguration", mock.Anything, member2.Address, mock.MatchedBy(func(r *chainnode.UpdateConfigurationRequest) bool {
		return r.Configuration.Equal(config)
	})).Return(&chainnode.UpdateConfigurationResponse{}, nil)
	coordinator.memberStates = map[string]*memberState{
		member1.ID: {lastContact: time.Now(), configVersion: version},
		member2.ID: {lastContact: time.Now(), configVersion: version - 1},
		member3.ID: {lastContact: time.Now(), configVersion: version},
	}
	require.NoError(t, coordinator.onConfigSync(context.Background()))
	raft.AssertExpectations(t)
	tn.AssertExpectations(t)

	// This coordinator is not the leader. It should not attempt to update the configuration of any chain members.
	coordinator.isLeader = false
	require.NoError(t, coordinator.onFailedChainMember(context.Background()))
}
