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
	args := m.MethodCalled("JoinCluster", id, address)
	return args.Error(0)
}

func (m *mockRaft) RemoveFromCluster(ctx context.Context, id string) error {
	args := m.MethodCalled("RemoveFromCluster", id)
	return args.Error(0)
}

func (m *mockRaft) AddChainMember(ctx context.Context, id, address string) (*chainnode.Configuration, error) {
	args := m.MethodCalled("AddChainMember", id, address)
	return args.Get(0).(*chainnode.Configuration), args.Error(1)
}

func (m *mockRaft) RemoveChainMember(ctx context.Context, id string) (*chainnode.Configuration, error) {
	args := m.MethodCalled("RemoveChainMember", id)
	return args.Get(0).(*chainnode.Configuration), args.Error(1)
}

func (m *mockRaft) ReadChainConfiguration(ctx context.Context) (*chainnode.Configuration, error) {
	args := m.MethodCalled("ReadChainConfiguration")
	return args.Get(0).(*chainnode.Configuration), args.Error(1)
}

func (m *mockRaft) LeaderCh() <-chan bool {
	args := m.MethodCalled("LeaderCh")
	return args.Get(0).(chan bool)
}

func (m *mockRaft) ChainConfiguration() *chainnode.Configuration {
	args := m.MethodCalled("ChainConfiguration")
	return args.Get(0).(*chainnode.Configuration)
}

func (m *mockRaft) ClusterStatus() (*raft.Status, error) {
	args := m.MethodCalled("ClusterStatus")
	return args.Get(0).(*raft.Status), args.Error(1)
}

type mockTransport struct {
	mock.Mock
}

func (m *mockTransport) Ping(ctx context.Context, address string, request *chainnode.PingRequest, response *chainnode.PingResponse) error {
	args := m.MethodCalled("Ping", address)
	return args.Error(0)
}

func (m *mockTransport) UpdateConfiguration(ctx context.Context, address string, request *chainnode.UpdateConfigurationRequest, response *chainnode.UpdateConfigurationResponse) error {
	args := m.MethodCalled("UpdateConfiguration", address)
	return args.Error(0)
}

func TestNewCoordinator(t *testing.T) {
	tn := new(mockTransport)
	raft := new(mockRaft)
	addr := "127.0.0.1:9000"
	raft.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(addr, tn, raft)
	raft.AssertExpectations(t)

	require.NotNil(t, coordinator)
	require.Equal(t, addr, coordinator.address)
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

	raft.On("AddChainMember", memberID, memberAddr).Return(config, nil).Once()
	tn.On("UpdateConfiguration", memberAddr).Return(nil).Once()
	err := coordinator.AddMember(context.Background(), memberID, memberAddr)
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

	memberID := "member-1"
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{}, 0)

	raft.On("RemoveChainMember", memberID).Return(config, nil).Once()
	err := coordinator.RemoveMember(context.Background(), memberID)
	require.NoError(t, err)
	raft.AssertExpectations(t)
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
	raft.On("ReadChainConfiguration").Return(expectedConfig, nil).Once()
	config, err := coordinator.ReadMembershipConfiguration(context.Background())
	require.NoError(t, err)
	require.Equal(t, expectedConfig, config)
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
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{member1, member2, member3}, 0)

	// This coordinator is the leader and all pings to chain members are successful.
	coordinator.isLeader = true
	raft.On("ChainConfiguration").Return(config).Once()
	tn.On("Ping", member1.Address).Return(nil).Once()
	tn.On("Ping", member2.Address).Return(nil).Once()
	tn.On("Ping", member3.Address).Return(nil).Once()
	require.NoError(t, coordinator.onHeartbeat(context.Background()))
	require.Contains(t, coordinator.memberStates, member1.ID)
	require.Contains(t, coordinator.memberStates, member2.ID)
	require.Contains(t, coordinator.memberStates, member3.ID)
	raft.AssertExpectations(t)
	tn.AssertExpectations(t)

	// This coordinator is the leader and a ping to one of the chain members fails.
	coordinator.failedChainMemberCh = make(chan any, 1)
	raft.On("ChainConfiguration").Return(config).Once()
	tn.On("Ping", member1.Address).Return(nil).Once()
	tn.On("Ping", member2.Address).Return(errors.New("ping RPC failed")).Once()
	tn.On("Ping", member3.Address).Return(nil).Once()
	require.Error(t, coordinator.onHeartbeat(context.Background()))
	require.Len(t, coordinator.failedChainMemberCh, 1)
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
		member1.ID: &memberState{lastContact: time.Now()},
		member2.ID: &memberState{lastContact: time.Now()},
		member3.ID: &memberState{lastContact: time.Now()},
	}
	require.NoError(t, coordinator.onFailedChainMember(context.Background()))

	// This coordinator is the leader and one of the chain members has not been contacted in a while.
	// It should attempt to remove the failed member.
	coordinator.memberStates = map[string]*memberState{
		member1.ID: &memberState{lastContact: time.Now()},
		member2.ID: &memberState{lastContact: time.Now().Add(-60 * time.Second)},
		member3.ID: &memberState{lastContact: time.Now()},
	}
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{member1, member3}, 0)
	raft.On("RemoveChainMember", member2.ID).Return(config, nil)
	tn.On("UpdateConfiguration", member1.Address).Return(nil)
	tn.On("UpdateConfiguration", member3.Address).Return(nil)
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
		memberID: &memberState{lastContact: time.Now()},
	}
	coordinator.onLeadershipChange(true)
	require.True(t, coordinator.isLeader)
	require.Empty(t, coordinator.memberStates)

	coordinator.memberStates = map[string]*memberState{
		memberID: &memberState{lastContact: time.Now()},
	}
	coordinator.onLeadershipChange(false)
	require.False(t, coordinator.isLeader)
	require.Empty(t, coordinator.memberStates)
}
