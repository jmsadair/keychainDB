package coordinator

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/jmsadair/keychain/chain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockRaft struct {
	mock.Mock
}

func (m *mockRaft) AddChainMember(ctx context.Context, member net.Addr) (*chain.Configuration, error) {
	args := m.MethodCalled("AddChainMember", member.String())
	return args.Get(0).(*chain.Configuration), args.Error(1)
}

func (m *mockRaft) RemoveChainMember(ctx context.Context, member net.Addr) (*chain.Configuration, error) {
	args := m.MethodCalled("RemoveChainMember", member.String())
	return args.Get(0).(*chain.Configuration), args.Error(1)
}

func (m *mockRaft) ReadChainConfiguration(ctx context.Context) (*chain.Configuration, error) {
	args := m.MethodCalled("ReadChainConfiguration")
	return args.Get(0).(*chain.Configuration), args.Error(1)
}

func (m *mockRaft) LeaderCh() <-chan bool {
	args := m.MethodCalled("LeaderCh")
	return args.Get(0).(chan bool)
}

func (m *mockRaft) ChainConfiguration() *chain.Configuration {
	args := m.MethodCalled("ChainConfiguration")
	return args.Get(0).(*chain.Configuration)
}

type mockTransport struct {
	mock.Mock
}

func (m *mockTransport) Ping(ctx context.Context, address net.Addr) error {
	args := m.MethodCalled("Ping", address.String())
	return args.Error(0)
}

func (m *mockTransport) UpdateConfiguration(ctx context.Context, address net.Addr, config *chain.Configuration) error {
	args := m.MethodCalled("UpdateConfiguration", address.String())
	return args.Error(0)
}

func TestNewCoordinator(t *testing.T) {
	tn := new(mockTransport)
	raft := new(mockRaft)
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:9000")
	require.NoError(t, err)

	raft.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(addr, tn, raft)
	raft.AssertExpectations(t)
	require.NotNil(t, coordinator)
	require.Equal(t, addr, coordinator.address)
	require.NotNil(t, coordinator.lastContacted)
	require.NotNil(t, coordinator.failedChainMemberCh)
	require.False(t, coordinator.isLeader)
}

func TestAddMember(t *testing.T) {
	tn := new(mockTransport)
	raft := new(mockRaft)
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:9000")
	require.NoError(t, err)
	raft.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(addr, tn, raft)
	raft.AssertExpectations(t)

	member, err := net.ResolveTCPAddr("tcp", "127.0.0.2:9000")
	require.NoError(t, err)
	config, err := chain.NewConfiguration([]net.Addr{member})
	require.NoError(t, err)

	raft.On("AddChainMember", member.String()).Return(config, nil).Once()
	tn.On("UpdateConfiguration", member.String()).Return(nil).Once()
	err = coordinator.AddMember(context.Background(), member)
	require.NoError(t, err)
	tn.AssertExpectations(t)
	raft.AssertExpectations(t)
}

func TestRemoveMember(t *testing.T) {
	tn := new(mockTransport)
	raft := new(mockRaft)
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:9000")
	require.NoError(t, err)
	raft.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(addr, tn, raft)
	raft.AssertExpectations(t)

	member, err := net.ResolveTCPAddr("tcp", "127.0.0.2:9000")
	require.NoError(t, err)
	config, err := chain.NewConfiguration([]net.Addr{})
	require.NoError(t, err)

	raft.On("RemoveChainMember", member.String()).Return(config, nil).Once()
	err = coordinator.RemoveMember(context.Background(), member)
	require.NoError(t, err)
	raft.AssertExpectations(t)
}

func TestReadMembershipConfiguration(t *testing.T) {
	tn := new(mockTransport)
	raft := new(mockRaft)
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:9000")
	require.NoError(t, err)
	raft.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(addr, tn, raft)
	raft.AssertExpectations(t)

	member1, err := net.ResolveTCPAddr("tcp", "127.0.0.1:9000")
	require.NoError(t, err)
	member2, err := net.ResolveTCPAddr("tcp", "127.0.0.2:9000")
	require.NoError(t, err)

	expectedConfig, err := chain.NewConfiguration([]net.Addr{member1, member2})
	require.NoError(t, err)
	raft.On("ReadChainConfiguration").Return(expectedConfig, nil).Once()
	config, err := coordinator.ReadMembershipConfiguration(context.Background())
	require.NoError(t, err)
	require.Equal(t, expectedConfig, config)
	raft.AssertExpectations(t)
}

func TestOnHeartbeat(t *testing.T) {
	tn := new(mockTransport)
	raft := new(mockRaft)
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:9000")
	require.NoError(t, err)
	raft.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(addr, tn, raft)
	raft.AssertExpectations(t)

	member1, err := net.ResolveTCPAddr("tcp", "127.0.0.2:9000")
	require.NoError(t, err)
	member2, err := net.ResolveTCPAddr("tcp", "127.0.0.3:9000")
	require.NoError(t, err)
	member3, err := net.ResolveTCPAddr("tcp", "127.0.0.4:9000")
	require.NoError(t, err)
	config, err := chain.NewConfiguration([]net.Addr{member1, member2, member3})
	require.NoError(t, err)

	// This coordinator is the leader and all pings to chain members are successful.
	coordinator.isLeader = true
	raft.On("ChainConfiguration").Return(config).Once()
	tn.On("Ping", member1.String()).Return(nil).Once()
	tn.On("Ping", member2.String()).Return(nil).Once()
	tn.On("Ping", member3.String()).Return(nil).Once()
	require.NoError(t, coordinator.onHeartbeat(context.Background()))
	require.Contains(t, coordinator.lastContacted, member1.String())
	require.Contains(t, coordinator.lastContacted, member2.String())
	require.Contains(t, coordinator.lastContacted, member3.String())
	raft.AssertExpectations(t)
	tn.AssertExpectations(t)

	// This coordinator is the leader and a ping to one of the chain members fails.
	coordinator.failedChainMemberCh = make(chan any, 1)
	raft.On("ChainConfiguration").Return(config).Once()
	tn.On("Ping", member1.String()).Return(nil).Once()
	tn.On("Ping", member2.String()).Return(errors.New("ping RPC failed")).Once()
	tn.On("Ping", member3.String()).Return(nil).Once()
	require.Error(t, coordinator.onHeartbeat(context.Background()))
	require.Len(t, coordinator.failedChainMemberCh, 1)
	raft.AssertExpectations(t)
	tn.AssertExpectations(t)

	// This coordinator is not the leader. It should not send heartbeats.
	coordinator.isLeader = false
	coordinator.lastContacted = make(map[string]time.Time)
	require.NoError(t, coordinator.onHeartbeat(context.Background()))
	require.Empty(t, coordinator.lastContacted)
}

func TestOnFailedChainMember(t *testing.T) {
	tn := new(mockTransport)
	raft := new(mockRaft)
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:9000")
	require.NoError(t, err)
	raft.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(addr, tn, raft)
	raft.AssertExpectations(t)

	member1, err := net.ResolveTCPAddr("tcp", "127.0.0.2:9000")
	require.NoError(t, err)
	member2, err := net.ResolveTCPAddr("tcp", "127.0.0.3:9000")
	require.NoError(t, err)
	member3, err := net.ResolveTCPAddr("tcp", "127.0.0.4:9000")
	require.NoError(t, err)

	// This coordinator is the leader and all chain members have been recently contacted.
	// It should not attempt to remove any of them.
	coordinator.isLeader = true
	coordinator.lastContacted = map[string]time.Time{
		member1.String(): time.Now(),
		member2.String(): time.Now(),
		member3.String(): time.Now(),
	}
	require.NoError(t, coordinator.onFailedChainMember(context.Background()))

	// This coordinator is the leader and one of the chain members has not been contacted in a while.
	// It should attempt to remove the failed member.
	coordinator.lastContacted = map[string]time.Time{
		member1.String(): time.Now(),
		member2.String(): time.Now().Add(-60 * time.Second),
		member3.String(): time.Now(),
	}
	config, err := chain.NewConfiguration([]net.Addr{member1, member3})
	require.NoError(t, err)
	raft.On("RemoveChainMember", member2.String()).Return(config, nil)
	tn.On("UpdateConfiguration", member1.String()).Return(nil)
	tn.On("UpdateConfiguration", member3.String()).Return(nil)
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
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:9000")
	require.NoError(t, err)
	raft.On("LeaderCh").Return(make(chan bool)).Once()
	coordinator := NewCoordinator(addr, tn, raft)
	raft.AssertExpectations(t)

	member1, err := net.ResolveTCPAddr("tcp", "127.0.0.2:9000")
	require.NoError(t, err)

	coordinator.lastContacted = map[string]time.Time{
		member1.String(): time.Now(),
	}
	coordinator.onLeadershipChange(true)
	require.True(t, coordinator.isLeader)
	require.Empty(t, coordinator.lastContacted)

	coordinator.lastContacted = map[string]time.Time{
		member1.String(): time.Now(),
	}
	coordinator.onLeadershipChange(false)
	require.False(t, coordinator.isLeader)
	require.Empty(t, coordinator.lastContacted)
}
