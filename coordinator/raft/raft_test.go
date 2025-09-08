package raft

import (
	"context"
	"net"
	"testing"
	"time"

	chainnode "github.com/jmsadair/keychain/chain/node"
	"github.com/stretchr/testify/require"
)

const leaderTimeout = 5 * time.Second

func newTestRaftBackend(t *testing.T, nodeID string, address string, bootstrap bool) *RaftBackend {
	storeDir := t.TempDir()
	snapshotDir := t.TempDir()
	bindAddr, err := net.ResolveTCPAddr("tcp", address)
	require.NoError(t, err)

	rb, err := NewRaftBackend(nodeID, bindAddr, storeDir, snapshotDir, bootstrap)
	require.NoError(t, err)

	return rb
}

func TestJoinCluster(t *testing.T) {
	nodeID1 := "leader"
	addr1, err := net.ResolveTCPAddr("tcp", "127.0.0.1:9001")
	require.NoError(t, err)
	leader := newTestRaftBackend(t, nodeID1, addr1.String(), true)
	defer func() {
		require.NoError(t, leader.Shutdown())
	}()

	nodeID2 := "follower-1"
	addr2, err := net.ResolveTCPAddr("tcp", "127.0.0.2:9001")
	require.NoError(t, err)
	follower1 := newTestRaftBackend(t, nodeID2, addr2.String(), false)
	defer func() {
		require.NoError(t, follower1.Shutdown())
	}()

	nodeID3 := "follower-3"
	addr3, err := net.ResolveTCPAddr("tcp", "127.0.0.3:9001")
	require.NoError(t, err)
	follower2 := newTestRaftBackend(t, nodeID3, addr3.String(), false)
	defer func() {
		require.NoError(t, follower2.Shutdown())
	}()

	leaderCh := leader.LeadershipCh()
	select {
	case isLeader := <-leaderCh:
		require.True(t, isLeader)
	case <-time.After(leaderTimeout):
		t.Fatal("failed to elect leader")
	}

	status, err := leader.ClusterStatus()
	require.NoError(t, err)
	require.Equal(t, nodeID1, status.Leader)
	require.Len(t, status.Members, 1)
	require.Contains(t, status.Members, nodeID1)

	// Add two nodes to the cluster.
	err = leader.JoinCluster(context.TODO(), nodeID2, addr2)
	require.NoError(t, err)
	status, err = leader.ClusterStatus()
	require.NoError(t, err)
	require.Len(t, status.Members, 2)
	require.Contains(t, status.Members, nodeID1)
	require.Contains(t, status.Members, nodeID2)
	require.Equal(t, addr1.String(), status.Members[nodeID1])
	require.Equal(t, addr2.String(), status.Members[nodeID2])
	err = leader.JoinCluster(context.TODO(), nodeID3, addr3)
	require.NoError(t, err)
	status, err = leader.ClusterStatus()
	require.NoError(t, err)
	require.Len(t, status.Members, 3)
	require.Contains(t, status.Members, nodeID1)
	require.Contains(t, status.Members, nodeID2)
	require.Contains(t, status.Members, nodeID3)
	require.Equal(t, addr1.String(), status.Members[nodeID1])
	require.Equal(t, addr2.String(), status.Members[nodeID2])
	require.Equal(t, addr3.String(), status.Members[nodeID3])

	// Ensure an error is returned if the caller attempts to add a node that has an address
	// or ID that matches the address or ID of an existing node in the cluster.
	require.ErrorIs(t, leader.JoinCluster(context.TODO(), nodeID2, addr3), ErrNodeExists)

	// Remove the nodes that were added.
	err = leader.RemoveFromCluster(context.TODO(), nodeID3)
	require.NoError(t, err)
	status, err = leader.ClusterStatus()
	require.NoError(t, err)
	require.Len(t, status.Members, 2)
	require.Contains(t, status.Members, nodeID1)
	require.Contains(t, status.Members, nodeID2)
	require.Equal(t, addr1.String(), status.Members[nodeID1])
	require.Equal(t, addr2.String(), status.Members[nodeID2])
	err = leader.RemoveFromCluster(context.TODO(), nodeID2)
	require.NoError(t, err)
	status, err = leader.ClusterStatus()
	require.NoError(t, err)
	require.Len(t, status.Members, 1)
	require.Contains(t, status.Members, nodeID1)
	require.Equal(t, addr1.String(), status.Members[nodeID1])
}

func TestAddRemoveChainMember(t *testing.T) {
	nodeID1 := "leader"
	addr1 := "127.0.0.1:8080"
	leader := newTestRaftBackend(t, nodeID1, addr1, true)
	defer func() {
		require.NoError(t, leader.Shutdown())
	}()

	leaderCh := leader.LeadershipCh()
	select {
	case isLeader := <-leaderCh:
		require.True(t, isLeader)
	case <-time.After(leaderTimeout):
		t.Fatal("failed to elect leader")
	}

	// Chain configuration should initially not contain any members.
	readConfig, err := leader.ReadChainConfiguration(context.TODO())
	require.NoError(t, err)
	require.True(t, readConfig.Equal(chainnode.EmptyChain))

	// Add three members to the chain.
	memberAddr1, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	newConfig, err := leader.AddChainMember(context.TODO(), memberAddr1)
	require.NoError(t, err)
	require.True(t, newConfig.IsMember(memberAddr1))
	memberAddr2, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)
	newConfig, err = leader.AddChainMember(context.TODO(), memberAddr2)
	require.NoError(t, err)
	require.True(t, newConfig.IsMember(memberAddr2))
	memberAddr3, err := net.ResolveTCPAddr("tcp", "127.0.0.4:8080")
	require.NoError(t, err)
	newConfig, err = leader.AddChainMember(context.TODO(), memberAddr3)
	require.NoError(t, err)
	require.True(t, newConfig.IsMember(memberAddr3))

	readConfig, err = leader.ReadChainConfiguration(context.TODO())
	require.NoError(t, err)
	expectedConfig, err := chainnode.NewConfiguration([]net.Addr{memberAddr1, memberAddr2, memberAddr3})
	require.NoError(t, err)
	require.True(t, expectedConfig.Equal(readConfig))

	// Remove two of the chain members that were added.
	newConfig, err = leader.RemoveChainMember(context.TODO(), memberAddr1)
	require.NoError(t, err)
	require.False(t, newConfig.IsMember(memberAddr1))
	newConfig, err = leader.RemoveChainMember(context.TODO(), memberAddr3)
	require.NoError(t, err)
	require.False(t, newConfig.IsMember(memberAddr3))

	readConfig, err = leader.ReadChainConfiguration(context.TODO())
	require.NoError(t, err)
	expectedConfig, err = chainnode.NewConfiguration([]net.Addr{memberAddr2})
	require.NoError(t, err)
	require.True(t, expectedConfig.Equal(readConfig))
}
