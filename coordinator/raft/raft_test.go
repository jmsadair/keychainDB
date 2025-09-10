package raft

import (
	"context"
	"testing"
	"time"

	chainnode "github.com/jmsadair/keychain/chain/node"
	"github.com/stretchr/testify/require"
)

const leaderTimeout = 5 * time.Second

func newTestRaftBackend(t *testing.T, nodeID string, address string, bootstrap bool) *RaftBackend {
	storeDir := t.TempDir()
	snapshotDir := t.TempDir()

	rb, err := NewRaftBackend(nodeID, address, storeDir, snapshotDir, bootstrap)
	require.NoError(t, err)

	return rb
}

func TestJoinCluster(t *testing.T) {
	nodeID1 := "leader"
	addr1 := "127.0.0.1:9001"
	leader := newTestRaftBackend(t, nodeID1, addr1, true)
	defer func() {
		require.NoError(t, leader.Shutdown())
	}()

	nodeID2 := "follower-1"
	addr2 := "127.0.0.2:9001"
	follower1 := newTestRaftBackend(t, nodeID2, addr2, false)
	defer func() {
		require.NoError(t, follower1.Shutdown())
	}()

	nodeID3 := "follower-3"
	addr3 := "127.0.0.3:9001"
	follower2 := newTestRaftBackend(t, nodeID3, addr3, false)
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
	require.Equal(t, addr1, status.Members[nodeID1])
	require.Equal(t, addr2, status.Members[nodeID2])
	err = leader.JoinCluster(context.TODO(), nodeID3, addr3)
	require.NoError(t, err)
	status, err = leader.ClusterStatus()
	require.NoError(t, err)
	require.Len(t, status.Members, 3)
	require.Contains(t, status.Members, nodeID1)
	require.Contains(t, status.Members, nodeID2)
	require.Contains(t, status.Members, nodeID3)
	require.Equal(t, addr1, status.Members[nodeID1])
	require.Equal(t, addr2, status.Members[nodeID2])
	require.Equal(t, addr3, status.Members[nodeID3])

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
	require.Equal(t, addr1, status.Members[nodeID1])
	require.Equal(t, addr2, status.Members[nodeID2])
	err = leader.RemoveFromCluster(context.TODO(), nodeID2)
	require.NoError(t, err)
	status, err = leader.ClusterStatus()
	require.NoError(t, err)
	require.Len(t, status.Members, 1)
	require.Contains(t, status.Members, nodeID1)
	require.Equal(t, addr1, status.Members[nodeID1])
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
	memberID1 := "member-1"
	memberAddr1 := "127.0.0.2:8080"
	newConfig, err := leader.AddChainMember(context.TODO(), memberID1, memberAddr1)
	require.NoError(t, err)
	require.True(t, newConfig.IsMemberByID(memberID1))
	memberID2 := "member-2"
	memberAddr2 := "127.0.0.3:8080"
	newConfig, err = leader.AddChainMember(context.TODO(), memberID2, memberAddr2)
	require.NoError(t, err)
	require.True(t, newConfig.IsMemberByID(memberID2))
	memberID3 := "member-3"
	memberAddr3 := "127.0.0.4:8080"
	newConfig, err = leader.AddChainMember(context.TODO(), memberID3, memberAddr3)
	require.NoError(t, err)
	require.True(t, newConfig.IsMemberByID(memberID3))

	readConfig, err = leader.ReadChainConfiguration(context.TODO())
	require.NoError(t, err)
	member1 := &chainnode.ChainMember{ID: memberID1, Address: memberAddr1}
	member2 := &chainnode.ChainMember{ID: memberID2, Address: memberAddr2}
	member3 := &chainnode.ChainMember{ID: memberID3, Address: memberAddr3}
	expectedConfig, err := chainnode.NewConfiguration([]*chainnode.ChainMember{member1, member2, member3})
	require.NoError(t, err)
	require.True(t, expectedConfig.Equal(readConfig))

	// Remove two of the chain members that were added.
	newConfig, err = leader.RemoveChainMember(context.TODO(), memberID1)
	require.NoError(t, err)
	require.False(t, newConfig.IsMemberByID(memberID1))
	newConfig, err = leader.RemoveChainMember(context.TODO(), memberID3)
	require.NoError(t, err)
	require.False(t, newConfig.IsMemberByID(memberID3))

	readConfig, err = leader.ReadChainConfiguration(context.TODO())
	require.NoError(t, err)
	expectedConfig, err = chainnode.NewConfiguration([]*chainnode.ChainMember{member2})
	require.NoError(t, err)
	require.True(t, expectedConfig.Equal(readConfig))
}
