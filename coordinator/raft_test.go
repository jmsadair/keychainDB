package coordinator

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/jmsadair/keychainDB/chain"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type testRaftBackend struct {
	rb  *RaftBackend
	srv *grpc.Server
}

func (trb *testRaftBackend) start(t *testing.T) error {
	ln, err := net.Listen("tcp", trb.rb.Address)
	require.NoError(t, err)
	go func() {
		trb.srv.Serve(ln)
	}()
	return nil
}

func (trb *testRaftBackend) stop(t *testing.T) {
	trb.srv.Stop()
	require.NoError(t, trb.rb.Shutdown())
}

func waitForLeader(t *testing.T, expectedLeader *RaftBackend) {
	require.Eventually(t, func() bool {
		_, leader := expectedLeader.LeaderWithID()
		return leader == expectedLeader.ID
	}, 5*time.Second, 100*time.Millisecond)
}

func newTestRaftBackend(t *testing.T, id string, address string, bootstrap bool) *testRaftBackend {
	rb, err := NewRaftBackend(id, address, t.TempDir(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	srv := grpc.NewServer()
	rb.Register(srv)

	if bootstrap {
		config := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(id),
					Address: raft.ServerAddress(address),
				},
			},
		}
		err = rb.Bootstrap(config)
		require.NoError(t, err)
	}

	testBackend := &testRaftBackend{rb: rb, srv: srv}
	testBackend.start(t)
	t.Cleanup(func() {
		testBackend.stop(t)
	})

	return testBackend
}

func TestJoinRemoveFromCluster(t *testing.T) {
	nodeID1 := "leader"
	addr1 := "127.0.0.1:9001"
	leader := newTestRaftBackend(t, nodeID1, addr1, true)
	nodeID2 := "follower-1"
	addr2 := "127.0.0.1:9002"
	newTestRaftBackend(t, nodeID2, addr2, false)
	nodeID3 := "follower-2"
	addr3 := "127.0.0.1:9003"
	newTestRaftBackend(t, nodeID3, addr3, false)

	// Wait for leader to be elected
	waitForLeader(t, leader.rb)

	// Verify initial cluster state
	status, err := leader.rb.ClusterStatus()
	require.NoError(t, err)
	require.Equal(t, nodeID1, status.Leader)
	require.Len(t, status.Members, 1)
	require.Contains(t, status.Members, nodeID1)

	// Add two nodes to the cluster.
	err = leader.rb.JoinCluster(context.Background(), nodeID2, addr2)
	require.NoError(t, err)
	status, err = leader.rb.ClusterStatus()
	require.NoError(t, err)
	require.Len(t, status.Members, 2)
	require.Contains(t, status.Members, nodeID1)
	require.Contains(t, status.Members, nodeID2)
	require.Equal(t, addr1, status.Members[nodeID1])
	require.Equal(t, addr2, status.Members[nodeID2])

	err = leader.rb.JoinCluster(context.Background(), nodeID3, addr3)
	require.NoError(t, err)
	status, err = leader.rb.ClusterStatus()
	require.NoError(t, err)
	require.Len(t, status.Members, 3)
	require.Contains(t, status.Members, nodeID1)
	require.Contains(t, status.Members, nodeID2)
	require.Contains(t, status.Members, nodeID3)
	require.Equal(t, addr1, status.Members[nodeID1])
	require.Equal(t, addr2, status.Members[nodeID2])
	require.Equal(t, addr3, status.Members[nodeID3])

	// Ensure an error is returned if an existing node in the cluster has the same ID or address.
	require.ErrorIs(t, leader.rb.JoinCluster(context.Background(), nodeID2, addr3), ErrNodeExists)

	// Remove the nodes that were added.
	err = leader.rb.RemoveFromCluster(context.Background(), nodeID3)
	require.NoError(t, err)
	status, err = leader.rb.ClusterStatus()
	require.NoError(t, err)
	require.Len(t, status.Members, 2)
	require.Contains(t, status.Members, nodeID1)
	require.Contains(t, status.Members, nodeID2)
	require.Equal(t, addr1, status.Members[nodeID1])
	require.Equal(t, addr2, status.Members[nodeID2])

	err = leader.rb.RemoveFromCluster(context.Background(), nodeID2)
	require.NoError(t, err)
	status, err = leader.rb.ClusterStatus()
	require.NoError(t, err)
	require.Len(t, status.Members, 1)
	require.Contains(t, status.Members, nodeID1)
	require.Equal(t, addr1, status.Members[nodeID1])
}

func TestAddRemoveMember(t *testing.T) {
	nodeID1 := "leader"
	addr1 := "127.0.0.1:9001"
	leader := newTestRaftBackend(t, nodeID1, addr1, true)

	// Wait for leader to be elected
	waitForLeader(t, leader.rb)

	// Chain configuration should initially not contain any members.
	readConfig, err := leader.rb.GetMembers(context.Background())
	require.NoError(t, err)
	require.True(t, readConfig.Equal(chain.EmptyChain))

	// Add three members to the chain.
	memberID1 := "member-1"
	memberAddr1 := "127.0.0.2:8080"
	newConfig, err := leader.rb.AddMember(context.Background(), memberID1, memberAddr1)
	require.NoError(t, err)
	require.True(t, newConfig.IsMemberByID(memberID1))
	memberID2 := "member-2"
	memberAddr2 := "127.0.0.3:8080"
	newConfig, err = leader.rb.AddMember(context.Background(), memberID2, memberAddr2)
	require.NoError(t, err)
	require.True(t, newConfig.IsMemberByID(memberID2))
	memberID3 := "member-3"
	memberAddr3 := "127.0.0.4:8080"
	newConfig, err = leader.rb.AddMember(context.Background(), memberID3, memberAddr3)
	require.NoError(t, err)
	require.True(t, newConfig.IsMemberByID(memberID3))

	// Verify the configuration contains all the members.
	readConfig, err = leader.rb.GetMembers(context.Background())
	require.NoError(t, err)
	member1 := &chain.ChainMember{ID: memberID1, Address: memberAddr1}
	member2 := &chain.ChainMember{ID: memberID2, Address: memberAddr2}
	member3 := &chain.ChainMember{ID: memberID3, Address: memberAddr3}
	expectedConfig := chain.NewConfiguration([]*chain.ChainMember{member1, member2, member3}, 3)
	require.True(t, expectedConfig.Equal(readConfig))

	// Remove two of the chain members that were added.
	newConfig, removed, err := leader.rb.RemoveMember(context.Background(), memberID1)
	require.NoError(t, err)
	require.NotNil(t, removed)
	require.Equal(t, memberID1, removed.ID)
	require.Equal(t, memberAddr1, removed.Address)
	require.False(t, newConfig.IsMemberByID(memberID1))
	newConfig, removed, err = leader.rb.RemoveMember(context.Background(), memberID3)
	require.NoError(t, err)
	require.NotNil(t, removed)
	require.Equal(t, memberID3, removed.ID)
	require.Equal(t, memberAddr3, removed.Address)
	require.False(t, newConfig.IsMemberByID(memberID3))

	readConfig, err = leader.rb.GetMembers(context.Background())
	require.NoError(t, err)
	expectedConfig = chain.NewConfiguration([]*chain.ChainMember{member2}, 5)
	require.True(t, expectedConfig.Equal(readConfig))
}
