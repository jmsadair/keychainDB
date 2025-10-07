package tests

import (
	"testing"

	chainnode "github.com/jmsadair/keychain/chain/node"
	"github.com/stretchr/testify/require"
)

func TestJoinClusterRemoveFromClusterHTTP(t *testing.T) {
	cluster := newTestCoordinatorCluster(t, "coordinator-1")
	defer cluster.stop()

	followerID1 := "coordinator-2"
	followerID2 := "coordinator-3"
	cluster.addServerToClusterHTTP(t, followerID1)
	cluster.addServerToClusterHTTP(t, followerID2)

	clusterStatusResp := cluster.clusterStatusHTTP(t)
	require.Equal(t, cluster.leaderID(), clusterStatusResp.GetLeader())
	require.Equal(t, cluster.clusterMembers(), clusterStatusResp.GetMembers())

	cluster.removeServerFromClusterHTTP(t, followerID1)
	cluster.removeServerFromClusterHTTP(t, followerID2)

	clusterStatusResp = cluster.clusterStatusHTTP(t)
	require.Equal(t, cluster.leaderID(), clusterStatusResp.GetLeader())
	require.Equal(t, cluster.clusterMembers(), clusterStatusResp.GetMembers())
}

func TestJoinClusterRemoveFromClusterRPC(t *testing.T) {
	cluster := newTestCoordinatorCluster(t, "coordinator-1")
	defer cluster.stop()

	client := newTestCoordinatorClient(t)

	followerID1 := "coordinator-2"
	followerID2 := "coordinator-3"
	cluster.addServerToClusterRPC(t, client, followerID1)
	cluster.addServerToClusterRPC(t, client, followerID2)

	clusterStatusResp := cluster.clusterStatusRPC(t, client)
	require.Equal(t, cluster.leaderID(), clusterStatusResp.GetLeader())
	require.Equal(t, cluster.clusterMembers(), clusterStatusResp.GetMembers())

	cluster.removeServerFromClusterRPC(t, client, followerID1)
	cluster.removeServerFromClusterRPC(t, client, followerID2)

	clusterStatusResp = cluster.clusterStatusRPC(t, client)
	require.Equal(t, cluster.leaderID(), clusterStatusResp.GetLeader())
	require.Equal(t, cluster.clusterMembers(), clusterStatusResp.GetMembers())
}

func TestAddToChainThenRemoveRPC(t *testing.T) {
	cluster := newTestCoordinatorCluster(t, "coordinator-1")
	defer cluster.stop()
	chainServer1 := newTestChainServer(t, "chain-node-1", 8080, false)
	defer chainServer1.stop()
	chainServer2 := newTestChainServer(t, "chain-node-2", 8081, false)
	defer chainServer2.stop()

	client := newTestCoordinatorClient(t)

	member1 := &chainnode.ChainMember{ID: chainServer1.server.Node.ID, Address: chainServer1.server.Node.Address}
	member2 := &chainnode.ChainMember{ID: chainServer2.server.Node.ID, Address: chainServer2.server.Node.Address}

	cluster.addChainMemberRPC(t, client, member1.ID, member1.Address)
	cluster.addChainMemberRPC(t, client, member2.ID, member2.Address)

	getMembersResp := cluster.getChainMembersRPC(t, client)
	config := chainnode.NewConfigurationFromProto(getMembersResp.GetConfiguration())
	expectedConfig := chainnode.NewConfiguration([]*chainnode.ChainMember{member1, member2}, 2)
	require.True(t, config.Equal(expectedConfig))

	cluster.removeChainMemberRPC(t, client, member2.ID)

	getMembersResp = cluster.getChainMembersRPC(t, client)
	config = chainnode.NewConfigurationFromProto(getMembersResp.GetConfiguration())
	expectedConfig = chainnode.NewConfiguration([]*chainnode.ChainMember{member1}, 3)
	require.True(t, config.Equal(expectedConfig))
}

func TestAddToChainThenRemoveHTTP(t *testing.T) {
	cluster := newTestCoordinatorCluster(t, "coordinator-1")
	defer cluster.stop()
	chainServer1 := newTestChainServer(t, "chain-node-1", 8080, false)
	defer chainServer1.stop()
	chainServer2 := newTestChainServer(t, "chain-node-2", 8081, false)
	defer chainServer2.stop()

	member1 := &chainnode.ChainMember{ID: chainServer1.server.Node.ID, Address: chainServer1.server.Node.Address}
	member2 := &chainnode.ChainMember{ID: chainServer2.server.Node.ID, Address: chainServer2.server.Node.Address}

	cluster.addChainMemberHTTP(t, member1.ID, member1.Address)
	cluster.addChainMemberHTTP(t, member2.ID, member2.Address)

	getMembersResp := cluster.getChainMembersHTTP(t)
	config := chainnode.NewConfigurationFromProto(getMembersResp.GetConfiguration())
	expectedConfig := chainnode.NewConfiguration([]*chainnode.ChainMember{member1, member2}, 2)
	require.True(t, config.Equal(expectedConfig))

	cluster.removeChainMemberHTTP(t, member2.ID)

	getMembersResp = cluster.getChainMembersHTTP(t)
	config = chainnode.NewConfigurationFromProto(getMembersResp.GetConfiguration())
	expectedConfig = chainnode.NewConfiguration([]*chainnode.ChainMember{member1}, 3)
	require.True(t, config.Equal(expectedConfig))
}
