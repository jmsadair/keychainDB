package tests

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetGetValue(t *testing.T) {
	cluster := newTestCoordinatorCluster(t, "coordinator-1")
	defer cluster.stop()
	clusterClient := newTestCoordinatorClient(t)
	cluster.addServerToClusterRPC(t, clusterClient, "coordinator-2")
	cluster.addServerToClusterRPC(t, clusterClient, "coordinator-3")

	chain := newTestChain(cluster)
	chainClient := newTestChainClient(t)
	defer chain.stop()
	chain.addServer(t, clusterClient, "chain-node-1")
	chain.addServer(t, clusterClient, "chain-node-2")
	chain.addServer(t, clusterClient, "chain-node-3")
	waitForActiveChainStatus(t, chainClient, chain.srvs...)

	proxy := newTestProxyServer(t, cluster.rpcAddresses())
	defer proxy.stop()

	ctx := context.Background()
	client := newTestAPIClient(t, proxy.server.GRPCServer.Address)

	key := "key-1"
	value := []byte("value-1")
	require.NoError(t, client.Set(ctx, key, value))

	v, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value, v)
}

func TestRemoveMemberThenGetValue(t *testing.T) {
	cluster := newTestCoordinatorCluster(t, "coordinator-1")
	defer cluster.stop()
	clusterClient := newTestCoordinatorClient(t)
	cluster.addServerToClusterRPC(t, clusterClient, "coordinator-2")
	cluster.addServerToClusterRPC(t, clusterClient, "coordinator-3")

	chain := newTestChain(cluster)
	chainClient := newTestChainClient(t)
	defer chain.stop()
	chain.addServer(t, clusterClient, "chain-node-1")
	chain.addServer(t, clusterClient, "chain-node-2")
	chain.addServer(t, clusterClient, "chain-node-3")
	waitForActiveChainStatus(t, chainClient, chain.srvs...)

	proxy := newTestProxyServer(t, cluster.rpcAddresses())
	defer proxy.stop()

	ctx := context.Background()
	client := newTestAPIClient(t, proxy.server.GRPCServer.Address)

	key := "key-1"
	value := []byte("value-1")
	require.NoError(t, client.Set(ctx, key, value))

	chain.removeServer(t, clusterClient, "chain-node-3")

	// This can fail since the new tail may still be committing its uncommitted keys.
	require.Eventually(t, func() bool {
		v, err := client.Get(ctx, key)
		return err == nil && bytes.Equal(value, v)
	}, eventuallyTimeout, eventuallyTick)
}

func TestAddMemberThenGetValue(t *testing.T) {
	cluster := newTestCoordinatorCluster(t, "coordinator-1")
	defer cluster.stop()
	clusterClient := newTestCoordinatorClient(t)
	cluster.addServerToClusterRPC(t, clusterClient, "coordinator-2")
	cluster.addServerToClusterRPC(t, clusterClient, "coordinator-3")

	chain := newTestChain(cluster)
	chainClient := newTestChainClient(t)
	defer chain.stop()
	chain.addServer(t, clusterClient, "chain-node-1")
	chain.addServer(t, clusterClient, "chain-node-2")
	chain.addServer(t, clusterClient, "chain-node-3")
	waitForActiveChainStatus(t, chainClient, chain.srvs...)

	proxy := newTestProxyServer(t, cluster.rpcAddresses())
	defer proxy.stop()

	ctx := context.Background()
	client := newTestAPIClient(t, proxy.server.GRPCServer.Address)

	key := "key-1"
	value := []byte("value-1")
	require.NoError(t, client.Set(ctx, key, value))

	chain.addServer(t, clusterClient, "chain-node-4")

	v, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value, v)
}
