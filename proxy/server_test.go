package proxy

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jmsadair/keychain/api"
	"github.com/jmsadair/keychain/chain"
	"github.com/jmsadair/keychain/coordinator"
	coordinatorpb "github.com/jmsadair/keychain/proto/coordinator"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	eventuallyTimeout = 5 * time.Second
	eventuallyTick    = 100 * time.Millisecond
	clusterAddress    = "127.0.0.1"
	chainAddress      = "127.0.0.2"
	proxyAddress      = "127.0.0.3"
)

var creds = grpc.WithTransportCredentials(insecure.NewCredentials())

type testCluster struct {
	coordinators []*coordinator.Server
	leader       *coordinator.Server
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

func makeCluster(t *testing.T, clusterSize int) *testCluster {
	tc := &testCluster{coordinators: make([]*coordinator.Server, 0, clusterSize)}
	ctx, cancel := context.WithCancel(context.Background())
	tc.cancel = cancel

	basePort := 8080
	tc.wg.Add(clusterSize)
	for i := range clusterSize {
		id := fmt.Sprintf("coordinator-%d", i+1)
		rpcAddr := fmt.Sprintf("%s:%d", clusterAddress, basePort)
		raftAddr := fmt.Sprintf("%s:%d", clusterAddress, basePort+1)
		httpAddr := fmt.Sprintf("%s:%d", clusterAddress, basePort+2)
		basePort += 3
		c, err := coordinator.NewServer(id, httpAddr, rpcAddr, raftAddr, t.TempDir(), t.TempDir(), i == 0, creds)
		require.NoError(t, err)
		tc.coordinators = append(tc.coordinators, c)
		go func() {
			defer tc.wg.Done()
			require.NoError(t, c.Run(ctx))
		}()
	}

	// Wait for the bootstrapped coordinator to elect itself leader.
	require.Eventually(t, func() bool {
		bootstrapped := tc.coordinators[0]
		status, err := bootstrapped.Raft.ClusterStatus()
		if err == nil && status.Leader == bootstrapped.Raft.ID {
			tc.leader = bootstrapped
			return true
		}
		return false
	}, eventuallyTimeout, eventuallyTick)

	// Add the other coordinators to the cluster.
	for _, srv := range tc.coordinators {
		if srv == tc.leader {
			continue
		}
		require.NoError(t, tc.leader.Coordinator.Raft.JoinCluster(ctx, srv.Raft.ID, srv.Raft.Address))
	}

	return tc
}

func (tc *testCluster) stop() {
	tc.cancel()
	tc.wg.Wait()
}

func (tc *testCluster) addChainMember(t *testing.T, id string, addr string) {
	req := &coordinatorpb.AddMemberRequest{Id: id, Address: addr}
	_, err := tc.leader.Coordinator.AddMember(context.Background(), req)
	require.NoError(t, err)
}

func (tc *testCluster) removeChainMember(t *testing.T, id string) {
	req := &coordinatorpb.RemoveMemberRequest{Id: id}
	_, err := tc.leader.Coordinator.RemoveMember(context.Background(), req)
	require.NoError(t, err)
}

func (tc *testCluster) members() []string {
	members := make([]string, 0, len(tc.coordinators))
	for _, srv := range tc.coordinators {
		members = append(members, srv.GRPCServer.Address)
	}
	return members
}

type testChain struct {
	cluster *testCluster
	nodes   []*chain.Server
	cancels []context.CancelFunc
	wg      sync.WaitGroup
}

func makeTestChain(cluster *testCluster) *testChain {
	return &testChain{cluster: cluster}
}

func (tc *testChain) addMember(t *testing.T, id string) {
	serverConfig := chain.ServerConfig{
		ID:          id,
		ListenAddr:  fmt.Sprintf("%s:%d", chainAddress, 8080+len(tc.nodes)),
		StoragePath: t.TempDir(),
		DialOptions: []grpc.DialOption{creds},
		Log:         slog.Default(),
	}
	srv, err := chain.NewServer(serverConfig)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	tc.cancels = append(tc.cancels, cancel)
	tc.nodes = append(tc.nodes, srv)
	tc.wg.Add(1)
	go func() {
		defer tc.wg.Done()
		require.NoError(t, srv.Run(ctx))
	}()

	tc.cluster.addChainMember(t, srv.Node.ID, srv.Node.Address)
}

func (tc *testChain) removeMember(t *testing.T, id string) {
	tc.cluster.removeChainMember(t, id)
}

func (tc *testChain) stop() {
	for _, cancel := range tc.cancels {
		cancel()
	}
	tc.wg.Wait()
}

func makeProxy(t *testing.T, clusterMembers []string) (*Server, func()) {
	srv, err := NewServer(fmt.Sprintf("%s:8080", proxyAddress), fmt.Sprintf("%s:8081", proxyAddress), clusterMembers, creds)
	require.NoError(t, err)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, srv.Run(ctx))
	}()

	// Wait for HTTP server to be healthy.
	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/healthz", srv.HTTPServer.Address))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, eventuallyTimeout, eventuallyTick)

	return srv, func() {
		cancel()
		wg.Wait()
	}
}

func TestSetGetValue(t *testing.T) {
	cluster := makeCluster(t, 3)
	defer cluster.stop()
	chain := makeTestChain(cluster)
	defer chain.stop()
	chain.addMember(t, "chain-node-1")
	chain.addMember(t, "chain-node-2")
	chain.addMember(t, "chain-node-3")
	proxy, cancel := makeProxy(t, cluster.members())
	defer cancel()

	client, err := api.NewClient(proxy.GRPCServer.Address, creds)
	require.NoError(t, err)

	ctx := context.Background()
	key := "key-1"
	value := []byte("value-1")

	require.NoError(t, client.Set(ctx, key, value))

	v, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value, v)
}

func TestRemoveMemberThenGetValue(t *testing.T) {
	cluster := makeCluster(t, 3)
	defer cluster.stop()
	chain := makeTestChain(cluster)
	defer chain.stop()
	chain.addMember(t, "chain-node-1")
	chain.addMember(t, "chain-node-2")
	chain.addMember(t, "chain-node-3")
	proxy, cancel := makeProxy(t, cluster.members())
	defer cancel()

	ctx := context.Background()
	client, err := api.NewClient(proxy.GRPCServer.Address, creds)
	require.NoError(t, err)

	key := "key-1"
	value := []byte("value-1")

	require.NoError(t, client.Set(ctx, key, value))

	// Remove the tail.
	chain.removeMember(t, "chain-node-3")

	// This may fail due to new tail not yet having committed all of its key-value pairs.
	// It should succeed eventually, though.
	require.Eventually(t, func() bool {
		v, err := client.Get(ctx, key)
		return err == nil && bytes.Equal(v, value)
	}, 1*time.Second, 50*time.Millisecond)
}

func TestAddMemberThenGetValue(t *testing.T) {
	cluster := makeCluster(t, 3)
	defer cluster.stop()
	chain := makeTestChain(cluster)
	defer chain.stop()
	chain.addMember(t, "chain-node-1")
	chain.addMember(t, "chain-node-2")
	proxy, cancel := makeProxy(t, cluster.members())
	defer cancel()

	client, err := api.NewClient(proxy.GRPCServer.Address, creds)
	require.NoError(t, err)

	ctx := context.Background()
	key := "key-1"
	value := []byte("value-1")

	require.NoError(t, client.Set(ctx, key, value))

	// Add a member to the tail.
	chain.addMember(t, "chain-node-3")

	// This should succeed since the newly added node should just forward the request to
	// its predecessor if it is still catching up.
	v, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, value, v)
}
