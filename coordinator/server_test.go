package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jmsadair/keychain/chain"
	coordinatorgrpc "github.com/jmsadair/keychain/coordinator/grpc"
	"github.com/jmsadair/keychain/coordinator/node"
	"github.com/jmsadair/keychain/coordinator/raft"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	baseTestPort = 8080
	portOffset   = 3 // http, grpc, raft
)

type testCluster struct {
	Servers     map[string]*Server
	BootstrapID string
	Leader      *Server
}

func postJSON(t *testing.T, url string, body any) *http.Response {
	b, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	return resp
}

func getJSON(t *testing.T, url string, v any) {
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NoError(t, json.NewDecoder(resp.Body).Decode(v))
}

func newTestCoordinatorClient(t *testing.T) *coordinatorgrpc.Client {
	client, err := coordinatorgrpc.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return client
}

func verifyClusterStatus(t *testing.T, status raft.Status, expectedServers map[string]*Server, expectedLeader string) {
	require.Len(t, status.Members, len(expectedServers))
	for id, srv := range expectedServers {
		require.Equal(t, status.Members[id], srv.Raft.Address)
	}
	require.Equal(t, expectedLeader, status.Leader)
}

func joinNonBootstrappedMembers(t *testing.T, client *coordinatorgrpc.Client, cluster *testCluster) {
	for id, srv := range cluster.Servers {
		if id == cluster.BootstrapID {
			continue
		}
		req := &node.JoinClusterRequest{ID: id, Address: srv.Raft.Address}
		var resp node.JoinClusterResponse
		require.NoError(t, client.JoinCluster(context.Background(), cluster.Leader.GRPCServer.Address, req, &resp))
	}
}

func waitForLeader(t *testing.T, srv *Server) {
	require.Eventually(t, func() bool {
		status, err := srv.Raft.ClusterStatus()
		return err == nil && status.Leader == srv.Raft.ID
	}, 5*time.Second, 100*time.Millisecond)
}

func makeCoordinatorServer(t *testing.T, id string, httpAddr string, grpcAddr string, raftAddr string, bootstrap bool) *Server {
	srv, err := NewServer(
		id,
		httpAddr,
		grpcAddr,
		raftAddr,
		t.TempDir(),
		t.TempDir(),
		bootstrap,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	return srv
}

func makeCluster(t *testing.T) *testCluster {
	idToServer := make(map[string]*Server, 3)
	var bootstrapped string
	basePort := baseTestPort
	for i := range 3 {
		id := fmt.Sprintf("coordinator-%d", i)
		httpAddr := fmt.Sprintf("127.0.0.1:%d", basePort)
		grpcAddr := fmt.Sprintf("127.0.0.1:%d", basePort+1)
		raftAddr := fmt.Sprintf("127.0.0.1:%d", basePort+2)
		basePort += portOffset
		idToServer[id] = makeCoordinatorServer(t, id, httpAddr, grpcAddr, raftAddr, i == 0)
		if i == 0 {
			bootstrapped = id
		}
	}

	return &testCluster{
		Servers:     idToServer,
		BootstrapID: bootstrapped,
		Leader:      idToServer[bootstrapped],
	}
}

func makeChainServer(t *testing.T, id string, port int) (*chain.Server, func()) {
	srv, err := chain.NewServer(id, fmt.Sprintf("127.0.0.2:%d", port), t.TempDir(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		require.NoError(t, srv.Run(ctx))
	}()

	cleanup := func() {
		cancel()
		wg.Wait()
	}
	return srv, cleanup
}

func startCluster(t *testing.T, cluster *testCluster) func() {
	var wg sync.WaitGroup
	wg.Add(len(cluster.Servers))
	ctx, cancel := context.WithCancel(context.Background())
	for _, srv := range cluster.Servers {
		go func() {
			srv.Run(ctx)
			wg.Done()
		}()
	}
	waitForLeader(t, cluster.Leader)
	return func() {
		cancel()
		wg.Wait()
	}
}

func TestJoinClusterThenRemoveHTTP(t *testing.T) {
	cluster := makeCluster(t)
	cancel := startCluster(t, cluster)
	defer cancel()

	statusURL := fmt.Sprintf("http://%s/v1/cluster/status", cluster.Leader.HTTPServer.Address)
	clusterURL := fmt.Sprintf("http://%s/v1/cluster/members", cluster.Leader.HTTPServer.Address)

	for id, srv := range cluster.Servers {
		if id == cluster.BootstrapID {
			continue
		}
		req := map[string]string{"id": id, "address": srv.Raft.Address}
		resp := postJSON(t, clusterURL, req)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	var status raft.Status
	getJSON(t, statusURL, &status)
	verifyClusterStatus(t, status, cluster.Servers, cluster.BootstrapID)

	for id := range cluster.Servers {
		if id == cluster.BootstrapID {
			continue
		}
		req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/%s", clusterURL, id), nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	status = raft.Status{}
	getJSON(t, statusURL, &status)
	require.Len(t, status.Members, 1)
	require.Equal(t, status.Members[cluster.BootstrapID], cluster.Leader.Raft.Address)
	require.Equal(t, cluster.BootstrapID, status.Leader)
}

func TestJoinClusterThenRemoveRPC(t *testing.T) {
	cluster := makeCluster(t)
	cancel := startCluster(t, cluster)
	defer cancel()

	client := newTestCoordinatorClient(t)
	joinNonBootstrappedMembers(t, client, cluster)

	var statusReq node.ClusterStatusRequest
	var statusResp node.ClusterStatusResponse
	require.NoError(t, client.ClusterStatus(context.Background(), cluster.Leader.GRPCServer.Address, &statusReq, &statusResp))
	verifyClusterStatus(t, statusResp.Status, cluster.Servers, cluster.BootstrapID)

	for id := range cluster.Servers {
		if id == cluster.BootstrapID {
			continue
		}
		req := &node.RemoveFromClusterRequest{ID: id}
		var resp node.RemoveFromClusterResponse
		require.NoError(t, client.RemoveFromCluster(context.Background(), cluster.Leader.GRPCServer.Address, req, &resp))
	}

	statusResp = node.ClusterStatusResponse{}
	require.NoError(t, client.ClusterStatus(context.Background(), cluster.Leader.GRPCServer.Address, &statusReq, &statusResp))
	status := statusResp.Status
	require.Len(t, status.Members, 1)
	require.Equal(t, status.Members[cluster.BootstrapID], cluster.Leader.Raft.Address)
	require.Equal(t, cluster.BootstrapID, status.Leader)
}

func TestAddToChainThenRemoveRPC(t *testing.T) {
	cluster := makeCluster(t)
	cancel := startCluster(t, cluster)
	defer cancel()
	chainServer1, cancelChainServer1 := makeChainServer(t, "chain-node-1", 8080)
	defer cancelChainServer1()
	chainServer2, cancelChainServer2 := makeChainServer(t, "chain-node-2", 8081)
	defer cancelChainServer2()

	client := newTestCoordinatorClient(t)

	addMemberReq := &node.AddMemberRequest{ID: chainServer1.Node.ID, Address: chainServer1.Node.Address}
	var addMemberResp node.AddMemberResponse
	require.NoError(t, client.AddMember(context.Background(), cluster.Leader.GRPCServer.Address, addMemberReq, &addMemberResp))
	addMemberReq = &node.AddMemberRequest{ID: chainServer2.Node.ID, Address: chainServer2.Node.Address}
	require.NoError(t, client.AddMember(context.Background(), cluster.Leader.GRPCServer.Address, addMemberReq, &addMemberResp))

	var readChainConfigReq node.ReadChainConfigurationRequest
	var readChainConfigResp node.ReadChainConfigurationResponse
	require.NoError(t, client.ReadChainConfiguration(context.Background(), cluster.Leader.GRPCServer.Address, &readChainConfigReq, &readChainConfigResp))
	config := readChainConfigResp.Configuration
	members := config.Members()
	require.Len(t, members, 2)
	require.Equal(t, chainServer1.Node.ID, members[0].ID)
	require.Equal(t, chainServer1.Node.Address, members[0].Address)
	require.Equal(t, chainServer2.Node.ID, members[1].ID)
	require.Equal(t, chainServer2.Node.Address, members[1].Address)

	removeMemberReq := &node.RemoveMemberRequest{ID: chainServer2.Node.ID}
	var removeMemberResp node.RemoveMemberResponse
	require.NoError(t, client.RemoveMember(context.Background(), cluster.Leader.GRPCServer.Address, removeMemberReq, &removeMemberResp))
	readChainConfigResp = node.ReadChainConfigurationResponse{}
	require.NoError(t, client.ReadChainConfiguration(context.Background(), cluster.Leader.GRPCServer.Address, &readChainConfigReq, &readChainConfigResp))
	config = readChainConfigResp.Configuration
	members = config.Members()
	require.Len(t, members, 1)
	require.Equal(t, chainServer1.Node.ID, members[0].ID)
	require.Equal(t, chainServer1.Node.Address, members[0].Address)
}
