package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jmsadair/keychain/chain"
	chainnode "github.com/jmsadair/keychain/chain/node"
	coordinatorgrpc "github.com/jmsadair/keychain/coordinator/grpc"
	"github.com/jmsadair/keychain/coordinator/node"
	"github.com/jmsadair/keychain/coordinator/raft"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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

type testCluster struct {
	servers map[string]*Server
	leader  *Server
	cancels []func()
}

func verifyClusterStatus(t *testing.T, status raft.Status, expectedServers map[string]*Server, expectedleader string) {
	require.Len(t, status.Members, len(expectedServers))
	for id, srv := range expectedServers {
		require.Equal(t, status.Members[id], srv.Raft.Address)
	}
	require.Equal(t, expectedleader, status.Leader)
}

func waitForleader(t *testing.T, srv *Server) {
	require.Eventually(t, func() bool {
		status, err := srv.Raft.ClusterStatus()
		return err == nil && status.Leader == srv.Raft.ID
	}, 5*time.Second, 100*time.Millisecond)
}

func newTestCoordinatorClient(t *testing.T) *coordinatorgrpc.Client {
	client, err := coordinatorgrpc.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return client
}

func makeCoordinator(t *testing.T, id string, httpAddr string, grpcAddr string, raftAddr string, bootstrap bool) (*Server, func()) {
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

	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		srv.Run(ctx)
	}()

	return srv, func() {
		cancel()
		wg.Wait()
	}
}

func makeCluster(t *testing.T) *testCluster {
	idToServer := make(map[string]*Server, 3)
	cancels := make([]func(), 0, 3)

	var bootstrapped string
	basePort := 8080
	for i := range 3 {
		id := fmt.Sprintf("coordinator-%d", i)
		httpAddr := fmt.Sprintf("127.0.0.1:%d", basePort)
		grpcAddr := fmt.Sprintf("127.0.0.1:%d", basePort+1)
		raftAddr := fmt.Sprintf("127.0.0.1:%d", basePort+2)
		basePort += 3
		srv, cancel := makeCoordinator(t, id, httpAddr, grpcAddr, raftAddr, i == 0)
		idToServer[id] = srv
		cancels = append(cancels, cancel)
		if i == 0 {
			bootstrapped = id
		}
	}

	waitForleader(t, idToServer[bootstrapped])

	return &testCluster{
		servers: idToServer,
		leader:  idToServer[bootstrapped],
		cancels: cancels,
	}
}

func (tc *testCluster) stop() {
	for _, c := range tc.cancels {
		c()
	}
}

type testChain struct {
	idToServer map[string]*chain.Server
	cancels    []func()
}

func makeChainServer(t *testing.T, id string, address string) (*chain.Server, func()) {
	srv, err := chain.NewServer(
		id,
		address,
		t.TempDir(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		require.NoError(t, srv.Run(ctx))
	}()

	return srv, func() {
		cancel()
		wg.Wait()
	}

}

func verifyChainConfiguration(t *testing.T, config *chainnode.Configuration, expectedMembers []*chainnode.ChainMember) {
	members := config.Members()
	require.Len(t, members, len(expectedMembers))
	for i, member := range members {
		expectedMember := expectedMembers[i]
		require.Equal(t, expectedMember.ID, member.ID)
		require.Equal(t, expectedMember.Address, member.Address)
	}
}

func makeTestChain(t *testing.T, numNodes int) *testChain {
	idToServer := make(map[string]*chain.Server, numNodes)
	cancels := make([]func(), 0, numNodes)

	basePort := 8080
	for i := range 3 {
		id := fmt.Sprintf("chain-node-%d", i)
		grpcAddr := fmt.Sprintf("127.0.0.2:%d", basePort+1)
		basePort += 2
		srv, cancel := makeChainServer(t, id, grpcAddr)
		idToServer[id] = srv
		cancels = append(cancels, cancel)
	}

	return &testChain{idToServer: idToServer, cancels: cancels}
}

func (tc *testChain) stop() {
	for _, c := range tc.cancels {
		c()
	}
}

func TestJoinClusterThenRemoveHTTP(t *testing.T) {
	cluster := makeCluster(t)
	defer cluster.stop()

	statusURL := fmt.Sprintf("http://%s/v1/cluster/status", cluster.leader.HTTPServer.Address)
	clusterURL := fmt.Sprintf("http://%s/v1/cluster/members", cluster.leader.HTTPServer.Address)
	leaderID := cluster.leader.Raft.ID

	for id, srv := range cluster.servers {
		if id == leaderID {
			continue
		}
		resp := postJSON(t, clusterURL, map[string]string{"id": id, "address": srv.Raft.Address})
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	var clusterStatusResp node.ClusterStatusResponse
	getJSON(t, statusURL, &clusterStatusResp)
	verifyClusterStatus(t, clusterStatusResp.Status, cluster.servers, leaderID)

	for id := range cluster.servers {
		if id == leaderID {
			continue
		}
		req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/%s", clusterURL, id), nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	clusterStatusResp = node.ClusterStatusResponse{}
	getJSON(t, statusURL, &clusterStatusResp)
	verifyClusterStatus(t, clusterStatusResp.Status, map[string]*Server{leaderID: cluster.leader}, leaderID)
}

func TestJoinClusterThenRemoveRPC(t *testing.T) {
	cluster := makeCluster(t)
	defer cluster.stop()
	client := newTestCoordinatorClient(t)

	for id, srv := range cluster.servers {
		if id == cluster.leader.Raft.ID {
			continue
		}
		req := &node.JoinClusterRequest{ID: id, Address: srv.Raft.Address}
		var resp node.JoinClusterResponse
		require.NoError(t, client.JoinCluster(context.Background(), cluster.leader.GRPCServer.Address, req, &resp))
	}

	var statusReq node.ClusterStatusRequest
	var statusResp node.ClusterStatusResponse
	require.NoError(t, client.ClusterStatus(context.Background(), cluster.leader.GRPCServer.Address, &statusReq, &statusResp))
	verifyClusterStatus(t, statusResp.Status, cluster.servers, cluster.leader.Raft.ID)

	for id := range cluster.servers {
		if id == cluster.leader.Raft.ID {
			continue
		}
		req := &node.RemoveFromClusterRequest{ID: id}
		var resp node.RemoveFromClusterResponse
		require.NoError(t, client.RemoveFromCluster(context.Background(), cluster.leader.GRPCServer.Address, req, &resp))
	}

	statusResp = node.ClusterStatusResponse{}
	require.NoError(t, client.ClusterStatus(context.Background(), cluster.leader.GRPCServer.Address, &statusReq, &statusResp))
	status := statusResp.Status
	require.Len(t, status.Members, 1)
	require.Equal(t, status.Members[cluster.leader.Raft.ID], cluster.leader.Raft.Address)
	require.Equal(t, cluster.leader.Raft.ID, status.Leader)
}

func TestAddToChainThenRemoveRPC(t *testing.T) {
	cluster := makeCluster(t)
	defer cluster.stop()
	tc := makeTestChain(t, 2)
	defer tc.stop()
	client := newTestCoordinatorClient(t)

	ids := slices.Collect(maps.Keys(tc.idToServer))
	srv1 := tc.idToServer[ids[0]]
	member1 := &chainnode.ChainMember{ID: srv1.Node.ID, Address: srv1.Node.Address}
	srv2 := tc.idToServer[ids[1]]
	member2 := &chainnode.ChainMember{ID: srv2.Node.ID, Address: srv2.Node.Address}

	addMemberReq := &node.AddMemberRequest{ID: srv1.Node.ID, Address: srv1.Node.Address}
	var addMemberResp node.AddMemberResponse
	require.NoError(t, client.AddMember(context.Background(), cluster.leader.GRPCServer.Address, addMemberReq, &addMemberResp))
	addMemberReq = &node.AddMemberRequest{ID: srv2.Node.ID, Address: srv2.Node.Address}
	require.NoError(t, client.AddMember(context.Background(), cluster.leader.GRPCServer.Address, addMemberReq, &addMemberResp))

	var readChainConfigReq node.ReadChainConfigurationRequest
	var readChainConfigResp node.ReadChainConfigurationResponse
	require.NoError(t, client.ReadChainConfiguration(context.Background(), cluster.leader.GRPCServer.Address, &readChainConfigReq, &readChainConfigResp))
	verifyChainConfiguration(t, readChainConfigResp.Configuration, []*chainnode.ChainMember{member1, member2})

	removeMemberReq := &node.RemoveMemberRequest{ID: srv2.Node.ID}
	var removeMemberResp node.RemoveMemberResponse
	require.NoError(t, client.RemoveMember(context.Background(), cluster.leader.GRPCServer.Address, removeMemberReq, &removeMemberResp))
	readChainConfigResp = node.ReadChainConfigurationResponse{}
	require.NoError(t, client.ReadChainConfiguration(context.Background(), cluster.leader.GRPCServer.Address, &readChainConfigReq, &readChainConfigResp))
	verifyChainConfiguration(t, readChainConfigResp.Configuration, []*chainnode.ChainMember{member1})
}

func TestAddToChainThenRemoveHTTP(t *testing.T) {
	cluster := makeCluster(t)
	defer cluster.stop()
	tc := makeTestChain(t, 2)
	defer tc.stop()

	configURL := fmt.Sprintf("http://%s/v1/chain/configuration", cluster.leader.HTTPServer.Address)
	membersURL := fmt.Sprintf("http://%s/v1/chain/members", cluster.leader.HTTPServer.Address)

	ids := slices.Collect(maps.Keys(tc.idToServer))
	srv1 := tc.idToServer[ids[0]]
	member1 := &chainnode.ChainMember{ID: srv1.Node.ID, Address: srv1.Node.Address}
	srv2 := tc.idToServer[ids[1]]
	member2 := &chainnode.ChainMember{ID: srv2.Node.ID, Address: srv2.Node.Address}

	resp1 := postJSON(t, membersURL, map[string]string{"id": srv1.Node.ID, "address": srv1.Node.Address})
	require.Equal(t, http.StatusOK, resp1.StatusCode)
	defer resp1.Body.Close()
	resp2 := postJSON(t, membersURL, map[string]string{"id": srv2.Node.ID, "address": srv2.Node.Address})
	require.Equal(t, http.StatusOK, resp2.StatusCode)
	defer resp2.Body.Close()

	var readChainConfigResp node.ReadChainConfigurationResponse
	getJSON(t, configURL, &readChainConfigResp)
	config := readChainConfigResp.Configuration
	verifyChainConfiguration(t, config, []*chainnode.ChainMember{member1, member2})

	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/%s", membersURL, srv2.Node.ID), nil)
	require.NoError(t, err)
	resp3, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp3.StatusCode)
	defer resp3.Body.Close()

	readChainConfigResp = node.ReadChainConfigurationResponse{}
	getJSON(t, configURL, &readChainConfigResp)
	config = readChainConfigResp.Configuration
	verifyChainConfiguration(t, config, []*chainnode.ChainMember{member1})

}
