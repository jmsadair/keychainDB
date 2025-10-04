package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jmsadair/keychain/chain"
	chainnode "github.com/jmsadair/keychain/chain/node"
	coordinatorgrpc "github.com/jmsadair/keychain/coordinator/grpc"
	pb "github.com/jmsadair/keychain/proto/coordinator"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	protojson "google.golang.org/protobuf/encoding/protojson"
)

const (
	clusterAddress    = "127.0.0.1"
	chainAddress      = "127.0.0.2"
	eventuallyTimeout = 5 * time.Second
	eventuallyTick    = 100 * time.Millisecond
)

var creds = grpc.WithTransportCredentials(insecure.NewCredentials())

func verifyClusterStatus(t *testing.T, members map[string]string, expectedMembers map[string]*Server, leader, expectedleader string) {
	require.Len(t, members, len(expectedMembers))
	for id, srv := range expectedMembers {
		require.Equal(t, members[id], srv.Raft.Address)
	}
	require.Equal(t, expectedleader, leader)
}

func waitForleader(t *testing.T, srv *Server) {
	require.Eventually(t, func() bool {
		status, err := srv.Raft.ClusterStatus()
		return err == nil && status.Leader == srv.Raft.ID
	}, eventuallyTimeout, eventuallyTick)
}

type testCluster struct {
	servers map[string]*Server
	leader  *Server
	cancels []func()
}

func newTestCoordinatorClient(t *testing.T) *coordinatorgrpc.Client {
	client, err := coordinatorgrpc.NewClient(creds)
	require.NoError(t, err)
	return client
}

func makeCoordinator(t *testing.T, id string, httpAddr string, grpcAddr string, raftAddr string, bootstrap bool) (*Server, func()) {
	srv, err := NewServer(id, httpAddr, grpcAddr, raftAddr, t.TempDir(), t.TempDir(), bootstrap, creds)
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
		httpAddr := fmt.Sprintf("%s:%d", clusterAddress, basePort)
		grpcAddr := fmt.Sprintf("%s:%d", clusterAddress, basePort+1)
		raftAddr := fmt.Sprintf("%s:%d", clusterAddress, basePort+2)
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
	srv, err := chain.NewServer(id, address, t.TempDir(), creds)
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

func makeTestChain(t *testing.T, numNodes int) *testChain {
	idToServer := make(map[string]*chain.Server, numNodes)
	cancels := make([]func(), 0, numNodes)

	basePort := 8080
	for i := range 3 {
		id := fmt.Sprintf("chain-node-%d", i)
		grpcAddr := fmt.Sprintf("%s:%d", chainAddress, basePort)
		basePort += 1
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

func TestJoinClusterRemoveFromClusterHTTP(t *testing.T) {
	cluster := makeCluster(t)
	defer cluster.stop()

	cluserStatusURL := fmt.Sprintf("http://%s/v1/cluster/status", cluster.leader.HTTPServer.Address)
	clusterMembersURL := fmt.Sprintf("http://%s/v1/cluster/members", cluster.leader.HTTPServer.Address)
	leaderID := cluster.leader.Raft.ID

	for id, srv := range cluster.servers {
		if id == leaderID {
			continue
		}
		req := &pb.JoinClusterRequest{Id: id, Address: srv.Raft.Address}
		b, err := protojson.Marshal(req)
		require.NoError(t, err)
		resp, err := http.Post(clusterMembersURL, "application/json", bytes.NewReader(b))
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	resp1, err := http.Get(cluserStatusURL)
	require.NoError(t, err)
	defer resp1.Body.Close()
	b, err := io.ReadAll(resp1.Body)
	require.NoError(t, err)
	var clusterStatusResp pb.ClusterStatusResponse
	require.NoError(t, protojson.Unmarshal(b, &clusterStatusResp))
	verifyClusterStatus(t, clusterStatusResp.GetMembers(), cluster.servers, clusterStatusResp.GetLeader(), leaderID)

	for id := range cluster.servers {
		if id == leaderID {
			continue
		}
		deleteURL, err := url.JoinPath(clusterMembersURL, id)
		require.NoError(t, err)
		req, err := http.NewRequest(http.MethodDelete, deleteURL, nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	resp2, err := http.Get(cluserStatusURL)
	require.NoError(t, err)
	defer resp2.Body.Close()
	b, err = io.ReadAll(resp2.Body)
	require.NoError(t, err)
	clusterStatusResp = pb.ClusterStatusResponse{}
	require.NoError(t, protojson.Unmarshal(b, &clusterStatusResp))
	verifyClusterStatus(
		t,
		clusterStatusResp.GetMembers(),
		map[string]*Server{leaderID: cluster.leader},
		clusterStatusResp.GetLeader(),
		leaderID,
	)
}

func TestJoinClusterThenRemoveRPC(t *testing.T) {
	cluster := makeCluster(t)
	defer cluster.stop()
	client := newTestCoordinatorClient(t)

	ctx := context.Background()
	leaderID := cluster.leader.Raft.ID

	for id, srv := range cluster.servers {
		if id == leaderID {
			continue
		}
		req := &pb.JoinClusterRequest{Id: id, Address: srv.Raft.Address}
		_, err := client.JoinCluster(ctx, cluster.leader.GRPCServer.Address, req)
		require.NoError(t, err)
	}

	var req pb.ClusterStatusRequest
	resp, err := client.ClusterStatus(ctx, cluster.leader.GRPCServer.Address, &req)
	require.NoError(t, err)
	verifyClusterStatus(t, resp.GetMembers(), cluster.servers, resp.GetLeader(), cluster.leader.Raft.ID)

	for id := range cluster.servers {
		if id == leaderID {
			continue
		}
		req := &pb.RemoveFromClusterRequest{Id: id}
		_, err := client.RemoveFromCluster(ctx, cluster.leader.GRPCServer.Address, req)
		require.NoError(t, err)
	}

	resp, err = client.ClusterStatus(ctx, cluster.leader.GRPCServer.Address, &req)
	require.NoError(t, err)
	verifyClusterStatus(t, resp.GetMembers(), map[string]*Server{leaderID: cluster.leader}, resp.GetLeader(), leaderID)
}

func TestAddToChainThenRemoveRPC(t *testing.T) {
	cluster := makeCluster(t)
	defer cluster.stop()
	tc := makeTestChain(t, 2)
	defer tc.stop()
	client := newTestCoordinatorClient(t)

	ctx := context.Background()
	ids := slices.Collect(maps.Keys(tc.idToServer))
	srv1 := tc.idToServer[ids[0]]
	member1 := &chainnode.ChainMember{ID: srv1.Node.ID, Address: srv1.Node.Address}
	srv2 := tc.idToServer[ids[1]]
	member2 := &chainnode.ChainMember{ID: srv2.Node.ID, Address: srv2.Node.Address}

	addMemberReq := &pb.AddMemberRequest{Id: srv1.Node.ID, Address: srv1.Node.Address}
	_, err := client.AddMember(ctx, cluster.leader.GRPCServer.Address, addMemberReq)
	require.NoError(t, err)
	addMemberReq = &pb.AddMemberRequest{Id: srv2.Node.ID, Address: srv2.Node.Address}
	_, err = client.AddMember(ctx, cluster.leader.GRPCServer.Address, addMemberReq)
	require.NoError(t, err)

	var readChainConfigReq pb.GetMembersRequest
	readChainConfigResp, err := client.GetMembers(ctx, cluster.leader.GRPCServer.Address, &readChainConfigReq)
	require.NoError(t, err)
	config := chainnode.NewConfigurationFromProto(readChainConfigResp.GetConfiguration())
	expectedConfig := chainnode.NewConfiguration([]*chainnode.ChainMember{member1, member2}, 2)
	require.True(t, config.Equal(expectedConfig))

	removeMemberReq := &pb.RemoveMemberRequest{Id: srv2.Node.ID}
	_, err = client.RemoveMember(ctx, cluster.leader.GRPCServer.Address, removeMemberReq)
	require.NoError(t, err)
	readChainConfigResp, err = client.GetMembers(ctx, cluster.leader.GRPCServer.Address, &readChainConfigReq)
	require.NoError(t, err)
	config = chainnode.NewConfigurationFromProto(readChainConfigResp.GetConfiguration())
	expectedConfig = chainnode.NewConfiguration([]*chainnode.ChainMember{member1}, 3)
	require.True(t, config.Equal(expectedConfig))
}

func TestAddToChainThenRemoveHTTP(t *testing.T) {
	cluster := makeCluster(t)
	defer cluster.stop()
	tc := makeTestChain(t, 2)
	defer tc.stop()

	chainMembersURL := fmt.Sprintf("http://%s/v1/chain/members", cluster.leader.HTTPServer.Address)

	ids := slices.Collect(maps.Keys(tc.idToServer))
	srv1 := tc.idToServer[ids[0]]
	member1 := &chainnode.ChainMember{ID: srv1.Node.ID, Address: srv1.Node.Address}
	srv2 := tc.idToServer[ids[1]]
	member2 := &chainnode.ChainMember{ID: srv2.Node.ID, Address: srv2.Node.Address}

	req1 := &pb.AddMemberOperation{Id: srv1.Node.ID, Address: srv1.Node.Address}
	b, err := protojson.Marshal(req1)
	require.NoError(t, err)
	resp1, err := http.Post(chainMembersURL, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	defer resp1.Body.Close()
	require.Equal(t, http.StatusOK, resp1.StatusCode)
	req2 := &pb.AddMemberOperation{Id: srv2.Node.ID, Address: srv2.Node.Address}
	b, err = protojson.Marshal(req2)
	require.NoError(t, err)
	resp2, err := http.Post(chainMembersURL, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Equal(t, http.StatusOK, resp2.StatusCode)

	resp3, err := http.Get(chainMembersURL)
	require.NoError(t, err)
	defer resp3.Body.Close()
	var readChainConfigResp pb.GetMembersResponse
	b, err = io.ReadAll(resp3.Body)
	require.NoError(t, err)
	require.NoError(t, protojson.Unmarshal(b, &readChainConfigResp))
	config := chainnode.NewConfigurationFromProto(readChainConfigResp.GetConfiguration())
	expectedConfig := chainnode.NewConfiguration([]*chainnode.ChainMember{member1, member2}, 2)
	require.True(t, config.Equal(expectedConfig))

	deleteURL, err := url.JoinPath(chainMembersURL, srv2.Node.ID)
	require.NoError(t, err)
	req4, err := http.NewRequest(http.MethodDelete, deleteURL, nil)
	require.NoError(t, err)
	resp4, err := http.DefaultClient.Do(req4)
	require.NoError(t, err)
	defer resp4.Body.Close()
	require.Equal(t, http.StatusOK, resp4.StatusCode)

	readChainConfigResp = pb.GetMembersResponse{}
	resp5, err := http.Get(chainMembersURL)
	require.NoError(t, err)
	defer resp5.Body.Close()
	b, err = io.ReadAll(resp5.Body)
	require.NoError(t, err)
	require.NoError(t, protojson.Unmarshal(b, &readChainConfigResp))
	config = chainnode.NewConfigurationFromProto(readChainConfigResp.GetConfiguration())
	expectedConfig = chainnode.NewConfiguration([]*chainnode.ChainMember{member1}, 3)
	require.True(t, config.Equal(expectedConfig))
}
