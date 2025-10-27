package tests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/jmsadair/keychain/api/kv"
	"github.com/jmsadair/keychain/chain"
	chainclient "github.com/jmsadair/keychain/chain/client"
	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/coordinator"
	coordinatorclient "github.com/jmsadair/keychain/coordinator/client"
	chainpb "github.com/jmsadair/keychain/proto/chain"
	coordinatorpb "github.com/jmsadair/keychain/proto/coordinator"
	"github.com/jmsadair/keychain/proxy"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	protojson "google.golang.org/protobuf/encoding/protojson"
)

const (
	eventuallyTimeout = 5 * time.Second
	eventuallyTick    = 25 * time.Millisecond
	clusterAddress    = "127.0.0.1"
	chainAddress      = "127.0.0.2"
	proxyAddress      = "127.0.0.3"
)

var creds = grpc.WithTransportCredentials(insecure.NewCredentials())

type portAllocator struct {
	basePort int
}

func newPortAllocator(basePort int) *portAllocator {
	return &portAllocator{basePort: basePort}
}

func (pa *portAllocator) allocate(numPorts int) []int {
	ports := make([]int, numPorts)
	for i := range numPorts {
		ports[i] = pa.basePort
		pa.basePort++
	}
	return ports
}

type serverRunner struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	t      *testing.T
}

func newServerRunner(t *testing.T) *serverRunner {
	ctx, cancel := context.WithCancel(context.Background())
	return &serverRunner{
		ctx:    ctx,
		cancel: cancel,
		t:      t,
	}
}

type runnable interface {
	Run(context.Context) error
}

func (sr *serverRunner) runServer(srv runnable) {
	sr.wg.Add(1)
	go func() {
		defer sr.wg.Done()
		srv.Run(sr.ctx)
	}()
}

func (sr *serverRunner) stop() {
	sr.cancel()
	sr.wg.Wait()
}

func newTestCoordinatorClient(t *testing.T) *coordinatorclient.Client {
	client, err := coordinatorclient.NewClient(creds)
	require.NoError(t, err)
	return client
}

func newTestChainClient(t *testing.T) *chainclient.Client {
	client, err := chainclient.NewClient(creds)
	require.NoError(t, err)
	return client
}

func newTestKvClient(t *testing.T, endpoint string) *kv.Client {
	cfg := kv.Config{
		Endpoint:    endpoint,
		Credentials: insecure.NewCredentials(),
		MaxRetries:  5,
	}
	client, err := kv.NewClient(cfg)
	require.NoError(t, err)
	return client
}

func waitForLeader(t *testing.T, srv *coordinator.Service) {
	require.Eventually(t, func() bool {
		_, id := srv.Raft.LeaderWithID()
		return string(id) == srv.Config.ID
	}, eventuallyTimeout, eventuallyTick)
}

func waitForHealthy(t *testing.T, addr string) {
	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/healthz", addr))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, eventuallyTimeout, eventuallyTick)
}

func updateChainConfiguration(t *testing.T, client *chainclient.Client, config *node.Configuration, srvs ...*testChainServer) {
	req := &chainpb.UpdateConfigurationRequest{Configuration: config.Proto()}
	for _, srv := range srvs {
		_, err := client.UpdateConfiguration(context.Background(), srv.server.Node.Address, req)
		require.NoError(t, err)
	}
}

func waitForActiveChainStatus(t *testing.T, client *chainclient.Client, srvs ...*testChainServer) {
	var req chainpb.PingRequest
	for _, srv := range srvs {
		require.Eventually(t, func() bool {
			resp, err := client.Ping(context.Background(), srv.server.Node.Address, &req)
			if err != nil {
				return false
			}
			return node.Status(resp.GetStatus()) == node.Active
		}, eventuallyTimeout, eventuallyTick)
	}
}

func makeKeyValuePairs(numKeys int) map[string][]byte {
	kvPairs := make(map[string][]byte, numKeys)
	for i := range numKeys {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Appendf(nil, "value-%d", i)
		kvPairs[key] = value
	}
	return kvPairs
}

type testCluster struct {
	srvs      []*testCoordinator
	members   map[string]*testCoordinator
	leader    *testCoordinator
	allocator *portAllocator
}

func newTestCoordinatorCluster(t *testing.T, bootstrapID string) *testCluster {
	tc := &testCluster{
		srvs:      make([]*testCoordinator, 0, 1),
		allocator: newPortAllocator(8080),
		members:   make(map[string]*testCoordinator, 1),
	}

	ports := tc.allocator.allocate(2)
	srv := newTestCoordinator(t, bootstrapID, ports[0], ports[1], true)
	tc.srvs = append(tc.srvs, srv)
	tc.members[bootstrapID] = srv
	tc.leader = srv

	return tc
}

func (tc *testCluster) addServerToClusterRPC(t *testing.T, client *coordinatorclient.Client, id string) {
	ports := tc.allocator.allocate(2)
	srv := newTestCoordinator(t, id, ports[0], ports[1], false)
	tc.srvs = append(tc.srvs, srv)
	tc.members[id] = srv

	req := &coordinatorpb.JoinClusterRequest{Id: srv.server.Coordinator.ID, Address: srv.server.Coordinator.Address}
	_, err := client.JoinCluster(context.Background(), tc.leader.server.Coordinator.Address, req)
	require.NoError(t, err)
}

func (tc *testCluster) removeServerFromClusterRPC(t *testing.T, client *coordinatorclient.Client, id string) {
	req := &coordinatorpb.RemoveFromClusterRequest{Id: id}
	_, err := client.RemoveFromCluster(context.Background(), tc.leader.server.Coordinator.Address, req)
	require.NoError(t, err)
	delete(tc.members, id)
}

func (tc *testCluster) addChainMemberRPC(t *testing.T, client *coordinatorclient.Client, id string, addr string) {
	req := &coordinatorpb.AddMemberRequest{Id: id, Address: addr}
	_, err := client.AddMember(context.Background(), tc.leader.server.Coordinator.Address, req)
	require.NoError(t, err)
}

func (tc *testCluster) removeChainMemberRPC(t *testing.T, client *coordinatorclient.Client, id string) {
	req := &coordinatorpb.RemoveMemberRequest{Id: id}
	_, err := client.RemoveMember(context.Background(), tc.leader.server.Coordinator.Address, req)
	require.NoError(t, err)
}

func (tc *testCluster) addServerToClusterHTTP(t *testing.T, id string) {
	ports := tc.allocator.allocate(3)
	srv := newTestCoordinator(t, id, ports[0], ports[1], false)
	tc.srvs = append(tc.srvs, srv)
	tc.members[id] = srv

	clusterMembersURL := fmt.Sprintf("http://%s/v1/cluster/members", tc.leader.server.HTTPServer.Listen)
	req := &coordinatorpb.JoinClusterRequest{Id: srv.server.Coordinator.ID, Address: srv.server.Coordinator.Address}
	b, err := protojson.Marshal(req)
	require.NoError(t, err)
	resp1, err := http.Post(clusterMembersURL, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	resp1.Body.Close()
	require.Equal(t, http.StatusOK, resp1.StatusCode)
}

func (tc *testCluster) removeServerFromClusterHTTP(t *testing.T, id string) {
	clusterMembersURL := fmt.Sprintf("http://%s/v1/cluster/members", tc.leader.server.HTTPServer.Listen)
	deleteURL, err := url.JoinPath(clusterMembersURL, id)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodDelete, deleteURL, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	delete(tc.members, id)
}

func (tc *testCluster) addChainMemberHTTP(t *testing.T, id string, addr string) {
	chainMembersURL := fmt.Sprintf("http://%s/v1/chain/members", tc.leader.server.HTTPServer.Listen)

	req := &coordinatorpb.AddMemberRequest{Id: id, Address: addr}
	b, err := protojson.Marshal(req)
	require.NoError(t, err)
	resp, err := http.Post(chainMembersURL, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func (tc *testCluster) removeChainMemberHTTP(t *testing.T, id string) {
	chainMembersURL := fmt.Sprintf("http://%s/v1/chain/members", tc.leader.server.HTTPServer.Listen)
	deleteURL, err := url.JoinPath(chainMembersURL, id)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodDelete, deleteURL, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func (tc *testCluster) clusterStatusRPC(t *testing.T, client *coordinatorclient.Client) *coordinatorpb.ClusterStatusResponse {
	var req coordinatorpb.ClusterStatusRequest
	resp, err := client.ClusterStatus(context.Background(), tc.leader.server.Coordinator.Address, &req)
	require.NoError(t, err)
	return resp
}

func (tc *testCluster) clusterStatusHTTP(t *testing.T) *coordinatorpb.ClusterStatusResponse {
	cluserStatusURL := fmt.Sprintf("http://%s/v1/cluster/status", tc.leader.server.HTTPServer.Listen)

	resp, err := http.Get(cluserStatusURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var clusterStatusResp coordinatorpb.ClusterStatusResponse
	require.NoError(t, protojson.Unmarshal(b, &clusterStatusResp))
	return &clusterStatusResp
}

func (tc *testCluster) getChainMembersRPC(t *testing.T, client *coordinatorclient.Client) *coordinatorpb.GetMembersResponse {
	var req coordinatorpb.GetMembersRequest
	resp, err := client.GetMembers(context.Background(), tc.leader.server.Coordinator.Address, &req)
	require.NoError(t, err)
	return resp
}

func (tc *testCluster) getChainMembersHTTP(t *testing.T) *coordinatorpb.GetMembersResponse {
	chainMembersURL := fmt.Sprintf("http://%s/v1/chain/members", tc.leader.server.HTTPServer.Listen)

	resp, err := http.Get(chainMembersURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	var getMembersResp coordinatorpb.GetMembersResponse
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, protojson.Unmarshal(b, &getMembersResp))
	return &getMembersResp
}

func (tc *testCluster) clusterMembers() map[string]string {
	members := make(map[string]string, len(tc.members))
	for id, srv := range tc.members {
		members[id] = srv.server.Coordinator.Address
	}
	return members
}

func (tc *testCluster) rpcAddresses() []string {
	addrs := make([]string, 0, len(tc.members))
	for _, srv := range tc.members {
		addrs = append(addrs, srv.server.Coordinator.Address)
	}
	return addrs
}

func (tc *testCluster) leaderID() string {
	return tc.leader.server.Coordinator.ID
}

func (tc *testCluster) stop() {
	for _, srv := range tc.srvs {
		srv.stop()
	}
}

type testCoordinator struct {
	server *coordinator.Service
	runner *serverRunner
}

func newTestCoordinator(t *testing.T, id string, httpPort int, rpcPort int, bootstrap bool) *testCoordinator {
	config := coordinator.ServiceConfig{
		ID:          id,
		HTTPListen:  fmt.Sprintf("%s:%d", clusterAddress, httpPort),
		Listen:      fmt.Sprintf("%s:%d", clusterAddress, rpcPort),
		Advertise:   fmt.Sprintf("%s:%d", clusterAddress, rpcPort),
		StorageDir:  t.TempDir(),
		Bootstrap:   bootstrap,
		DialOptions: []grpc.DialOption{creds},
		Log:         slog.Default(),
	}
	srv, err := coordinator.NewService(config)
	require.NoError(t, err)

	runner := newServerRunner(t)
	runner.runServer(srv)

	waitForHealthy(t, srv.HTTPServer.Listen)
	if bootstrap {
		waitForLeader(t, srv)
	}

	return &testCoordinator{
		server: srv,
		runner: runner,
	}
}

func (tc *testCoordinator) stop() {
	tc.runner.stop()
}

type testChain struct {
	cluster   *testCluster
	srvs      []*testChainServer
	allocator *portAllocator
}

func newTestChain(cluster *testCluster) *testChain {
	return &testChain{
		cluster:   cluster,
		allocator: newPortAllocator(8080),
	}
}

func (tc *testChain) addServer(t *testing.T, client *coordinatorclient.Client, id string) {
	ports := tc.allocator.allocate(1)
	srv := newTestChainServer(t, id, ports[0], false)
	tc.srvs = append(tc.srvs, srv)
	tc.cluster.addChainMemberRPC(t, client, srv.server.Node.ID, srv.server.Node.Address)
}

func (tc *testChain) removeServer(t *testing.T, client *coordinatorclient.Client, id string) {
	tc.cluster.removeChainMemberRPC(t, client, id)
}

func (tc *testChain) stop() {
	for _, srv := range tc.srvs {
		srv.stop()
	}
}

type testChainServer struct {
	server *chain.Service
	runner *serverRunner
}

func newTestChainServer(t *testing.T, id string, port int, bootstrap bool) *testChainServer {
	serverConfig := chain.ServiceConfig{
		ID:          id,
		ListenAddr:  fmt.Sprintf("%s:%d", chainAddress, port),
		StoragePath: t.TempDir(),
		DialOptions: []grpc.DialOption{creds},
		Log:         slog.Default(),
	}
	srv, err := chain.NewService(serverConfig)
	require.NoError(t, err)

	runner := newServerRunner(t)
	runner.runServer(srv)

	// Directly bootstrap the service as a single node chain.
	if bootstrap {
		config := node.EmptyChain.AddMember(srv.Node.ID, srv.Node.Address)
		req := &chainpb.UpdateConfigurationRequest{Configuration: config.Proto()}
		_, err := srv.Node.UpdateConfiguration(context.Background(), req)
		require.NoError(t, err)
	}

	return &testChainServer{
		server: srv,
		runner: runner,
	}
}

func (tcs *testChainServer) stop() {
	tcs.runner.stop()
}

type testProxyServer struct {
	server *proxy.Service
	runner *serverRunner
}

func newTestProxyServer(t *testing.T, clusterMembers []string) *testProxyServer {
	cfg := proxy.ServiceConfig{
		HTTPListen:   fmt.Sprintf("%s:8080", proxyAddress),
		GRPCListen:   fmt.Sprintf("%s:8081", proxyAddress),
		Coordinators: clusterMembers,
		DialOptions:  []grpc.DialOption{creds},
		Log:          slog.Default(),
	}
	srv, err := proxy.NewService(cfg)
	require.NoError(t, err)

	runner := newServerRunner(t)
	runner.runServer(srv)
	waitForHealthy(t, srv.HTTPServer.Address)

	return &testProxyServer{
		server: srv,
		runner: runner,
	}
}

func (tps *testProxyServer) stop() {
	tps.runner.stop()
}
