package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jmsadair/keychain/chain"
	"github.com/jmsadair/keychain/coordinator"
	proxyhttp "github.com/jmsadair/keychain/proxy/http"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultTimeout = 5 * time.Second
	defaultTick    = 100 * time.Millisecond
)

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
		rpcAddr := fmt.Sprintf("127.0.0.1:%d", basePort)
		raftAddr := fmt.Sprintf("127.0.0.1:%d", basePort+1)
		httpAddr := fmt.Sprintf("127.0.0.1:%d", basePort+2)
		basePort += 3
		c, err := coordinator.NewServer(
			id,
			httpAddr,
			rpcAddr,
			raftAddr,
			t.TempDir(),
			t.TempDir(),
			i == 0,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		tc.coordinators = append(tc.coordinators, c)
		go func() {
			defer tc.wg.Done()
			require.NoError(t, c.Run(ctx))
		}()
	}

	require.Eventually(t, func() bool {
		bootstrapped := tc.coordinators[0]
		status, err := bootstrapped.Raft.ClusterStatus()
		if err == nil && status.Leader == bootstrapped.Raft.ID {
			tc.leader = bootstrapped
			return true
		}
		return false
	}, defaultTimeout, defaultTick)

	for _, srv := range tc.coordinators {
		if srv == tc.leader {
			continue
		}
		require.NoError(t, tc.leader.Coordinator.Raft.JoinCluster(context.Background(), srv.Raft.ID, srv.Raft.Address))
	}

	return tc
}

func (tc *testCluster) stop() {
	tc.cancel()
	tc.wg.Wait()
}

func (tc *testCluster) addChainMember(t *testing.T, id string, addr string) {
	require.NoError(t, tc.leader.Coordinator.AddMember(context.Background(), id, addr))
}

func (tc *testCluster) removeChainMember(t *testing.T, id string) {
	require.NoError(t, tc.leader.Coordinator.RemoveMember(context.Background(), id))
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
	c, err := chain.NewServer(
		id,
		fmt.Sprintf("127.0.0.2:%d", 8080+len(tc.nodes)),
		t.TempDir(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	tc.cancels = append(tc.cancels, cancel)
	tc.nodes = append(tc.nodes, c)
	tc.wg.Add(1)
	go func() {
		defer tc.wg.Done()
		require.NoError(t, c.Run(ctx))
	}()

	tc.cluster.addChainMember(t, c.Node.ID, c.Node.Address)
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
	srv, err := NewServer(
		"127.0.0.3:8080",
		clusterMembers,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, srv.Run(ctx))
	}()

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://%s/healthz", srv.HTTPServer.Address))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, defaultTimeout, defaultTick)

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

	key := "key-1"
	value := []byte("value-1")
	setURL := fmt.Sprintf("http://%s/set", proxy.HTTPServer.Address)
	setRequest := proxyhttp.SetRequest{Key: key, Value: value}
	b, err := json.Marshal(&setRequest)
	require.NoError(t, err)

	setResponse, err := http.Post(setURL, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	defer setResponse.Body.Close()
	require.Equal(t, http.StatusNoContent, setResponse.StatusCode)

	getURL := fmt.Sprintf("http://%s/get", proxy.HTTPServer.Address)
	b, err = json.Marshal(&proxyhttp.GetRequest{Key: key})
	require.NoError(t, err)
	getRequest, err := http.NewRequest(http.MethodGet, getURL, bytes.NewReader(b))
	require.NoError(t, err)

	getResponse, err := http.DefaultClient.Do(getRequest)
	require.NoError(t, err)
	defer getResponse.Body.Close()
	readValue, err := io.ReadAll(getResponse.Body)
	require.NoError(t, err)
	require.Equal(t, value, readValue)
	require.Equal(t, http.StatusOK, getResponse.StatusCode)
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

	key := "key-1"
	value := []byte("value-1")
	setURL := fmt.Sprintf("http://%s/set", proxy.HTTPServer.Address)
	setRequest := proxyhttp.SetRequest{Key: key, Value: value}
	b, err := json.Marshal(&setRequest)
	require.NoError(t, err)

	setResponse, err := http.Post(setURL, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	defer setResponse.Body.Close()
	require.Equal(t, http.StatusNoContent, setResponse.StatusCode)

	// Remove the tail.
	chain.removeMember(t, "chain-node-3")

	getURL := fmt.Sprintf("http://%s/get", proxy.HTTPServer.Address)
	b, err = json.Marshal(&proxyhttp.GetRequest{Key: key})
	require.NoError(t, err)
	getRequest, err := http.NewRequest(http.MethodGet, getURL, bytes.NewReader(b))
	require.NoError(t, err)

	// This may fail due to new tail not yet having committed all of its key-value pairs.
	var getResponse *http.Response
	require.Eventually(t, func() bool {
		getResponse, err = http.DefaultClient.Do(getRequest)
		return err == nil && getResponse.StatusCode == http.StatusOK
	}, 1*time.Second, 50*time.Millisecond)

	defer getResponse.Body.Close()
	readValue, err := io.ReadAll(getResponse.Body)
	require.NoError(t, err)
	require.Equal(t, value, readValue)
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

	key := "key-1"
	value := []byte("value-1")
	setURL := fmt.Sprintf("http://%s/set", proxy.HTTPServer.Address)
	setRequest := proxyhttp.SetRequest{Key: key, Value: value}
	b, err := json.Marshal(&setRequest)
	require.NoError(t, err)

	setResponse, err := http.Post(setURL, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	defer setResponse.Body.Close()
	require.Equal(t, http.StatusNoContent, setResponse.StatusCode)

	// Add a member to the tail.
	chain.addMember(t, "chain-node-3")

	getURL := fmt.Sprintf("http://%s/get", proxy.HTTPServer.Address)
	b, err = json.Marshal(&proxyhttp.GetRequest{Key: key})
	require.NoError(t, err)
	getRequest, err := http.NewRequest(http.MethodGet, getURL, bytes.NewReader(b))
	require.NoError(t, err)

	getResponse, err := http.DefaultClient.Do(getRequest)
	require.NoError(t, err)
	defer getResponse.Body.Close()
	readValue, err := io.ReadAll(getResponse.Body)
	require.NoError(t, err)
	require.Equal(t, value, readValue)
	require.Equal(t, http.StatusOK, getResponse.StatusCode)
}
