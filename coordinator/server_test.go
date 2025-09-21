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

	chaingrpc "github.com/jmsadair/keychain/chain/grpc"
	coordinatorhttp "github.com/jmsadair/keychain/coordinator/http"
	"github.com/jmsadair/keychain/coordinator/raft"
	"github.com/stretchr/testify/require"
)

func waitForLeader(t *testing.T, srv *Server) {
	require.Eventually(t, func() bool {
		status, err := srv.Raft.ClusterStatus()
		return err == nil && status.Leader == srv.Raft.ID
	}, 5*time.Second, 100*time.Millisecond)
}

func makeServer(t *testing.T, id string, httpAddr string, raftAddr string, bootstrap bool) *Server {
	tn, err := chaingrpc.NewClient()
	require.NoError(t, err)
	srv, err := NewServer(id, httpAddr, raftAddr, tn, t.TempDir(), t.TempDir(), bootstrap)
	require.NoError(t, err)
	return srv
}

func makeCluster(t *testing.T) (map[string]*Server, string) {
	idToServer := make(map[string]*Server, 3)
	var bootstrapped string
	for i := range 3 {
		id := fmt.Sprintf("coordinator-%d", i)
		httpAddr := fmt.Sprintf("127.0.0.%d:8080", i)
		raftAddr := fmt.Sprintf("127.0.0.%d:8081", i)
		idToServer[id] = makeServer(t, id, httpAddr, raftAddr, i == 0)
		if i == 0 {
			bootstrapped = id
		}
	}

	return idToServer, bootstrapped
}

func startCluster(t *testing.T, idToServer map[string]*Server, bootstrapped string) func() {
	var wg sync.WaitGroup
	wg.Add(len(idToServer))
	ctx, cancel := context.WithCancel(context.Background())
	for _, srv := range idToServer {
		go func() {
			srv.Run(ctx)
			wg.Done()
		}()
	}

	var wgHealth sync.WaitGroup
	wgHealth.Add(len(idToServer))
	for _, srv := range idToServer {
		go func() {
			defer wgHealth.Done()
			waitForHealthy := func() bool {
				healthURL := fmt.Sprintf("http://%s/healthz", srv.HTTPServer.Address)
				resp, err := http.Get(healthURL)
				if err != nil {
					return false
				}
				defer resp.Body.Close()
				return resp.StatusCode == http.StatusOK
			}
			require.Eventually(t, waitForHealthy, 3*time.Second, 100*time.Millisecond)
		}()
	}
	wgHealth.Wait()

	waitForLeader(t, idToServer[bootstrapped])

	return func() {
		cancel()
		wg.Wait()
	}
}

func TestJoinClusterThenRemove(t *testing.T) {
	idToServer, bootstrapped := makeCluster(t)
	cancel := startCluster(t, idToServer, bootstrapped)
	defer cancel()

	statusURL := fmt.Sprintf("http://%s/cluster/status", idToServer[bootstrapped].HTTPServer.Address)
	resp, err := http.Get(statusURL)
	require.NoError(t, err)
	var status raft.Status
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&status))
	require.Len(t, status.Members, 1)
	require.Equal(t, status.Members[bootstrapped], idToServer[bootstrapped].Raft.Address)

	joinURL := fmt.Sprintf("http://%s/cluster/join", idToServer[bootstrapped].HTTPServer.Address)
	for id, srv := range idToServer {
		if id == bootstrapped {
			continue
		}
		req := coordinatorhttp.JoinClusterRequest{ID: id, Address: srv.Raft.Address}
		b, err := json.Marshal(req)
		require.NoError(t, err)
		_, err = http.Post(joinURL, "application/json", bytes.NewReader(b))
		require.NoError(t, err)
	}

	resp, err = http.Get(statusURL)
	require.NoError(t, err)
	status = raft.Status{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&status))
	require.Len(t, status.Members, 3)
	for id, srv := range idToServer {
		require.Equal(t, status.Members[id], srv.Raft.Address)
	}

	removeURL := fmt.Sprintf("http://%s/cluster/remove", idToServer[bootstrapped].HTTPServer.Address)
	for id := range idToServer {
		if id == bootstrapped {
			continue
		}
		req := coordinatorhttp.RemoveFromClusterRequest{ID: id}
		b, err := json.Marshal(req)
		require.NoError(t, err)
		_, err = http.Post(removeURL, "application/json", bytes.NewReader(b))
		require.NoError(t, err)
	}

	resp, err = http.Get(statusURL)
	require.NoError(t, err)
	status = raft.Status{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&status))
	require.Len(t, status.Members, 1)
	require.Equal(t, status.Members[bootstrapped], idToServer[bootstrapped].Raft.Address)
}
