package chain

import (
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chaingrpc "github.com/jmsadair/keychain/chain/grpc"
	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/chain/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultTimeout = 3 * time.Second
	defaultTick    = 10 * time.Millisecond
)

func makeServer(t *testing.T, id string, port int, bootstrapConfig bool) (*Server, func()) {
	srv, err := NewServer(id, fmt.Sprintf(":%d", port), t.TempDir(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		require.NoError(t, srv.Run(ctx))
	}()

	// Make this server a single member chain.
	if bootstrapConfig {
		config := node.EmptyChain.AddMember(srv.Node.ID, srv.Node.Address)
		updateConfiguration(t, config, srv)
	}

	return srv, func() {
		cancel()
		wg.Wait()
	}
}

func updateConfiguration(t *testing.T, config *node.Configuration, srvs ...*Server) {
	for _, srv := range srvs {
		require.NoError(t, srv.Node.UpdateConfiguration(
			context.Background(),
			&node.UpdateConfigurationRequest{Configuration: config},
			&node.UpdateConfigurationResponse{},
		))
	}
}

func waitForActiveStatus(t *testing.T, c *chaingrpc.Client, srvs ...*Server) {
	for _, srv := range srvs {
		isActive := func() bool {
			var req node.PingRequest
			var resp node.PingResponse
			err := c.Ping(context.Background(), srv.Node.Address, &req, &resp)
			if err != nil {
				return false
			}
			return resp.Status == node.Active
		}

		require.Eventually(t, isActive, defaultTimeout, defaultTick)
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

func TestPing(t *testing.T) {
	srv, cancel := makeServer(t, "chain-node-1", 8080, true)
	defer cancel()
	config := srv.Node.Configuration()

	client, err := chaingrpc.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	var req node.PingRequest
	var resp node.PingResponse
	require.NoError(t, client.Ping(context.TODO(), srv.Node.Address, &req, &resp))
	require.Equal(t, node.Active, resp.Status)
	require.Equal(t, config.Version, resp.Version)
}

func TestReplicateReadSingleMember(t *testing.T) {
	srv, cancel := makeServer(t, "chain-node-1", 8080, true)
	defer cancel()
	config := srv.Node.Configuration()

	client, err := chaingrpc.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	head := config.Head()
	require.NotNil(t, head)

	req1 := &node.ReplicateRequest{Key: "key-1", Value: []byte("value-1"), ConfigVersion: config.Version}
	var resp1 node.ReplicateResponse
	require.NoError(t, client.Replicate(context.Background(), head.Address, req1, &resp1))

	req2 := &node.ReadRequest{Key: "key-1", ConfigVersion: config.Version}
	var resp2 node.ReadResponse
	require.NoError(t, client.Read(context.Background(), head.Address, req2, &resp2))
	require.Equal(t, []byte("value-1"), resp2.Value)
}

func TestDirtyKeysNotRead(t *testing.T) {
	srv1, cancel1 := makeServer(t, "chain-node-1", 8080, true)
	defer cancel1()
	srv2, cancel2 := makeServer(t, "chain-node-2", 8081, false)
	defer cancel2()
	srv3, cancel3 := makeServer(t, "chain-node-3", 8082, false)
	defer cancel3()

	client, err := chaingrpc.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	config := srv1.Node.Configuration()
	config = config.AddMember(srv2.Node.ID, srv2.Node.Address)
	config = config.AddMember(srv3.Node.ID, srv3.Node.Address)
	updateConfiguration(t, config, srv1, srv2, srv3)
	waitForActiveStatus(t, client, srv1, srv2, srv3)

	store := srv1.Node.Store()
	require.NoError(t, store.UncommittedWrite("key-1", []byte("value-1"), 1))
	store = srv2.Node.Store()
	require.NoError(t, store.UncommittedWrite("key-1", []byte("value-1"), 1))
	store = srv3.Node.Store()
	require.NoError(t, store.CommittedWrite("key-1", []byte("value-2"), 2))

	// Reads of dirty keys should be forwarded to the tail of the chain.
	readReq1 := &node.ReadRequest{Key: "key-1", ConfigVersion: config.Version}
	var readResp1 node.ReadResponse
	require.NoError(t, client.Read(context.Background(), srv1.Node.Address, readReq1, &readResp1))
	require.Equal(t, []byte("value-2"), readResp1.Value)
	readReq2 := &node.ReadRequest{Key: "key-1", ConfigVersion: config.Version}
	var readResp2 node.ReadResponse
	require.NoError(t, client.Read(context.Background(), srv2.Node.Address, readReq2, &readResp2))
	require.Equal(t, []byte("value-2"), readResp2.Value)
}

func TestReplicateReadMultipleMembers(t *testing.T) {
	srv1, cancel1 := makeServer(t, "chain-node-1", 8080, true)
	defer cancel1()
	srv2, cancel2 := makeServer(t, "chain-node-2", 8081, false)
	defer cancel2()
	srv3, cancel3 := makeServer(t, "chain-node-3", 8082, false)
	defer cancel3()

	client, err := chaingrpc.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	config := srv1.Node.Configuration()
	config = config.AddMember(srv2.Node.ID, srv2.Node.Address)
	config = config.AddMember(srv3.Node.ID, srv3.Node.Address)
	updateConfiguration(t, config, srv1, srv2, srv3)
	waitForActiveStatus(t, client, srv1, srv2, srv3)

	head := config.Head()
	require.NotNil(t, head)

	replicateReq := &node.ReplicateRequest{Key: "key-1", Value: []byte("value-1"), ConfigVersion: config.Version}
	var replicateResp node.ReplicateResponse
	require.NoError(t, client.Replicate(context.Background(), head.Address, replicateReq, &replicateResp))

	for _, srv := range []*Server{srv1, srv2, srv3} {
		readReq := &node.ReadRequest{Key: "key-1", ConfigVersion: config.Version}
		var readResp node.ReadResponse
		require.NoError(t, client.Read(context.Background(), srv.Node.Address, readReq, &readResp))
		require.Equal(t, []byte("value-1"), readResp.Value)
	}
}

func TestReplicateReadMultipleMembersConcurrent(t *testing.T) {
	srv1, cancel1 := makeServer(t, "chain-node-1", 8080, true)
	defer cancel1()
	srv2, cancel2 := makeServer(t, "chain-node-2", 8081, false)
	defer cancel2()
	srv3, cancel3 := makeServer(t, "chain-node-3", 8082, false)
	defer cancel3()

	client, err := chaingrpc.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	config := srv1.Node.Configuration()
	config = config.AddMember(srv2.Node.ID, srv2.Node.Address)
	config = config.AddMember(srv3.Node.ID, srv3.Node.Address)
	updateConfiguration(t, config, srv1, srv2, srv3)
	waitForActiveStatus(t, client, srv1, srv2, srv3)

	head := config.Head()
	require.NotNil(t, head)

	var wg sync.WaitGroup
	numKeys := 1000
	numWorkers := 10
	readyCh := make(chan any)
	kvPairs := makeKeyValuePairs(numKeys)
	keys := slices.Collect(maps.Keys(kvPairs))
	wg.Add(numWorkers)
	for i := range numWorkers {
		go func(keys []string) {
			defer wg.Done()
			<-readyCh
			for _, key := range keys {
				value := kvPairs[key]
				req := &node.ReplicateRequest{Key: key, Value: value, ConfigVersion: config.Version}
				var resp node.ReplicateResponse
				require.NoError(t, client.Replicate(context.Background(), head.Address, req, &resp))
			}
		}(keys[i*100 : (i+1)*100])
	}

	close(readyCh)
	wg.Wait()

	for _, srv := range []*Server{srv1, srv2, srv3} {
		for key, value := range kvPairs {
			req := &node.ReadRequest{Key: key, ConfigVersion: config.Version}
			var resp node.ReadResponse
			require.NoError(t, client.Read(context.Background(), srv.Node.Address, req, &resp))
			require.Equal(t, value, resp.Value)
		}
	}
}

func TestAddNewMember(t *testing.T) {
	srv1, cancel1 := makeServer(t, "chain-node-1", 8080, true)
	defer cancel1()
	srv2, cancel2 := makeServer(t, "chain-node-2", 8081, false)
	defer cancel2()
	srv3, cancel3 := makeServer(t, "chain-node-3", 8082, false)
	defer cancel3()

	client, err := chaingrpc.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	config := srv1.Node.Configuration()
	config = config.AddMember(srv2.Node.ID, srv2.Node.Address)
	config = config.AddMember(srv3.Node.ID, srv3.Node.Address)
	updateConfiguration(t, config, srv1, srv2, srv3)
	waitForActiveStatus(t, client, srv1, srv2, srv3)

	head := config.Head()
	require.NotNil(t, head)

	numKeys := 1500
	kvPairs := makeKeyValuePairs(numKeys)
	keys := slices.Collect(maps.Keys(kvPairs))
	for i := range 500 {
		key := keys[i]
		req := &node.ReplicateRequest{Key: key, Value: kvPairs[key], ConfigVersion: config.Version}
		var resp node.ReplicateResponse
		require.NoError(t, client.Replicate(context.Background(), head.Address, req, &resp))
	}

	// Add two servers to the chain. Even if the new servers have yet to receive all of the key-value
	// pairs from their predecessor they should still be able to serve writes. They should be able to
	// serve once they are caught up.
	srv4, cancel4 := makeServer(t, "chain-node-4", 8083, false)
	defer cancel4()
	config = config.AddMember(srv4.Node.ID, srv4.Node.Address)
	updateConfiguration(t, config, srv1, srv2, srv3, srv4)
	for i := 500; i < 1000; i++ {
		key := keys[i]
		req := &node.ReplicateRequest{Key: key, Value: kvPairs[key], ConfigVersion: config.Version}
		var resp node.ReplicateResponse
		require.NoError(t, client.Replicate(context.Background(), head.Address, req, &resp))
	}
	waitForActiveStatus(t, client, srv4)
	for i := range 1000 {
		key := keys[i]
		req := &node.ReadRequest{Key: key, ConfigVersion: config.Version}
		var resp node.ReadResponse
		require.NoError(t, client.Read(context.Background(), srv4.Node.Address, req, &resp))
		require.Equal(t, kvPairs[key], resp.Value)
	}
	srv5, cancel5 := makeServer(t, "chain-node-5", 8084, false)
	defer cancel5()
	config = config.AddMember(srv5.Node.ID, srv5.Node.Address)
	updateConfiguration(t, config, srv1, srv2, srv3, srv4, srv5)
	for i := 1000; i < numKeys; i++ {
		key := keys[i]
		req := &node.ReplicateRequest{Key: key, Value: kvPairs[key], ConfigVersion: config.Version}
		var resp node.ReplicateResponse
		require.NoError(t, client.Replicate(context.Background(), head.Address, req, &resp))
	}
	waitForActiveStatus(t, client, srv5)
	for key, value := range kvPairs {
		req := &node.ReadRequest{Key: key, ConfigVersion: config.Version}
		var resp node.ReadResponse
		require.NoError(t, client.Read(context.Background(), srv5.Node.Address, req, &resp))
		require.Equal(t, value, resp.Value)
	}
}

func TestRemoveMember(t *testing.T) {

	runTest := func(ids []string, toRemove string) {
		require.Len(t, ids, 3, "Only chains of length 3 supported")

		srv1, cancel1 := makeServer(t, ids[0], 8080, true)
		defer cancel1()
		srv2, cancel2 := makeServer(t, ids[1], 8081, false)
		defer cancel2()
		srv3, cancel3 := makeServer(t, ids[2], 8082, false)
		defer cancel3()

		client, err := chaingrpc.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)

		var atomicConfig atomic.Pointer[node.Configuration]
		config := srv1.Node.Configuration()
		config = config.AddMember(srv2.Node.ID, srv2.Node.Address)
		config = config.AddMember(srv3.Node.ID, srv3.Node.Address)
		updateConfiguration(t, config, srv1, srv2, srv3)
		waitForActiveStatus(t, client, srv1, srv2, srv3)
		atomicConfig.Store(config)

		numKeys := 500
		kvPairs := makeKeyValuePairs(numKeys)
		readyCh := make(chan any)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-readyCh
			for key, value := range kvPairs {
				require.Eventually(t, func() bool {
					c := atomicConfig.Load()
					head := c.Head()
					configVersion := c.Version
					req := &node.ReplicateRequest{
						Key:           key,
						Value:         value,
						ConfigVersion: configVersion,
					}
					var resp node.ReplicateResponse
					return client.Replicate(context.Background(), head.Address, req, &resp) == nil
				}, defaultTimeout, defaultTick)
			}
		}()

		close(readyCh)

		// Remove the member.
		config = config.RemoveMember(toRemove)
		updateConfiguration(t, config, srv1, srv2, srv3)
		atomicConfig.Store(config)
		wg.Wait()

		for _, srv := range []*Server{srv1, srv2, srv3} {
			if srv.Node.ID == toRemove {
				continue
			}
			for key, value := range kvPairs {
				req := &node.ReadRequest{Key: key, ConfigVersion: config.Version}
				var resp node.ReadResponse
				require.NoError(t, client.Read(context.Background(), srv.Node.Address, req, &resp))
				require.Equal(t, value, resp.Value)
			}
		}
	}

	ids := []string{"chain-node-1", "chain-node-2", "chain-node-3"}
	for _, id := range ids {
		runTest(ids, id)
	}
}

func TestRemoveMultiple(t *testing.T) {
	srv1, cancel1 := makeServer(t, "chain-node-1", 8080, true)
	defer cancel1()
	srv2, cancel2 := makeServer(t, "chain-node-2", 8081, false)
	defer cancel2()
	srv3, cancel3 := makeServer(t, "chain-node-3", 8082, false)
	defer cancel3()

	client, err := chaingrpc.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	var atomicConfig atomic.Pointer[node.Configuration]
	config := srv1.Node.Configuration()
	config = config.AddMember(srv2.Node.ID, srv2.Node.Address)
	config = config.AddMember(srv3.Node.ID, srv3.Node.Address)
	updateConfiguration(t, config, srv1, srv2, srv3)
	waitForActiveStatus(t, client, srv1, srv2, srv3)
	atomicConfig.Store(config)

	numKeys := 500
	kvPairs := makeKeyValuePairs(numKeys)
	readyCh := make(chan any)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-readyCh
		for key, value := range kvPairs {
			require.Eventually(t, func() bool {
				c := atomicConfig.Load()
				head := c.Head()
				configVersion := c.Version
				req := &node.ReplicateRequest{
					Key:           key,
					Value:         value,
					ConfigVersion: configVersion,
				}
				var resp node.ReplicateResponse
				return client.Replicate(context.Background(), head.Address, req, &resp) == nil
			}, defaultTimeout, defaultTick)
		}
	}()

	close(readyCh)

	// Remove the tail and middle nodes
	config = config.RemoveMember(srv3.Node.ID)
	config = config.RemoveMember(srv2.Node.ID)
	updateConfiguration(t, config, srv1, srv2, srv3)
	atomicConfig.Store(config)
	wg.Wait()

	for key, value := range kvPairs {
		req := &node.ReadRequest{Key: key, ConfigVersion: config.Version}
		var resp node.ReadResponse
		require.NoError(t, client.Read(context.Background(), srv1.Node.Address, req, &resp))
		require.Equal(t, value, resp.Value)
	}
}

func TestPropagate(t *testing.T) {
	srv, cancel := makeServer(t, "chain-node-1", 8080, true)
	defer cancel()
	config := srv.Node.Configuration()

	client, err := chaingrpc.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	head := config.Head()
	require.NotNil(t, head)

	numKeys := 1000
	kvPairs := makeKeyValuePairs(numKeys)
	store := srv.Node.Store()
	dirtyKeys := make(map[string][]byte, 500)
	committedKeys := make(map[string][]byte, 500)
	allKeys := make(map[string][]byte, 1000)
	i := 0
	for key, value := range kvPairs {
		if i < 500 {
			store.CommittedWriteNewVersion(key, value)
			committedKeys[key] = value
		} else {
			store.UncommittedWrite(key, value, 1)
			dirtyKeys[key] = value
		}
		allKeys[key] = value
		i++
	}

	runTest := func(filter storage.KeyFilter, expectedKvPairs map[string][]byte) {
		req := &node.PropagateRequest{KeyFilter: filter, ConfigVersion: config.Version}
		s, err := client.Propagate(context.Background(), head.Address, req)
		require.NoError(t, err)
		numKeysFiltered := 0
		for {
			kv, err := s.Receive()
			if err == io.EOF {
				break
			}
			if filter == storage.DirtyKeys {
				require.False(t, kv.Committed)
			}

			if filter == storage.CommittedKeys {
				require.True(t, kv.Committed)
			}
			require.NoError(t, err)
			require.Contains(t, expectedKvPairs, kv.Key)
			require.Equal(t, expectedKvPairs[kv.Key], kv.Value)
			numKeysFiltered++
		}
		require.Equal(t, len(expectedKvPairs), numKeysFiltered)
	}

	runTest(storage.AllKeys, allKeys)
	runTest(storage.CommittedKeys, committedKeys)
	runTest(storage.DirtyKeys, dirtyKeys)
}
