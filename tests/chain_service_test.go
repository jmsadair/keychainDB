package tests

import (
	"context"
	"io"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/chain/storage"
	pb "github.com/jmsadair/keychain/proto/chain"
	"github.com/stretchr/testify/require"
)

func TestPing(t *testing.T) {
	srv := newTestChainServer(t, "chain-node-1", 8080, true)
	defer srv.stop()

	ctx := context.Background()
	client := newTestChainClient(t)

	config := srv.server.Node.Configuration()

	var req pb.PingRequest
	resp, err := client.Ping(ctx, srv.server.Node.Address, &req)
	require.NoError(t, err)
	require.Equal(t, int32(node.Active), resp.GetStatus())
	require.Equal(t, config.Version, resp.GetConfigVersion())
}

func TestReplicateReadSingleMember(t *testing.T) {
	srv := newTestChainServer(t, "chain-node-1", 8080, true)
	defer srv.stop()

	ctx := context.Background()
	client := newTestChainClient(t)

	config := srv.server.Node.Configuration()
	head := config.Head()
	require.NotNil(t, head)

	req1 := &pb.ReplicateRequest{Key: "key-1", Value: []byte("value-1"), ConfigVersion: config.Version}
	_, err := client.Replicate(ctx, head.Address, req1)
	require.NoError(t, err)

	req2 := &pb.ReadRequest{Key: "key-1", ConfigVersion: config.Version}
	resp2, err := client.Read(ctx, head.Address, req2)
	require.NoError(t, err)
	require.Equal(t, []byte("value-1"), resp2.GetValue())
}

func TestDirtyKeysNotRead(t *testing.T) {
	srv1 := newTestChainServer(t, "chain-node-1", 8080, true)
	defer srv1.stop()
	srv2 := newTestChainServer(t, "chain-node-2", 8081, false)
	defer srv2.stop()
	srv3 := newTestChainServer(t, "chain-node-3", 8082, false)
	defer srv3.stop()

	ctx := context.Background()
	client := newTestChainClient(t)

	config := srv1.server.Node.Configuration()
	config = config.AddMember(srv2.server.Node.ID, srv2.server.Node.Address)
	config = config.AddMember(srv3.server.Node.ID, srv3.server.Node.Address)
	updateChainConfiguration(t, client, config, srv1, srv2, srv3)
	waitForActiveChainStatus(t, client, srv1, srv2, srv3)

	store := srv1.server.Node.Store()
	require.NoError(t, store.UncommittedWrite("key-1", []byte("value-1"), 1))
	store = srv2.server.Node.Store()
	require.NoError(t, store.UncommittedWrite("key-1", []byte("value-1"), 1))
	store = srv3.server.Node.Store()
	require.NoError(t, store.CommittedWrite("key-1", []byte("value-2"), 2))

	// Reads of dirty keys should be forwarded to the tail of the chain.
	req1 := &pb.ReadRequest{Key: "key-1", ConfigVersion: config.Version}
	resp1, err := client.Read(ctx, srv1.server.Node.Address, req1)
	require.NoError(t, err)
	require.Equal(t, []byte("value-2"), resp1.GetValue())
	req2 := &pb.ReadRequest{Key: "key-1", ConfigVersion: config.Version}
	resp2, err := client.Read(ctx, srv2.server.Node.Address, req2)
	require.NoError(t, err)
	require.Equal(t, []byte("value-2"), resp2.GetValue())
}

func TestReplicateReadMultipleMembers(t *testing.T) {
	srv1 := newTestChainServer(t, "chain-node-1", 8080, true)
	defer srv1.stop()
	srv2 := newTestChainServer(t, "chain-node-2", 8081, false)
	defer srv2.stop()
	srv3 := newTestChainServer(t, "chain-node-3", 8082, false)
	defer srv3.stop()

	ctx := context.Background()
	client := newTestChainClient(t)

	config := srv1.server.Node.Configuration()
	config = config.AddMember(srv2.server.Node.ID, srv2.server.Node.Address)
	config = config.AddMember(srv3.server.Node.ID, srv3.server.Node.Address)
	updateChainConfiguration(t, client, config, srv1, srv2, srv3)
	waitForActiveChainStatus(t, client, srv1, srv2, srv3)

	head := config.Head()
	require.NotNil(t, head)

	req := &pb.ReplicateRequest{Key: "key-1", Value: []byte("value-1"), ConfigVersion: config.Version}
	_, err := client.Replicate(ctx, head.Address, req)
	require.NoError(t, err)

	for _, srv := range []*testChainServer{srv1, srv2, srv3} {
		req := &pb.ReadRequest{Key: "key-1", ConfigVersion: config.Version}
		resp, err := client.Read(ctx, srv.server.Node.Address, req)
		require.NoError(t, err)
		require.Equal(t, []byte("value-1"), resp.GetValue())
	}
}

func TestReplicateReadMultipleMembersConcurrent(t *testing.T) {
	srv1 := newTestChainServer(t, "chain-node-1", 8080, true)
	defer srv1.stop()
	srv2 := newTestChainServer(t, "chain-node-2", 8081, false)
	defer srv2.stop()
	srv3 := newTestChainServer(t, "chain-node-3", 8082, false)
	defer srv3.stop()

	ctx := context.Background()
	client := newTestChainClient(t)

	config := srv1.server.Node.Configuration()
	config = config.AddMember(srv2.server.Node.ID, srv2.server.Node.Address)
	config = config.AddMember(srv3.server.Node.ID, srv3.server.Node.Address)
	updateChainConfiguration(t, client, config, srv1, srv2, srv3)
	waitForActiveChainStatus(t, client, srv1, srv2, srv3)

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
				req := &pb.ReplicateRequest{Key: key, Value: value, ConfigVersion: config.Version}
				_, err := client.Replicate(ctx, head.Address, req)
				require.NoError(t, err)
			}
		}(keys[i*100 : (i+1)*100])
	}

	close(readyCh)
	wg.Wait()

	for _, srv := range []*testChainServer{srv1, srv2, srv3} {
		for key, value := range kvPairs {
			req := &pb.ReadRequest{Key: key, ConfigVersion: config.Version}
			resp, err := client.Read(ctx, srv.server.Node.Address, req)
			require.NoError(t, err)
			require.Equal(t, value, resp.GetValue())
		}
	}
}

func TestAddNewMember(t *testing.T) {
	srv1 := newTestChainServer(t, "chain-node-1", 8080, true)
	defer srv1.stop()
	srv2 := newTestChainServer(t, "chain-node-2", 8081, false)
	defer srv2.stop()
	srv3 := newTestChainServer(t, "chain-node-3", 8082, false)
	defer srv3.stop()

	ctx := context.Background()
	client := newTestChainClient(t)

	config := srv1.server.Node.Configuration()
	config = config.AddMember(srv2.server.Node.ID, srv2.server.Node.Address)
	config = config.AddMember(srv3.server.Node.ID, srv3.server.Node.Address)
	updateChainConfiguration(t, client, config, srv1, srv2, srv3)
	waitForActiveChainStatus(t, client, srv1, srv2, srv3)

	head := config.Head()
	require.NotNil(t, head)

	numKeys := 1500
	kvPairs := makeKeyValuePairs(numKeys)
	keys := slices.Collect(maps.Keys(kvPairs))
	for i := range 500 {
		key := keys[i]
		req := &pb.ReplicateRequest{Key: key, Value: kvPairs[key], ConfigVersion: config.Version}
		_, err := client.Replicate(ctx, head.Address, req)
		require.NoError(t, err)
	}

	// Add two servers to the chain. Even if the new servers have yet to receive all of the key-value
	// pairs from their predecessor they should still be able to serve writes. They should be able to
	// serve once they are caught up.
	srv4 := newTestChainServer(t, "chain-node-4", 8083, false)
	defer srv4.stop()
	config = config.AddMember(srv4.server.Node.ID, srv4.server.Node.Address)
	updateChainConfiguration(t, client, config, srv1, srv2, srv3, srv4)
	for i := 500; i < 1000; i++ {
		key := keys[i]
		req := &pb.ReplicateRequest{Key: key, Value: kvPairs[key], ConfigVersion: config.Version}
		_, err := client.Replicate(ctx, head.Address, req)
		require.NoError(t, err)
	}
	waitForActiveChainStatus(t, client, srv4)
	for i := range 1000 {
		key := keys[i]
		req := &pb.ReadRequest{Key: key, ConfigVersion: config.Version}
		resp, err := client.Read(ctx, srv4.server.Node.Address, req)
		require.NoError(t, err)
		require.Equal(t, kvPairs[key], resp.GetValue())
	}
	srv5 := newTestChainServer(t, "chain-node-5", 8084, false)
	defer srv5.stop()
	config = config.AddMember(srv5.server.Node.ID, srv5.server.Node.Address)
	updateChainConfiguration(t, client, config, srv1, srv2, srv3, srv4, srv5)
	for i := 1000; i < numKeys; i++ {
		key := keys[i]
		req := &pb.ReplicateRequest{Key: key, Value: kvPairs[key], ConfigVersion: config.Version}
		_, err := client.Replicate(ctx, head.Address, req)
		require.NoError(t, err)
	}
	waitForActiveChainStatus(t, client, srv5)
	for key, value := range kvPairs {
		req := &pb.ReadRequest{Key: key, ConfigVersion: config.Version}
		resp, err := client.Read(ctx, srv4.server.Node.Address, req)
		require.NoError(t, err)
		require.Equal(t, value, resp.GetValue())
	}
}

func TestRemoveMember(t *testing.T) {
	ctx := context.Background()

	runTest := func(ids []string, toRemove string) {
		require.Len(t, ids, 3, "Only chains of length 3 supported")

		srv1 := newTestChainServer(t, "chain-node-1", 8080, true)
		defer srv1.stop()
		srv2 := newTestChainServer(t, "chain-node-2", 8081, false)
		defer srv2.stop()
		srv3 := newTestChainServer(t, "chain-node-3", 8082, false)
		defer srv3.stop()

		client := newTestChainClient(t)

		var atomicConfig atomic.Pointer[node.Configuration]
		config := srv1.server.Node.Configuration()
		config = config.AddMember(srv2.server.Node.ID, srv2.server.Node.Address)
		config = config.AddMember(srv3.server.Node.ID, srv3.server.Node.Address)
		updateChainConfiguration(t, client, config, srv1, srv2, srv3)
		waitForActiveChainStatus(t, client, srv1, srv2, srv3)
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
					req := &pb.ReplicateRequest{
						Key:           key,
						Value:         value,
						ConfigVersion: configVersion,
					}
					_, err := client.Replicate(ctx, head.Address, req)
					return err == nil
				}, eventuallyTimeout, eventuallyTick)
			}
		}()

		close(readyCh)

		// Remove the member.
		config = config.RemoveMember(toRemove)
		updateChainConfiguration(t, client, config, srv1, srv2, srv3)
		atomicConfig.Store(config)
		wg.Wait()

		for _, srv := range []*testChainServer{srv1, srv2, srv3} {
			if srv.server.Node.ID == toRemove {
				continue
			}
			for key, value := range kvPairs {
				req := &pb.ReadRequest{Key: key, ConfigVersion: config.Version}
				resp, err := client.Read(context.Background(), srv.server.Node.Address, req)
				require.NoError(t, err)
				require.Equal(t, value, resp.GetValue())
			}
		}
	}

	ids := []string{"chain-node-1", "chain-node-2", "chain-node-3"}
	for _, id := range ids {
		runTest(ids, id)
	}
}

func TestRemoveMultiple(t *testing.T) {
	srv1 := newTestChainServer(t, "chain-node-1", 8080, true)
	defer srv1.stop()
	srv2 := newTestChainServer(t, "chain-node-2", 8081, false)
	defer srv2.stop()
	srv3 := newTestChainServer(t, "chain-node-3", 8082, false)
	defer srv3.stop()

	ctx := context.Background()
	client := newTestChainClient(t)

	var atomicConfig atomic.Pointer[node.Configuration]
	config := srv1.server.Node.Configuration()
	config = config.AddMember(srv2.server.Node.ID, srv2.server.Node.Address)
	config = config.AddMember(srv3.server.Node.ID, srv3.server.Node.Address)
	updateChainConfiguration(t, client, config, srv1, srv2, srv3)
	waitForActiveChainStatus(t, client, srv1, srv2, srv3)
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
				req := &pb.ReplicateRequest{
					Key:           key,
					Value:         value,
					ConfigVersion: configVersion,
				}
				_, err := client.Replicate(ctx, head.Address, req)
				return err == nil
			}, eventuallyTimeout, eventuallyTick)
		}
	}()

	close(readyCh)

	// Remove the tail and middle nodes
	config = config.RemoveMember(srv3.server.Node.ID)
	config = config.RemoveMember(srv2.server.Node.ID)
	updateChainConfiguration(t, client, config, srv1, srv2, srv3)
	atomicConfig.Store(config)
	wg.Wait()

	for key, value := range kvPairs {
		req := &pb.ReadRequest{Key: key, ConfigVersion: config.Version}
		resp, err := client.Read(ctx, srv1.server.Node.Address, req)
		require.NoError(t, err)
		require.Equal(t, value, resp.GetValue())
	}
}

func TestPropagate(t *testing.T) {
	srv := newTestChainServer(t, "chain-node-1", 8080, true)
	defer srv.stop()

	ctx := context.Background()
	client := newTestChainClient(t)

	config := srv.server.Node.Configuration()
	head := config.Head()
	require.NotNil(t, head)

	numKeys := 1000
	kvPairs := makeKeyValuePairs(numKeys)
	store := srv.server.Node.Store()
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
		req := &pb.PropagateRequest{KeyType: filter.Proto(), ConfigVersion: config.Version}
		s, err := client.Propagate(ctx, head.Address, req)
		require.NoError(t, err)
		numKeysFiltered := 0
		for {
			kv, err := s.Recv()
			if err == io.EOF {
				break
			}
			if filter == storage.DirtyKeys {
				require.False(t, kv.GetIsCommitted())
			}

			if filter == storage.CommittedKeys {
				require.True(t, kv.GetIsCommitted())
			}
			require.NoError(t, err)
			require.Contains(t, expectedKvPairs, kv.GetKey())
			require.Equal(t, expectedKvPairs[kv.Key], kv.GetValue())
			numKeysFiltered++
		}
		require.Equal(t, len(expectedKvPairs), numKeysFiltered)
	}

	runTest(storage.AllKeys, allKeys)
	runTest(storage.CommittedKeys, committedKeys)
	runTest(storage.DirtyKeys, dirtyKeys)
}
