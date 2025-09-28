package chain

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	chaingrpc "github.com/jmsadair/keychain/chain/grpc"
	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/chain/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func makeServer(t *testing.T, id string, port int) *Server {
	srv, err := NewServer(id, fmt.Sprintf(":%d", port), t.TempDir())
	require.NoError(t, err)
	return srv
}

func startServer(t *testing.T, srv *Server) (*node.Configuration, func()) {
	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		require.NoError(t, srv.Run(ctx))
	}()

	config := node.EmptyChain.AddMember(srv.Node.ID, srv.Node.Address)
	require.NoError(t, srv.Node.UpdateConfiguration(
		context.Background(),
		&node.UpdateConfigurationRequest{Configuration: config},
		&node.UpdateConfigurationResponse{},
	))

	return config, func() {
		cancel()
		wg.Wait()
	}
}

func TestReplicateRead(t *testing.T) {
	srv := makeServer(t, "chain-node-1", 8080)
	config, cancel := startServer(t, srv)
	defer cancel()
	client, err := chaingrpc.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	head := config.Head()
	require.NotNil(t, head)

	// Store a key-value pair.
	req1 := &node.ReplicateRequest{Key: "key-1", Value: []byte("value-1"), ConfigVersion: config.Version}
	var resp1 node.ReplicateResponse
	require.NoError(t, client.Replicate(context.Background(), head.Address, req1, &resp1))

	// Read the key-value pair back.
	req2 := &node.ReadRequest{Key: "key-1", ConfigVersion: config.Version}
	var resp2 node.ReadResponse
	require.NoError(t, client.Read(context.Background(), head.Address, req2, &resp2))
	require.Equal(t, []byte("value-1"), resp2.Value)
}

func TestPropagate(t *testing.T) {
	srv := makeServer(t, "chain-node-1", 8080)
	config, cancel := startServer(t, srv)
	defer cancel()
	client, err := chaingrpc.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	head := config.Head()
	require.NotNil(t, head)

	// Store key-value pairs.
	numKeys := 1000
	kvPairs := make(map[string][]byte, numKeys)
	for i := range numKeys {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Appendf(nil, "value-%d", i)
		kvPairs[key] = value
		req := &node.ReplicateRequest{Key: key, Value: value, ConfigVersion: config.Version}
		var resp node.ReplicateResponse
		require.NoError(t, client.Replicate(context.Background(), head.Address, req, &resp))
	}

	// Stream the written key-value pairs back. This is a single node chain so all keys should be committed.
	req := &node.PropagateRequest{KeyFilter: storage.CommittedKeys, ConfigVersion: config.Version}
	s, err := client.Propagate(context.Background(), head.Address, req)
	require.NoError(t, err)
	for {
		kv, err := s.Receive()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Contains(t, kvPairs, kv.Key)
		require.Equal(t, kvPairs[kv.Key], kv.Value)
		delete(kvPairs, kv.Key)
	}

	require.Empty(t, kvPairs)
}
