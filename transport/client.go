package transport

import (
	"context"
	"net"
	"sync"

	pb "github.com/jmsadair/zebraos/proto/pbchain"
	"github.com/jmsadair/zebraos/storage"
	"google.golang.org/grpc"
)

// ChainClient is a gRPC-based implementation of the Client interface.
type ChainClient struct {
	mu       sync.Mutex
	clients  map[string]pb.ChainServiceClient
	dialOpts []grpc.DialOption
}

// NewChainClient creates a new gRPC-based chain client with the provided dial options.
func NewChainClient(dialOpts ...grpc.DialOption) (*ChainClient, error) {
	return &ChainClient{
		clients:  make(map[string]pb.ChainServiceClient),
		dialOpts: dialOpts,
	}, nil
}

// Write writes a versioned key-value to another node in the chain.
func (cc *ChainClient) Write(ctx context.Context, address net.Addr, key string, value []byte, version uint64) error {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return err
	}
	_, err = client.Write(ctx, &pb.WriteRequest{Key: key, Value: value, Version: version})
	return err
}

// Read reads a key-value pair from a node in the chain.
func (cc *ChainClient) Read(ctx context.Context, address net.Addr, key string) ([]byte, error) {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return nil, err
	}
	response, err := client.Read(ctx, &pb.ReadRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return response.GetValue(), nil
}

// Commit commits a particular version of a key to storage.
func (cc *ChainClient) Commit(ctx context.Context, address net.Addr, key string, version uint64) error {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return err
	}
	_, err = client.Commit(ctx, &pb.CommitRequest{Key: key, Version: version})
	return err
}

// Propagate initiates a key-value stream where the keys will be filtered according to provided filter.
func (cc *ChainClient) Propagate(ctx context.Context, address net.Addr, keyFilter storage.KeyFilter) (*KeyValueReceiveStream, error) {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return nil, err
	}

	var keyType pb.KeyType
	switch keyFilter {
	case storage.AllKeys:
		keyType = pb.KeyType_KEYTYPE_ALL
	case storage.CommittedKeys:
		keyType = pb.KeyType_KEYTYPE_COMMITTED
	case storage.DirtyKeys:
		keyType = pb.KeyType_KEYTYPE_DIRTY
	}

	stream, err := client.Propagate(ctx, &pb.PropagateRequest{KeyType: keyType})
	if err != nil {
		return nil, err
	}
	return &KeyValueReceiveStream{Stream: stream}, nil
}

func (cc *ChainClient) getOrCreateClient(address net.Addr) (pb.ChainServiceClient, error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	client, ok := cc.clients[address.String()]
	if !ok {
		conn, err := grpc.NewClient(address.String(), cc.dialOpts...)
		if err != nil {
			return nil, err
		}
		client = pb.NewChainServiceClient(conn)
		cc.clients[address.String()] = client
	}

	return client, nil
}
