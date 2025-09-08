package grpc

import (
	"context"
	"net"
	"sync"

	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/chain/storage"
	pb "github.com/jmsadair/keychain/proto/pbchain"
	"google.golang.org/grpc"
)

type gRPCReceiveStream struct {
	stream grpc.ClientStream
}

func (g *gRPCReceiveStream) Receive() (*storage.KeyValuePair, error) {
	var msg *pb.KeyValuePair
	if err := g.stream.RecvMsg(msg); err != nil {
		return nil, err
	}
	return &storage.KeyValuePair{Key: msg.GetKey(), Value: msg.GetValue(), Version: msg.GetVersion(), Committed: msg.GetIsCommitted()}, nil
}

// Client is a grpc-based transport for used to communicate with chain nodes.
type Client struct {
	mu       sync.Mutex
	clients  map[string]pb.ChainServiceClient
	dialOpts []grpc.DialOption
}

// NewClient creates a new gRPC-based chain client with the provided dial options.
func NewClient(dialOpts ...grpc.DialOption) (*Client, error) {
	return &Client{
		clients:  make(map[string]pb.ChainServiceClient),
		dialOpts: dialOpts,
	}, nil
}

func (g *Client) Write(ctx context.Context, address net.Addr, key string, value []byte, version uint64) error {
	client, err := g.getOrCreateClient(address)
	if err != nil {
		return err
	}
	_, err = client.Write(ctx, &pb.WriteRequest{Key: key, Value: value, Version: version})
	return err
}

func (g *Client) Read(ctx context.Context, address net.Addr, key string) ([]byte, error) {
	client, err := g.getOrCreateClient(address)
	if err != nil {
		return nil, err
	}
	response, err := client.Read(ctx, &pb.ReadRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return response.GetValue(), nil
}

func (g *Client) Commit(ctx context.Context, address net.Addr, key string, version uint64) error {
	client, err := g.getOrCreateClient(address)
	if err != nil {
		return err
	}
	_, err = client.Commit(ctx, &pb.CommitRequest{Key: key, Version: version})
	return err
}

func (g *Client) Propagate(ctx context.Context, address net.Addr, keyFilter storage.KeyFilter) (node.KeyValueReceiveStream, error) {
	client, err := g.getOrCreateClient(address)
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
	return &gRPCReceiveStream{stream: stream}, nil
}

func (g *Client) getOrCreateClient(address net.Addr) (pb.ChainServiceClient, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	client, ok := g.clients[address.String()]
	if !ok {
		conn, err := grpc.NewClient(address.String(), g.dialOpts...)
		if err != nil {
			return nil, err
		}
		client = pb.NewChainServiceClient(conn)
		g.clients[address.String()] = client
	}

	return client, nil
}
