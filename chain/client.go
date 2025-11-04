package chain

import (
	"context"

	"github.com/jmsadair/keychain/internal/transport"
	pb "github.com/jmsadair/keychain/proto/chain"
	storagepb "github.com/jmsadair/keychain/proto/storage"
	"google.golang.org/grpc"
)

var defaultCallOps = []grpc.CallOption{grpc.WaitForReady(true)}

// Client is a gRPC client for the chain service.
type Client struct {
	cache *transport.ClientCache[pb.ChainServiceClient]
}

// NewClient creates a new client that can be used to communicate with chain nodes.
func NewClient(dialOpts ...grpc.DialOption) (*Client, error) {
	return &Client{cache: transport.NewClientCache(pb.NewChainServiceClient, dialOpts...)}, nil
}

// Replicate chain-replicates a key-value pair. This operation can only be invoked on the head of the chain otherwise it will be rejected.
func (c *Client) Replicate(ctx context.Context, address string, request *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	resp, err := client.Replicate(ctx, request, defaultCallOps...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Write writes a versioned key-value pair to storage. The receiving node will forward the request to its
// successor if it has one. If this node is the tail of the chain, it will immediately commit the
// key-value pair. This should only be invoked by other chain nodes
func (c *Client) Write(ctx context.Context, address string, request *pb.WriteRequest) (*pb.WriteResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	resp, err := client.Write(ctx, request, defaultCallOps...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Read is used to read a key-value pair. The read will be performed locally on the node if the key-value pair
// has been committed. If the key-value pair has not been committed, the node will attempt to forward
// the request to the tail of the chain.
func (c *Client) Read(ctx context.Context, address string, request *pb.ReadRequest) (*pb.ReadResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	resp, err := client.Read(ctx, request, defaultCallOps...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Commit commits a previously written version of a key-value and makes it visible to client reads.
// This should only be invoked by chain nodes.
func (c *Client) Commit(ctx context.Context, address string, request *pb.CommitRequest) (*pb.CommitResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	resp, err := client.Commit(ctx, request, defaultCallOps...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Propagate is used to initiate a stream of key-value pairs from the target node. This is useful when
// a new node has been added to the chain and needs to be caught up. This should only be invoked by chain
// nodes.
func (c *Client) Propagate(ctx context.Context, address string, request *pb.PropagateRequest) (grpc.ServerStreamingClient[storagepb.KeyValuePair], error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	stream, err := client.Propagate(ctx, request, defaultCallOps...)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

// Ping is used to check if a node is alive.
func (c *Client) Ping(ctx context.Context, address string, request *pb.PingRequest) (*pb.PingResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	resp, err := client.Ping(ctx, request, defaultCallOps...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateConfiguration is used to update the membership configuration of this node.
// This should only be invoked by the coordinator.
func (c *Client) UpdateConfiguration(
	ctx context.Context,
	address string,
	request *pb.UpdateConfigurationRequest,
) (*pb.UpdateConfigurationResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	resp, err := client.UpdateConfiguration(ctx, request, defaultCallOps...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Close closes all connections managed by this client.
func (c *Client) Close() error {
	return c.cache.Close()
}
