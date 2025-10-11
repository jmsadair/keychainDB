package client

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

// NewClient creates a new client.
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

// Write writes a versioned key-value pair. This operation should only be invoked by chain nodes that are chain-replicating a key-value pair.
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

// Read reads a key-value pair.
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

// Commit commits a versioned key-value pair.
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

// Propagate initiates a stream of key-value pairs.
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

// Ping will ping a node.
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

// UpdateConfiguration will update the chain configuration for a node.
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
