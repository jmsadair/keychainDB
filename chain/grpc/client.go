package grpc

import (
	"context"

	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/chain/storage"
	"github.com/jmsadair/keychain/internal/transport"
	pb "github.com/jmsadair/keychain/proto/pbchain"
	"google.golang.org/grpc"
)

type gRPCReceiveStream struct {
	stream grpc.ClientStream
}

func (g *gRPCReceiveStream) Receive() (*storage.KeyValuePair, error) {
	var msg pb.KeyValuePair
	if err := g.stream.RecvMsg(&msg); err != nil {
		return nil, err
	}
	return &storage.KeyValuePair{
		Key:       msg.GetKey(),
		Value:     msg.GetValue(),
		Version:   msg.GetVersion(),
		Committed: msg.GetIsCommitted(),
	}, nil
}

// Client is a gRPC client for the chain service.
type Client struct {
	cache *transport.ClientCache[pb.ChainServiceClient]
}

// NewClient creates a new client.
func NewClient(dialOpts ...grpc.DialOption) (*Client, error) {
	return &Client{cache: transport.NewClientCache(pb.NewChainServiceClient, dialOpts...)}, nil
}

// Replicate chain-replicates a key-value pair. This operation can only be invoked on the head of the chain otherwise it will be rejected.
func (c *Client) Replicate(ctx context.Context, address string, request *node.ReplicateRequest, response *node.ReplicateResponse) error {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return err
	}
	pbResp, err := client.Replicate(ctx, request.Proto())
	if err != nil {
		return err
	}
	response.FromProto(pbResp)
	return nil
}

// Write writes a versioned key-value pair. This operation should only be invoked by chain nodes that are chain-replicating a key-value pair.
func (c *Client) Write(ctx context.Context, address string, request *node.WriteRequest, response *node.WriteResponse) error {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return err
	}
	pbResp, err := client.Write(ctx, request.Proto())
	if err != nil {
		return err
	}
	response.FromProto(pbResp)
	return nil
}

// Read reads a key-value pair.
func (c *Client) Read(ctx context.Context, address string, request *node.ReadRequest, response *node.ReadResponse) error {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return err
	}
	pbResp, err := client.Read(ctx, request.Proto())
	if err != nil {
		return err
	}
	response.FromProto(pbResp)
	return nil
}

// Commit commits a versioned key-value pair.
func (c *Client) Commit(ctx context.Context, address string, request *node.CommitRequest, response *node.CommitResponse) error {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return err
	}
	pbResp, err := client.Commit(ctx, request.Proto())
	if err != nil {
		return err
	}
	response.FromProto(pbResp)
	return nil
}

// Propagate initiates a stream of key-value pairs.
func (c *Client) Propagate(ctx context.Context, address string, request *node.PropagateRequest) (node.KeyValueReceiveStream, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}

	stream, err := client.Propagate(ctx, request.Proto())
	if err != nil {
		return nil, err
	}
	return &gRPCReceiveStream{stream: stream}, nil
}

// Ping pings a node. Nodes will respond with their current status and configuration version.
func (c *Client) Ping(ctx context.Context, address string, request *node.PingRequest, response *node.PingResponse) error {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return err
	}
	pbResp, err := client.Ping(ctx, request.Proto())
	if err != nil {
		return err
	}
	response.FromProto(pbResp)
	return nil
}

// UpdateConfiguration will update the chain membership configuration of a node.
func (c *Client) UpdateConfiguration(
	ctx context.Context,
	address string,
	request *node.UpdateConfigurationRequest,
	response *node.UpdateConfigurationResponse,
) error {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return err
	}
	pbResp, err := client.UpdateConfiguration(ctx, request.Proto())
	if err != nil {
		return err
	}
	response.FromProto(pbResp)
	return nil
}
