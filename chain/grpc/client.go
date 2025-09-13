package grpc

import (
	"context"
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

func (c *Client) Write(ctx context.Context, address string, request *node.WriteRequest, response *node.WriteResponse) error {
	client, err := c.getOrCreateClient(address)
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

func (c *Client) Read(ctx context.Context, address string, request *node.ReadRequest, response *node.ReadResponse) error {
	client, err := c.getOrCreateClient(address)
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

func (c *Client) Commit(ctx context.Context, address string, request *node.CommitRequest, response *node.CommitResponse) error {
	client, err := c.getOrCreateClient(address)
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

func (c *Client) Propagate(ctx context.Context, address string, request *node.PropagateRequest) (node.KeyValueReceiveStream, error) {
	client, err := c.getOrCreateClient(address)
	if err != nil {
		return nil, err
	}

	stream, err := client.Propagate(ctx, request.Proto())
	if err != nil {
		return nil, err
	}
	return &gRPCReceiveStream{stream: stream}, nil
}

func (c *Client) Ping(ctx context.Context, address string, request *node.PingRequest, response *node.PingResponse) error {
	client, err := c.getOrCreateClient(address)
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

func (c *Client) UpdateConfiguration(ctx context.Context, address string, request *node.UpdateConfigurationRequest, response *node.UpdateConfigurationResponse) error {
	client, err := c.getOrCreateClient(address)
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

func (c *Client) getOrCreateClient(address string) (pb.ChainServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	client, ok := c.clients[address]
	if !ok {
		conn, err := grpc.NewClient(address, c.dialOpts...)
		if err != nil {
			return nil, err
		}
		client = pb.NewChainServiceClient(conn)
		c.clients[address] = client
	}

	return client, nil
}
