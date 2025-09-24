package grpc

import (
	"context"

	"github.com/jmsadair/keychain/coordinator/node"
	pb "github.com/jmsadair/keychain/proto/pbcoordinator"
	"github.com/jmsadair/keychain/transport"
	"google.golang.org/grpc"
)

// Client is a grpc-based transport used to communicate with coordinator nodes.
type Client struct {
	cache *transport.ClientCache[pb.CoordinatorServiceClient]
}

// NewClient creates a new gRPC-based chain client with the provided dial options.
func NewClient(dialOpts ...grpc.DialOption) (*Client, error) {
	return &Client{cache: transport.NewClientCache(pb.NewCoordinatorServiceClient, dialOpts...)}, nil
}

func (c *Client) ReadChainConfiguration(ctx context.Context, address string, request *node.ReadChainConfigurationRequest, response *node.ReadChainConfigurationResponse) error {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return err
	}
	pbResp, err := client.ReadChainConfiguration(ctx, request.Proto())
	if err != nil {
		return err
	}
	response.FromProto(pbResp)
	return nil
}
