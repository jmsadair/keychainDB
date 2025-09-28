package grpc

import (
	"context"

	"github.com/jmsadair/keychain/coordinator/node"
	"github.com/jmsadair/keychain/internal/transport"
	pb "github.com/jmsadair/keychain/proto/pbcoordinator"
	"google.golang.org/grpc"
)

// Client is a gRPC client for the coordinator service.
type Client struct {
	cache *transport.ClientCache[pb.CoordinatorServiceClient]
}

// NewClient creates a new client.
func NewClient(dialOpts ...grpc.DialOption) (*Client, error) {
	return &Client{cache: transport.NewClientCache(pb.NewCoordinatorServiceClient, dialOpts...)}, nil
}

// ReadChainConfiguration reads the chain configuration.
func (c *Client) ReadChainConfiguration(
	ctx context.Context,
	address string,
	request *node.ReadChainConfigurationRequest,
	response *node.ReadChainConfigurationResponse,
) error {
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
