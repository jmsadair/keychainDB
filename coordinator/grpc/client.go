package grpc

import (
	"context"

	"github.com/jmsadair/keychain/coordinator/node"
	"github.com/jmsadair/keychain/internal/transport"
	pb "github.com/jmsadair/keychain/proto/coordinator"
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

// JoinCluster adds a node to the coordinator cluster.
func (c *Client) JoinCluster(
	ctx context.Context,
	address string,
	request *node.JoinClusterRequest,
	response *node.JoinClusterResponse,
) error {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return err
	}
	pbResp, err := client.JoinCluster(ctx, request.Proto())
	if err != nil {
		return err
	}
	response.FromProto(pbResp)
	return nil
}

// RemoveFromCluster removes a node from the coordinator cluster.
func (c *Client) RemoveFromCluster(
	ctx context.Context,
	address string,
	request *node.RemoveFromClusterRequest,
	response *node.RemoveFromClusterResponse,
) error {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return err
	}
	pbResp, err := client.RemoveFromCluster(ctx, request.Proto())
	if err != nil {
		return err
	}
	response.FromProto(pbResp)
	return nil
}

// AddMember adds a node to a chain.
func (c *Client) AddMember(
	ctx context.Context,
	address string,
	request *node.AddMemberRequest,
	response *node.AddMemberResponse,
) error {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return err
	}
	pbResp, err := client.AddMember(ctx, request.Proto())
	if err != nil {
		return err
	}
	response.FromProto(pbResp)
	return nil
}

// RemoveMember removes a node from a chain.
func (c *Client) RemoveMember(
	ctx context.Context,
	address string,
	request *node.RemoveMemberRequest,
	response *node.RemoveMemberResponse,
) error {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return err
	}
	pbResp, err := client.RemoveMember(ctx, request.Proto())
	if err != nil {
		return err
	}
	response.FromProto(pbResp)
	return nil
}

// ClusterStatus gets the status of the coordinator cluster.
func (c *Client) ClusterStatus(
	ctx context.Context,
	address string,
	request *node.ClusterStatusRequest,
	response *node.ClusterStatusResponse,
) error {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return err
	}
	pbResp, err := client.ClusterStatus(ctx, request.Proto())
	if err != nil {
		return err
	}
	response.FromProto(pbResp)
	return nil
}
