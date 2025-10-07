package client

import (
	"context"

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

// GetMembers reads the chain configuration.
func (c *Client) GetMembers(
	ctx context.Context,
	address string,
	request *pb.GetMembersRequest,
) (*pb.GetMembersResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.GetMembers(ctx, request)
}

// JoinCluster adds a node to the coordinator cluster.
func (c *Client) JoinCluster(
	ctx context.Context,
	address string,
	request *pb.JoinClusterRequest,
) (*pb.JoinClusterResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.JoinCluster(ctx, request)
}

// RemoveFromCluster removes a node from the coordinator cluster.
func (c *Client) RemoveFromCluster(
	ctx context.Context,
	address string,
	request *pb.RemoveFromClusterRequest,
) (*pb.RemoveFromClusterResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.RemoveFromCluster(ctx, request)
}

// AddMember adds a node to a chain.
func (c *Client) AddMember(
	ctx context.Context,
	address string,
	request *pb.AddMemberRequest,
) (*pb.AddMemberResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.AddMember(ctx, request)
}

// RemoveMember removes a node from a chain.
func (c *Client) RemoveMember(
	ctx context.Context,
	address string,
	request *pb.RemoveMemberRequest,
) (*pb.RemoveMemberResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.RemoveMember(ctx, request)
}

// ClusterStatus gets the status of the coordinator cluster.
func (c *Client) ClusterStatus(
	ctx context.Context,
	address string,
	request *pb.ClusterStatusRequest,
) (*pb.ClusterStatusResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.ClusterStatus(ctx, request)
}
