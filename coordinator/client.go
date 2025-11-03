package coordinator

import (
	"context"

	"github.com/jmsadair/keychain/internal/transport"
	pb "github.com/jmsadair/keychain/proto/coordinator"
	"google.golang.org/grpc"
)

var defaultCallOps = []grpc.CallOption{grpc.WaitForReady(true)}

// Client is a gRPC client for the coordinator service.
type Client struct {
	cache *transport.ClientCache[pb.CoordinatorServiceClient]
}

// NewClient creates a new client that can communicate with the coordinator.
func NewClient(dialOpts ...grpc.DialOption) (*Client, error) {
	return &Client{cache: transport.NewClientCache(pb.NewCoordinatorServiceClient, dialOpts...)}, nil
}

// GetMembers is used to list the members of a chain.
func (c *Client) GetMembers(
	ctx context.Context,
	address string,
	request *pb.GetMembersRequest,
) (*pb.GetMembersResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.GetMembers(ctx, request, defaultCallOps...)
}

// JoinCluster is used to add a coordinator to the cluster. The node must be reachable at the address provided in
// the request and must be ready to start serving RPCs immediately. There must not be a node with the same address
// or the same ID that is already a member of the cluster.
func (c *Client) JoinCluster(
	ctx context.Context,
	address string,
	request *pb.JoinClusterRequest,
) (*pb.JoinClusterResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.JoinCluster(ctx, request, defaultCallOps...)
}

// RemoveFromCluster is used to remove a coordinator from the cluster.
func (c *Client) RemoveFromCluster(
	ctx context.Context,
	address string,
	request *pb.RemoveFromClusterRequest,
) (*pb.RemoveFromClusterResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.RemoveFromCluster(ctx, request, defaultCallOps...)
}

// AddMember is used to add a chain node to a chain. The chain node must be reachable at the address provicded in
// the request and must be ready to start serving RPCs immediately. It is possible that the configuration with the
// new member is successfully replicated across the coordinator cluster but the update is not successfully applied
// to all nodes in the chain. An error will be returned in this case, but the coordinator will continue to attempt
// to apply the configuration update until all nodes have it.
func (c *Client) AddMember(
	ctx context.Context,
	address string,
	request *pb.AddMemberRequest,
) (*pb.AddMemberResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.AddMember(ctx, request, defaultCallOps...)
}

// RemoveMember is used to remove a chain node from a chain. It is possible that the configuration with the
// member removed is successfully replicated across the coordinator cluster but the update is not successfully applied
// to all nodes in the chain. An error will be returned in this case, but the coordinator will continue to attempt
// to apply the configuration update until all nodes have it.
func (c *Client) RemoveMember(
	ctx context.Context,
	address string,
	request *pb.RemoveMemberRequest,
) (*pb.RemoveMemberResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.RemoveMember(ctx, request, defaultCallOps...)
}

// ClusterStatus is used to get the current members of the cluster and the leader.
func (c *Client) ClusterStatus(
	ctx context.Context,
	address string,
	request *pb.ClusterStatusRequest,
) (*pb.ClusterStatusResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.ClusterStatus(ctx, request, defaultCallOps...)
}

// Close closes all connections that are managed by this client.
func (c *Client) Close() error {
	return c.cache.Close()
}
