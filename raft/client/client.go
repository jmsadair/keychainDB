package client

import (
	"context"

	"github.com/jmsadair/keychain/internal/transport"
	pb "github.com/jmsadair/keychain/proto/raft"
	"google.golang.org/grpc"
)

var defaultCallOps = []grpc.CallOption{grpc.WaitForReady(true)}

// Client is a client of the raft service.
type Client struct {
	cache *transport.ClientCache[pb.RaftServiceClient]
}

// NewClient creates a new client.
func NewClient(dialOpts ...grpc.DialOption) (*Client, error) {
	return &Client{cache: transport.NewClientCache(pb.NewRaftServiceClient, dialOpts...)}, nil
}

// AppendEntries sends the appropriate RPC to the target node.
func (c *Client) AppendEntries(ctx context.Context, address string, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.AppendEntries(ctx, request, defaultCallOps...)
}

// AppendEntriesPipeline returns a stream that can be used to pipeline AppendEntries RPCs.
func (c *Client) AppendEntriesPipeline(ctx context.Context, address string) (pb.RaftService_AppendEntriesPipelineClient, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.AppendEntriesPipeline(ctx, defaultCallOps...)
}

// RequestVote sends the appropriate RPC to the target node.
func (c *Client) RequestVote(ctx context.Context, address string, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.RequestVote(ctx, request, defaultCallOps...)
}

// RequestPreVote sends the appropriate RPC to the target node.
func (c *Client) RequestPreVote(ctx context.Context, address string, request *pb.RequestPreVoteRequest) (*pb.RequestPreVoteResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.RequestPreVote(ctx, request, defaultCallOps...)
}

// TimeoutNow is used to start a leadership transfer to the target node.
func (c *Client) TimeoutNow(ctx context.Context, address string, request *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.TimeoutNow(ctx, request, defaultCallOps...)
}

// InstallSnapshot is used to stream a snapshot to a follower.
func (c *Client) InstallSnapshot(ctx context.Context, address string) (pb.RaftService_InstallSnapshotClient, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.InstallSnapshot(ctx, defaultCallOps...)
}

// Close closes all connections managed by this client.
func (c *Client) Close() error {
	return c.cache.Close()
}
