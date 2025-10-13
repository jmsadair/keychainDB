package client

import (
	"context"

	"github.com/jmsadair/keychain/internal/transport"
	pb "github.com/jmsadair/keychain/proto/raft"
	"google.golang.org/grpc"
)

var defaultCallOps = []grpc.CallOption{grpc.WaitForReady(true)}

type Client struct {
	cache *transport.ClientCache[pb.RaftServiceClient]
}

func NewClient(dialOpts ...grpc.DialOption) (*Client, error) {
	return &Client{cache: transport.NewClientCache(pb.NewRaftServiceClient, dialOpts...)}, nil
}

func (c *Client) AppendEntries(ctx context.Context, address string, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.AppendEntries(ctx, request, defaultCallOps...)
}

func (c *Client) RequestVote(ctx context.Context, address string, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.RequestVote(ctx, request, defaultCallOps...)
}

func (c *Client) TimeoutNow(ctx context.Context, address string, request *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.TimeoutNow(ctx, request, defaultCallOps...)
}

func (c *Client) InstallSnapshot(ctx context.Context, address string) (grpc.ClientStreamingClient[pb.InstallSnapshotRequest, pb.InstallSnapshotResponse], error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.InstallSnapshot(ctx, defaultCallOps...)
}
