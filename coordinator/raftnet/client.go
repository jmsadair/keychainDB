package raftnet

import (
	"context"

	"github.com/jmsadair/keychainDB/internal/transport"
	pb "github.com/jmsadair/keychainDB/proto/raft"
	"google.golang.org/grpc"
)

var defaultCallOps = []grpc.CallOption{grpc.WaitForReady(true)}

type client struct {
	cache *transport.ClientCache[pb.RaftServiceClient]
}

func newClient(dialOpts ...grpc.DialOption) (*client, error) {
	return &client{cache: transport.NewClientCache(pb.NewRaftServiceClient, dialOpts...)}, nil
}

func (c *client) AppendEntries(ctx context.Context, address string, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.AppendEntries(ctx, request, defaultCallOps...)
}

func (c *client) AppendEntriesPipeline(ctx context.Context, address string) (pb.RaftService_AppendEntriesPipelineClient, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.AppendEntriesPipeline(ctx, defaultCallOps...)
}

func (c *client) RequestVote(ctx context.Context, address string, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.RequestVote(ctx, request, defaultCallOps...)
}

func (c *client) RequestPreVote(ctx context.Context, address string, request *pb.RequestPreVoteRequest) (*pb.RequestPreVoteResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.RequestPreVote(ctx, request, defaultCallOps...)
}

func (c *client) TimeoutNow(ctx context.Context, address string, request *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.TimeoutNow(ctx, request, defaultCallOps...)
}

func (c *client) InstallSnapshot(ctx context.Context, address string) (pb.RaftService_InstallSnapshotClient, error) {
	client, err := c.cache.GetOrCreate(address)
	if err != nil {
		return nil, err
	}
	return client.InstallSnapshot(ctx, defaultCallOps...)
}

func (c *client) Close() error {
	return c.cache.Close()
}
