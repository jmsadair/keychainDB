package server

import (
	"context"

	"github.com/jmsadair/keychain/internal/transport"
	pb "github.com/jmsadair/keychain/proto/raft"
	"github.com/jmsadair/keychain/raft/node"
	"google.golang.org/grpc"
)

// RPCServer is the gRPC raft server.
type RPCServer struct {
	pb.RaftServiceServer
	*transport.Server
	Address string
	Node    *node.Raft
}

// NewServer creates a new server.
func NewServer(address string, node *node.Raft) *RPCServer {
	s := &RPCServer{Address: address, Node: node}
	s.Server = transport.NewServer(address, func(grpcServer *grpc.Server) {
		pb.RegisterRaftServiceServer(grpcServer, s)
	})
	return s
}

func (s *RPCServer) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return s.Node.AppendEntries(ctx, request)
}

func (s *RPCServer) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	return s.Node.RequestVote(ctx, request)
}

func (s *RPCServer) TimeoutNow(ctx context.Context, request *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	return s.Node.TimeoutNow(ctx, request)
}

func (s *RPCServer) InstallSnapshot(stream pb.RaftService_InstallSnapshotServer) error {
	return s.Node.InstallSnapshot(stream)
}
