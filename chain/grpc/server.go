package grpc

import (
	"context"

	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/internal/transport"
	pb "github.com/jmsadair/keychain/proto/chain"
	"google.golang.org/grpc"
)

// Server is a gRPC-based server implementation for chain nodes.
type Server struct {
	pb.UnimplementedChainServiceServer
	*transport.Server
	Address string
	Node    *node.ChainNode
}

// NewServer creates a new Server instance.
func NewServer(address string, node *node.ChainNode) *Server {
	s := &Server{
		Address: address,
		Node:    node,
	}
	s.Server = transport.NewServer(address, func(grpcServer *grpc.Server) {
		pb.RegisterChainServiceServer(grpcServer, s)
	})
	return s
}

// Replicate handles incoming requests from clients to replicate a key-value pair.
func (s *Server) Replicate(ctx context.Context, request *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	resp, err := s.Node.Replicate(ctx, request)
	if err != nil {
		return nil, gRPCError(err)
	}
	return resp, nil
}

// Write handles incoming requests from other nodes in the chain to write a particular version of a key-value pair to storage.
func (s *Server) Write(ctx context.Context, request *pb.WriteRequest) (*pb.WriteResponse, error) {
	resp, err := s.Node.WriteWithVersion(ctx, request)
	if err != nil {
		return nil, gRPCError(err)
	}
	return resp, nil
}

// Read handles incoming requests from other nodes in the chain to read the committed version of a key-value pair.
func (s *Server) Read(ctx context.Context, request *pb.ReadRequest) (*pb.ReadResponse, error) {
	resp, err := s.Node.Read(ctx, request)
	if err != nil {
		return nil, gRPCError(err)
	}
	return resp, nil
}

// Commit handles incoming requests from other nodes in the chain to commit a particular version of a key.
func (s *Server) Commit(ctx context.Context, request *pb.CommitRequest) (*pb.CommitResponse, error) {
	resp, err := s.Node.Commit(ctx, request)
	if err != nil {
		return nil, gRPCError(err)
	}
	return resp, nil
}

// UpdateConfiguration handles requests from the coordinator to update the membership configuration.
func (s *Server) UpdateConfiguration(ctx context.Context, request *pb.UpdateConfigurationRequest) (*pb.UpdateConfigurationResponse, error) {
	resp, err := s.Node.UpdateConfiguration(ctx, request)
	if err != nil {
		return nil, gRPCError(err)
	}
	return resp, nil
}

// Propagate handles requests from other nodes in the chain to initiate a server-side stream of key-value pairs.
func (s *Server) Propagate(request *pb.PropagateRequest, stream pb.ChainService_PropagateServer) error {
	return gRPCError(s.Node.Propagate(stream.Context(), request, stream))
}

// Ping handles requests from the coordinator for checking if this node is alive.
func (s *Server) Ping(ctx context.Context, request *pb.PingRequest) (*pb.PingResponse, error) {
	resp := s.Node.Ping(request)
	return resp, nil
}
