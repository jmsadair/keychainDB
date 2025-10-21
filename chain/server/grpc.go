package server

import (
	"context"

	"github.com/jmsadair/keychain/api/types"
	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/chain/storage"
	"github.com/jmsadair/keychain/internal/transport"
	pb "github.com/jmsadair/keychain/proto/chain"
	"google.golang.org/grpc"
)

var errToGRPCError = map[error]error{
	storage.ErrConflict:          types.ErrGRPCConflict,
	storage.ErrEmptyKey:          types.ErrGRPCEmptyKey,
	storage.ErrUncommittedRead:   types.ErrGRPCUncommittedRead,
	storage.ErrKeyNotFound:       types.ErrGRPCKeyNotFound,
	node.ErrInvalidConfigVersion: types.ErrGRPCInvalidConfigVersion,
	node.ErrNotHead:              types.ErrGRPCNotHead,
	node.ErrSyncing:              types.ErrGRPCSyncing,
	node.ErrInvalidConfigVersion: types.ErrGRPCInvalidConfigVersion,
}

// RPCServer is a gRPC-based server implementation for chain nodes.
type RPCServer struct {
	pb.UnimplementedChainServiceServer
	*transport.Server
	Address string
	Node    *node.ChainNode
}

// NewServer creates a new server.
func NewServer(address string, node *node.ChainNode) *RPCServer {
	s := &RPCServer{
		Address: address,
		Node:    node,
	}
	s.Server = transport.NewServer(
		address,
		func(grpcServer *grpc.Server) { pb.RegisterChainServiceServer(grpcServer, s) },
		grpc.UnaryInterceptor(transport.UnaryServerErrorInterceptor(errToGRPCError)),
		grpc.StreamInterceptor(transport.StreamServerErrorInterceptor(errToGRPCError)),
	)
	return s
}

// Replicate handles incoming requests from clients to replicate a key-value pair.
func (s *RPCServer) Replicate(ctx context.Context, request *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	resp, err := s.Node.Replicate(ctx, request)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Write handles incoming requests from other nodes in the chain to write a particular version of a key-value pair to storage.
func (s *RPCServer) Write(ctx context.Context, request *pb.WriteRequest) (*pb.WriteResponse, error) {
	resp, err := s.Node.WriteWithVersion(ctx, request)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Read handles incoming requests from other nodes in the chain to read the committed version of a key-value pair.
func (s *RPCServer) Read(ctx context.Context, request *pb.ReadRequest) (*pb.ReadResponse, error) {
	resp, err := s.Node.Read(ctx, request)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Commit handles incoming requests from other nodes in the chain to commit a particular version of a key.
func (s *RPCServer) Commit(ctx context.Context, request *pb.CommitRequest) (*pb.CommitResponse, error) {
	resp, err := s.Node.Commit(ctx, request)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateConfiguration handles requests from the coordinator to update the membership configuration.
func (s *RPCServer) UpdateConfiguration(ctx context.Context, request *pb.UpdateConfigurationRequest) (*pb.UpdateConfigurationResponse, error) {
	resp, err := s.Node.UpdateConfiguration(ctx, request)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Propagate handles requests from other nodes in the chain to initiate a server-side stream of key-value pairs.
func (s *RPCServer) Propagate(request *pb.PropagateRequest, stream pb.ChainService_PropagateServer) error {
	return s.Node.Propagate(stream.Context(), request, stream)
}

// Ping handles requests from the coordinator for checking if this node is alive.
func (s *RPCServer) Ping(ctx context.Context, request *pb.PingRequest) (*pb.PingResponse, error) {
	resp := s.Node.Ping(request)
	return resp, nil
}
