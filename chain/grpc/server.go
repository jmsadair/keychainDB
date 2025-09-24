package grpc

import (
	"context"
	"net"

	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/chain/storage"
	pb "github.com/jmsadair/keychain/proto/pbchain"
	"github.com/jmsadair/keychain/transport"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type gRPCSendStream struct {
	stream grpc.ServerStream
}

func (g *gRPCSendStream) Send(kv *storage.KeyValuePair) error {
	msg := &pb.KeyValuePair{Key: kv.Key, Value: kv.Value, Version: kv.Version, IsCommitted: kv.Committed}
	return g.stream.SendMsg(msg)
}

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
func (s *Server) Replicate(ctx context.Context, pbRequest *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	var request node.ReplicateRequest
	request.FromProto(pbRequest)
	var response node.ReplicateResponse
	if err := s.Node.Replicate(ctx, &request, &response); err != nil {
		return nil, err
	}
	return response.Proto(), nil
}

// Write handles incoming requests from other nodes in the chain to write a particular version of a key-value pair to storage.
func (s *Server) Write(ctx context.Context, pbRequest *pb.WriteRequest) (*pb.WriteResponse, error) {
	var request node.WriteRequest
	request.FromProto(pbRequest)
	var response node.WriteResponse
	if err := s.Node.WriteWithVersion(ctx, &request, &response); err != nil {
		return nil, err
	}
	return response.Proto(), nil
}

// Read handles incoming requests from other nodes in the chain to read the committed version of a key-value pair.
func (s *Server) Read(ctx context.Context, pbRequest *pb.ReadRequest) (*pb.ReadResponse, error) {
	var request node.ReadRequest
	request.FromProto(pbRequest)
	var response node.ReadResponse
	if err := s.Node.Read(ctx, &request, &response); err != nil {
		return nil, err
	}
	return response.Proto(), nil
}

// Commit handles incoming requests from other nodes in the chain to commit a particular version of a key.
func (s *Server) Commit(ctx context.Context, pbRequest *pb.CommitRequest) (*pb.CommitResponse, error) {
	var request node.CommitRequest
	request.FromProto(pbRequest)
	var response node.CommitResponse
	if err := s.Node.Commit(ctx, &request, &response); err != nil {
		return nil, err
	}
	return response.Proto(), nil
}

// UpdateConfiguration handles requests from the coordinator to update the membership configuration.
func (s *Server) UpdateConfiguration(ctx context.Context, pbRequest *pb.UpdateConfigurationRequest) (*pb.UpdateConfigurationResponse, error) {
	var request node.UpdateConfigurationRequest
	request.FromProto(pbRequest)
	var response node.UpdateConfigurationResponse
	if err := s.Node.UpdateConfiguration(ctx, &request, &response); err != nil {
		return nil, err
	}
	return response.Proto(), nil
}

// Propagate handles requests from other nodes in the chain to initiate a server-side stream of key-value pairs.
func (s *Server) Propagate(pbRequest *pb.PropagateRequest, stream pb.ChainService_PropagateServer) error {
	var request node.PropagateRequest
	request.FromProto(pbRequest)
	return s.Node.Propagate(stream.Context(), &request, &gRPCSendStream{stream: stream})
}

// Ping handles requests from the coordinator for checking if this node is alive.
func (s *Server) Ping(ctx context.Context, pbRequest *pb.PingRequest) (*pb.PingResponse, error) {
	var request node.PingRequest
	request.FromProto(pbRequest)
	var response node.PingResponse
	s.Node.Ping(&request, &response)
	return response.Proto(), nil
}
