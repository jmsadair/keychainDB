package grpc

import (
	"context"

	"github.com/jmsadair/keychain/coordinator/node"
	"github.com/jmsadair/keychain/internal/transport"
	pb "github.com/jmsadair/keychain/proto/pbcoordinator"
	"google.golang.org/grpc"
)

// Server is the gRPC coordinator service.
type Server struct {
	pb.CoordinatorServiceServer
	*transport.Server
	Address string
	Node    *node.Coordinator
}

// NewServer creates a new server.
func NewServer(address string, node *node.Coordinator) *Server {
	s := &Server{
		Address: address,
		Node:    node,
	}
	s.Server = transport.NewServer(address, func(grpcServer *grpc.Server) {
		pb.RegisterCoordinatorServiceServer(grpcServer, s)
	})
	return s
}

// ReadChainConfiguration handles requests for reading the chain configuration.
func (s *Server) ReadChainConfiguration(ctx context.Context, pbRequest *pb.ReadChainConfigurationRequest) (*pb.ReadChainConfigurationResponse, error) {
	config, err := s.Node.ReadMembershipConfiguration(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.ReadChainConfigurationResponse{Configuration: config.Proto()}, nil
}
