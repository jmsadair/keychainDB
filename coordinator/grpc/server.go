package grpc

import (
	"context"

	"github.com/jmsadair/keychain/coordinator/node"
	"github.com/jmsadair/keychain/internal/transport"
	pb "github.com/jmsadair/keychain/proto/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
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
		grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
	})
	return s
}

// ReadChainConfiguration handles requests for reading the chain configuration.
func (s *Server) ReadChainConfiguration(ctx context.Context, pbRequest *pb.ReadChainConfigurationRequest) (*pb.ReadChainConfigurationResponse, error) {
	req := &node.ReadChainConfigurationRequest{}
	var resp node.ReadChainConfigurationResponse
	err := s.Node.ReadMembershipConfiguration(ctx, req, &resp)
	if err != nil {
		return nil, err
	}
	return &pb.ReadChainConfigurationResponse{Configuration: resp.Configuration.Proto()}, nil
}

// AddMember handles requests for adding a member to the chain.
func (s *Server) AddMember(ctx context.Context, pbRequest *pb.AddMemberRequest) (*pb.AddMemberResponse, error) {
	req := &node.AddMemberRequest{}
	req.FromProto(pbRequest)
	var resp node.AddMemberResponse
	err := s.Node.AddMember(ctx, req, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Proto(), nil
}

// RemoveMember handles requests for removing a member from the chain.
func (s *Server) RemoveMember(ctx context.Context, pbRequest *pb.RemoveMemberRequest) (*pb.RemoveMemberResponse, error) {
	req := &node.RemoveMemberRequest{}
	req.FromProto(pbRequest)
	var resp node.RemoveMemberResponse
	err := s.Node.RemoveMember(ctx, req, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Proto(), nil
}

// JoinCluster handles requests for joining the coordinator cluster.
func (s *Server) JoinCluster(ctx context.Context, pbRequest *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
	req := &node.JoinClusterRequest{}
	req.FromProto(pbRequest)
	var resp node.JoinClusterResponse
	err := s.Node.JoinCluster(ctx, req, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Proto(), nil
}

// RemoveFromCluster handles requests for removing a node from the coordinator cluster.
func (s *Server) RemoveFromCluster(ctx context.Context, pbRequest *pb.RemoveFromClusterRequest) (*pb.RemoveFromClusterResponse, error) {
	req := &node.RemoveFromClusterRequest{}
	req.FromProto(pbRequest)
	var resp node.RemoveFromClusterResponse
	err := s.Node.RemoveFromCluster(ctx, req, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Proto(), nil
}

// ClusterStatus handles requests for getting the coordinator cluster status.
func (s *Server) ClusterStatus(ctx context.Context, pbRequest *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	req := &node.ClusterStatusRequest{}
	req.FromProto(pbRequest)
	var resp node.ClusterStatusResponse
	err := s.Node.ClusterStatus(ctx, req, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Proto(), nil
}

// Check implements the gRPC health check protocol.
func (s *Server) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// Watch implements the gRPC health check protocol.
func (s *Server) Watch(req *grpc_health_v1.HealthCheckRequest, _ grpc_health_v1.Health_WatchServer) error {
	return status.Error(codes.Unimplemented, "unimplemented")
}
