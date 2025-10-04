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
func (s *Server) ReadChainConfiguration(ctx context.Context, request *pb.ReadChainConfigurationRequest) (*pb.ReadChainConfigurationResponse, error) {
	return s.Node.ReadMembershipConfiguration(ctx, request)
}

// AddMember handles requests for adding a member to the chain.
func (s *Server) AddMember(ctx context.Context, request *pb.AddMemberRequest) (*pb.AddMemberResponse, error) {
	return s.Node.AddMember(ctx, request)
}

// RemoveMember handles requests for removing a member from the chain.
func (s *Server) RemoveMember(ctx context.Context, request *pb.RemoveMemberRequest) (*pb.RemoveMemberResponse, error) {
	return s.Node.RemoveMember(ctx, request)
}

// JoinCluster handles requests for joining the coordinator cluster.
func (s *Server) JoinCluster(ctx context.Context, request *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
	return s.Node.JoinCluster(ctx, request)
}

// RemoveFromCluster handles requests for removing a node from the coordinator cluster.
func (s *Server) RemoveFromCluster(ctx context.Context, request *pb.RemoveFromClusterRequest) (*pb.RemoveFromClusterResponse, error) {
	return s.Node.RemoveFromCluster(ctx, request)
}

// ClusterStatus handles requests for getting the coordinator cluster status.
func (s *Server) ClusterStatus(ctx context.Context, request *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	return s.Node.ClusterStatus(ctx, request)
}

// Check implements the gRPC health check protocol.
func (s *Server) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// Watch implements the gRPC health check protocol.
func (s *Server) Watch(req *grpc_health_v1.HealthCheckRequest, _ grpc_health_v1.Health_WatchServer) error {
	return status.Error(codes.Unimplemented, "unimplemented")
}
