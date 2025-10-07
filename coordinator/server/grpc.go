package server

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

// RPCServer is the gRPC coordinator server.
type RPCServer struct {
	pb.CoordinatorServiceServer
	*transport.Server
	Address string
	Node    *node.Coordinator
}

// NewServer creates a new server.
func NewServer(address string, node *node.Coordinator) *RPCServer {
	s := &RPCServer{
		Address: address,
		Node:    node,
	}
	s.Server = transport.NewServer(address, func(grpcServer *grpc.Server) {
		pb.RegisterCoordinatorServiceServer(grpcServer, s)
		grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
	})
	return s
}

// GetMembers handles requests for reading the chain configuration.
func (s *RPCServer) GetMembers(ctx context.Context, request *pb.GetMembersRequest) (*pb.GetMembersResponse, error) {
	return s.Node.GetMembers(ctx, request)
}

// AddMember handles requests for adding a member to the chain.
func (s *RPCServer) AddMember(ctx context.Context, request *pb.AddMemberRequest) (*pb.AddMemberResponse, error) {
	return s.Node.AddMember(ctx, request)
}

// RemoveMember handles requests for removing a member from the chain.
func (s *RPCServer) RemoveMember(ctx context.Context, request *pb.RemoveMemberRequest) (*pb.RemoveMemberResponse, error) {
	return s.Node.RemoveMember(ctx, request)
}

// JoinCluster handles requests for joining the coordinator cluster.
func (s *RPCServer) JoinCluster(ctx context.Context, request *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
	return s.Node.JoinCluster(ctx, request)
}

// RemoveFromCluster handles requests for removing a node from the coordinator cluster.
func (s *RPCServer) RemoveFromCluster(ctx context.Context, request *pb.RemoveFromClusterRequest) (*pb.RemoveFromClusterResponse, error) {
	return s.Node.RemoveFromCluster(ctx, request)
}

// ClusterStatus handles requests for getting the coordinator cluster status.
func (s *RPCServer) ClusterStatus(ctx context.Context, request *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	return s.Node.ClusterStatus(ctx, request)
}

// Check implements the gRPC health check protocol.
func (s *RPCServer) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// Watch implements the gRPC health check protocol.
func (s *RPCServer) Watch(req *grpc_health_v1.HealthCheckRequest, _ grpc_health_v1.Health_WatchServer) error {
	return status.Error(codes.Unimplemented, "unimplemented")
}
