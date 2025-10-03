package grpc

import (
	"context"

	"github.com/jmsadair/keychain/internal/transport"
	pb "github.com/jmsadair/keychain/proto/proxy"
	"github.com/jmsadair/keychain/proxy/node"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// Server is the gRPC proxy service.
type Server struct {
	pb.ProxyServiceServer
	*transport.Server
	Address string
	Node    *node.Proxy
}

// NewServer creates a new server.
func NewServer(address string, node *node.Proxy) *Server {
	s := &Server{
		Address: address,
		Node:    node,
	}
	s.Server = transport.NewServer(address, func(grpcServer *grpc.Server) {
		pb.RegisterProxyServiceServer(grpcServer, s)
		grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
	})
	return s
}

// Get handles requests for reading key-value pairs.
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, err := s.Node.GetValue(ctx, req.GetKey())
	if err != nil {
		return nil, err
	}
	return &pb.GetResponse{Value: value}, nil
}

// Set handles requests for setting key-value pairs.
func (s *Server) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	err := s.Node.SetValue(ctx, req.GetKey(), req.GetValue())
	if err != nil {
		return nil, err
	}
	return &pb.SetResponse{}, nil
}

// Check implements the gRPC health check protocol.
func (s *Server) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// Watch implements the gRPC health check protocol.
func (s *Server) Watch(req *grpc_health_v1.HealthCheckRequest, _ grpc_health_v1.Health_WatchServer) error {
	return status.Error(codes.Unimplemented, "unimplemented")
}
