package server

import (
	"context"

	"github.com/jmsadair/keychain/internal/transport"
	pb "github.com/jmsadair/keychain/proto/proxy"
	proxynode "github.com/jmsadair/keychain/proxy/node"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// RPCServer is the gRPC proxy server.
type RPCServer struct {
	pb.ProxyServiceServer
	*transport.Server
	Address string
	Proxy   *proxynode.Proxy
}

// NewServer creates a new server.
func NewServer(address string, p *proxynode.Proxy) *RPCServer {
	s := &RPCServer{Address: address, Proxy: p}
	s.Server = transport.NewServer(address, func(grpcServer *grpc.Server) {
		pb.RegisterProxyServiceServer(grpcServer, s)
		grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
	})
	return s
}

// Get handles requests for reading key-value pairs.
func (s *RPCServer) Get(ctx context.Context, request *pb.GetRequest) (*pb.GetResponse, error) {
	return s.Proxy.Get(ctx, request)
}

// Set handles requests for setting key-value pairs.
func (s *RPCServer) Set(ctx context.Context, request *pb.SetRequest) (*pb.SetResponse, error) {
	return s.Proxy.Set(ctx, request)
}

// Check implements the gRPC health check protocol.
func (s *RPCServer) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// Watch implements the gRPC health check protocol.
func (s *RPCServer) Watch(req *grpc_health_v1.HealthCheckRequest, _ grpc_health_v1.Health_WatchServer) error {
	return status.Error(codes.Unimplemented, "unimplemented")
}
