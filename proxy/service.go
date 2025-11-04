package proxy

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jmsadair/keychainDB/api/types"
	"github.com/jmsadair/keychainDB/chain"
	"github.com/jmsadair/keychainDB/coordinator"
	"github.com/jmsadair/keychainDB/internal/transport"
	proxypb "github.com/jmsadair/keychainDB/proto/proxy"
	"github.com/jmsadair/keychainDB/proxy/node"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// toGRPCError maps a service error to its gRPC equivalent.
// If no equivalent exists, this function will return nil.
func toGRPCError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, node.ErrCoordinatorUnavailable):
		return types.ErrGRPCCoordinatorUnavailable
	case errors.Is(err, node.ErrNoMembers):
		return types.ErrGRPCNoMembers
	}

	// No known gRPC equivalent.
	return nil
}

// ServiceConfig contains the configurations for a proxy service.
type ServiceConfig struct {
	// Addresses of the coordinators that the proxy is able to make requests to.
	Coordinators []string
	// Address that the service will listen for incoming HTTP requests on.
	HTTPListen string
	// Address that the service will listen for incoming RPCs on.
	Listen string
	// gRPC Dial options a service will use when making RPCs to other services.
	DialOptions []grpc.DialOption
	// Logger that the service will use for logging.
	Log *slog.Logger
}

// Service is the proxy service.
type Service struct {
	// HTTP gateway for the service.
	Gateway *transport.HTTPGateway
	// The proxy RPC service.
	Server *transport.Server
	// The proxy implementation.
	Proxy *node.Proxy
	// The configuration for this service.
	Config ServiceConfig
}

// NewService creates a new proxy service.
func NewService(cfg ServiceConfig) (*Service, error) {
	chainTn, err := chain.NewClient(cfg.DialOptions...)
	if err != nil {
		return nil, err
	}
	coordinatorTn, err := coordinator.NewClient(cfg.DialOptions...)
	if err != nil {
		return nil, err
	}

	p := node.NewProxy(cfg.Coordinators, coordinatorTn, chainTn, cfg.Log)
	srv := transport.NewServer(cfg.Listen, func(grpcServer *grpc.Server) {
		proxypb.RegisterProxyServiceServer(grpcServer, p)
		grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
	}, grpc.ChainUnaryInterceptor(transport.UnaryServerErrorInterceptor(toGRPCError)))
	gw := transport.NewHTTPGateway(cfg.HTTPListen, cfg.Listen, proxypb.RegisterProxyServiceHandlerFromEndpoint, cfg.DialOptions...)
	return &Service{Gateway: gw, Server: srv, Proxy: p, Config: cfg}, nil
}

// Run runs the service.
func (s *Service) Run(ctx context.Context) error {
	defer s.Proxy.Shutdown()

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.Server.Run(ctx)
	})
	g.Go(func() error {
		return s.Gateway.Run(ctx)
	})
	s.Config.Log.InfoContext(
		ctx,
		"running proxy service",
		"http-listen",
		s.Config.HTTPListen,
		"listen",
		s.Config.Listen,
	)
	return g.Wait()
}
