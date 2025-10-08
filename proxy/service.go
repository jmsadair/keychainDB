package proxy

import (
	"context"
	"log/slog"

	chainclient "github.com/jmsadair/keychain/chain/client"
	coordinatorclient "github.com/jmsadair/keychain/coordinator/client"
	"github.com/jmsadair/keychain/proxy/node"
	"github.com/jmsadair/keychain/proxy/server"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// ServiceConfig contains the configurations for a proxy service.
type ServiceConfig struct {
	// Addresses of the coordinators that the proxy is able make requests to.
	Coordinators []string
	// Address that the service will listen for incoming HTTP requests on.
	HTTPListen string
	// Address that the service will listen for incoming RPCs on.
	GRPCListen string
	// gRPC Dial options a service will use when making RPCs to other services.
	DialOptions []grpc.DialOption
	// Logger that the service will use for logging.
	Log *slog.Logger
}

// Service is the proxy service.
type Service struct {
	// The proxy HTTP server.
	HTTPServer *server.HTTPServer
	// The proxy RPC service.
	GRPCServer *server.RPCServer
	// The proxy implementation.
	Proxy *node.Proxy
	// The configuration for this service.
	Config ServiceConfig
}

// NewService creates a new proxy service.
func NewService(cfg ServiceConfig) (*Service, error) {
	chainTn, err := chainclient.NewClient(cfg.DialOptions...)
	if err != nil {
		return nil, err
	}
	coordinatorTn, err := coordinatorclient.NewClient(cfg.DialOptions...)
	if err != nil {
		return nil, err
	}
	p := node.NewProxy(cfg.Coordinators, coordinatorTn, chainTn, cfg.Log)
	gRPCServer := server.NewServer(cfg.GRPCListen, p)
	httpServer := &server.HTTPServer{Address: cfg.HTTPListen, GRPCAddress: cfg.GRPCListen, DialOptions: cfg.DialOptions}
	return &Service{HTTPServer: httpServer, GRPCServer: gRPCServer, Proxy: p, Config: cfg}, nil
}

// Run runs the service.
func (s *Service) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.GRPCServer.Run(ctx)
	})
	g.Go(func() error {
		return s.HTTPServer.Run(ctx)
	})
	s.Config.Log.InfoContext(
		ctx,
		"running proxy service",
		"http-listen",
		s.Config.HTTPListen,
		"grpc-listen",
		s.Config.GRPCListen,
	)
	return g.Wait()
}
