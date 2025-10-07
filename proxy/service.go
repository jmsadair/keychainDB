package proxy

import (
	"context"

	chainclient "github.com/jmsadair/keychain/chain/client"
	coordinatorclient "github.com/jmsadair/keychain/coordinator/client"
	"github.com/jmsadair/keychain/proxy/node"
	"github.com/jmsadair/keychain/proxy/server"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Service is the proxy service.
type Service struct {
	// The proxy HTTP server.
	HTTPServer *server.HTTPServer
	// The proxy RPC service.
	GRPCServer *server.RPCServer
	// The proxy implementation.
	Proxy *node.Proxy
}

// NewService creates a new proxy service.
func NewService(
	httpAddr string,
	grpcAddr string,
	raftMembers []string,
	dialOpts ...grpc.DialOption,
) (*Service, error) {
	chainTn, err := chainclient.NewClient(dialOpts...)
	if err != nil {
		return nil, err
	}
	coordinatorTn, err := coordinatorclient.NewClient(dialOpts...)
	if err != nil {
		return nil, err
	}
	p := node.NewProxy(raftMembers, coordinatorTn, chainTn)
	gRPCServer := server.NewServer(grpcAddr, p)
	httpServer := &server.HTTPServer{Address: httpAddr, GRPCAddress: grpcAddr, DialOptions: dialOpts}
	return &Service{HTTPServer: httpServer, GRPCServer: gRPCServer, Proxy: p}, nil
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
	return g.Wait()
}
