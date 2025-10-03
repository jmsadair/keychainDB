package proxy

import (
	"context"

	chaingrpc "github.com/jmsadair/keychain/chain/grpc"
	coordinatorgrpc "github.com/jmsadair/keychain/coordinator/grpc"
	proxyhttp "github.com/jmsadair/keychain/proxy/http"
	"github.com/jmsadair/keychain/proxy/node"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Server is the proxy service.
type Server struct {
	// HTTP server that exposes public API.
	HTTPServer *proxyhttp.Server
	// The proxy implementation.
	Proxy *node.Proxy
}

// NewServer creates a new server.
func NewServer(
	httpAddr string,
	raftMembers []string,
	dialOpts ...grpc.DialOption,
) (*Server, error) {
	chainTn, err := chaingrpc.NewClient(dialOpts...)
	if err != nil {
		return nil, err
	}
	coordinatorTn, err := coordinatorgrpc.NewClient(dialOpts...)
	if err != nil {
		return nil, err
	}
	p := node.NewProxy(raftMembers, coordinatorTn, chainTn)
	srv := &proxyhttp.Server{Address: httpAddr, Proxy: p}
	return &Server{HTTPServer: srv, Proxy: p}, nil
}

// Run runs the server.
func (s *Server) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.HTTPServer.Run(ctx)
	})
	return g.Wait()
}
