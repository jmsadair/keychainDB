package chain

import (
	"context"

	chaingrpc "github.com/jmsadair/keychain/chain/grpc"
	chainhttp "github.com/jmsadair/keychain/chain/http"
	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/chain/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Server is the chain service.
type Server struct {
	// HTTP server that exposes public API.
	HTTPServer *chainhttp.Server
	// gRPC server used by internal clients.
	GRPCServer *chaingrpc.Server
	// The chain node implementation.
	Node *node.ChainNode
}

// NewServer creates a new server.
func NewServer(id string, httpAddr string, gRPCAddr string, storePath string, dialOpts ...grpc.DialOption) (*Server, error) {
	tn, err := chaingrpc.NewClient(dialOpts...)
	if err != nil {
		return nil, err
	}
	store, err := storage.NewPersistentStorage(storePath)
	if err != nil {
		return nil, err
	}
	node := node.NewChainNode(id, gRPCAddr, store, tn)
	grpcServer := chaingrpc.NewServer(gRPCAddr, node)
	httpServer := &chainhttp.Server{Address: httpAddr, Node: node}
	return &Server{HTTPServer: httpServer, GRPCServer: grpcServer, Node: node}, nil
}

// Run runs the server.
func (s *Server) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		s.Node.Run(ctx)
		return nil
	})
	g.Go(func() error { return s.GRPCServer.Run(ctx) })
	g.Go(func() error { return s.HTTPServer.Run(ctx) })
	return g.Wait()
}
