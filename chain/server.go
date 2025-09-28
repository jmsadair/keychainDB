package chain

import (
	"context"

	chaingrpc "github.com/jmsadair/keychain/chain/grpc"
	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/chain/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Server is the chain service.
type Server struct {
	// A gRPC server.
	GRPCServer *chaingrpc.Server
	// Chain node implementation.
	Node *node.ChainNode
}

// NewServer creates a new server.
func NewServer(id string, address string, storePath string, dialOpts ...grpc.DialOption) (*Server, error) {
	tn, err := chaingrpc.NewClient(dialOpts...)
	if err != nil {
		return nil, err
	}
	store, err := storage.NewPersistentStorage(storePath)
	if err != nil {
		return nil, err
	}
	node := node.NewChainNode(id, address, store, tn)
	grpcServer := chaingrpc.NewServer(address, node)
	return &Server{GRPCServer: grpcServer, Node: node}, nil
}

// Run runs the server.
func (s *Server) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		s.Node.Run(ctx)
		return nil
	})
	g.Go(func() error { return s.GRPCServer.Run(ctx) })
	return g.Wait()
}
