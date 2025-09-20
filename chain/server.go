package chain

import (
	"context"

	chaingrpc "github.com/jmsadair/keychain/chain/grpc"
	chainhttp "github.com/jmsadair/keychain/chain/http"
	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/chain/storage"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	HTTPServer *chainhttp.Server
	GRPCServer *chaingrpc.Server
	Node       *node.ChainNode
}

func NewServer(ID string, HTTPAddr string, gRPCAddr string, storePath string) (*Server, error) {
	tn, err := chaingrpc.NewClient()
	if err != nil {
		return nil, err
	}
	store, err := storage.NewPersistentStorage(storePath)
	if err != nil {
		return nil, err
	}
	node := node.NewChainNode(ID, gRPCAddr, store, tn)
	grpcServer := chaingrpc.NewServer(gRPCAddr, node)
	httpServer := &chainhttp.Server{Address: HTTPAddr, Node: node}
	return &Server{HTTPServer: httpServer, GRPCServer: grpcServer, Node: node}, nil
}

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
