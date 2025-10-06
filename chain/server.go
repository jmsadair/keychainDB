package chain

import (
	"context"
	"log/slog"

	chaingrpc "github.com/jmsadair/keychain/chain/grpc"
	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/chain/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// ServerConfig contains the server configurations for a chain server.
type ServerConfig struct {
	// Unique ID that identifies the server.
	ID string
	// Address tht a server will listen for incoming requests on.
	ListenAddr string
	// Path to where a server will store on-disk data.
	StoragePath string
	// gRPC Dial options a erver will use when making RPCs to other servers.
	DialOptions []grpc.DialOption
	// Logger that a server will use for logging.
	Log *slog.Logger
}

// Server is the chain service.
type Server struct {
	// A gRPC server.
	GRPCServer *chaingrpc.Server
	// Chain node implementation.
	Node *node.ChainNode
	// The configuration for this server.
	Config ServerConfig
	// Storage for the chain node.
	store *storage.PersistentStorage
}

// NewServer creates a new server.
func NewServer(cfg ServerConfig) (*Server, error) {
	tn, err := chaingrpc.NewClient(cfg.DialOptions...)
	if err != nil {
		return nil, err
	}
	store, err := storage.NewPersistentStorage(cfg.StoragePath)
	if err != nil {
		return nil, err
	}
	node := node.NewChainNode(cfg.ID, cfg.ListenAddr, store, tn, cfg.Log)
	grpcServer := chaingrpc.NewServer(cfg.ListenAddr, node)
	return &Server{GRPCServer: grpcServer, Node: node, Config: cfg, store: store}, nil
}

// Run runs the server.
func (s *Server) Run(ctx context.Context) error {
	defer s.store.Close()
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		s.Node.Run(ctx)
		return nil
	})
	g.Go(func() error { return s.GRPCServer.Run(ctx) })
	s.Config.Log.InfoContext(ctx, "running chain server", "local-id", s.Config.ID, "listen", s.Config.ListenAddr)
	return g.Wait()
}
