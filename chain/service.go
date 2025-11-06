package chain

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jmsadair/keychainDB/api/types"
	"github.com/jmsadair/keychainDB/chain/storage"
	"github.com/jmsadair/keychainDB/internal/transport"
	pb "github.com/jmsadair/keychainDB/proto/chain"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// toGRPCError maps a service error to its gRPC equivalent.
// If no equivalent exists, this function will return nil.
func toGRPCError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, ErrInvalidConfigVersion):
		return types.ErrGRPCInvalidConfigVersion
	case errors.Is(err, ErrNotHead):
		return types.ErrGRPCNotHead
	case errors.Is(err, ErrNotMemberOfChain):
		return types.ErrGRPCNotMemberOfChain
	case errors.Is(err, ErrSyncing):
		return types.ErrGRPCSyncing
	case errors.Is(err, storage.ErrConflict):
		return types.ErrGRPCConflict
	case errors.Is(err, storage.ErrEmptyKey):
		return types.ErrGRPCEmptyKey
	case errors.Is(err, storage.ErrKeyNotFound):
		return types.ErrGRPCKeyNotFound
	case errors.Is(err, storage.ErrUncommittedRead):
		return types.ErrGRPCUncommittedRead
	}

	// No known gRPC equivalent.
	return nil
}

// ServiceConfig contains the configurations for a chain service.
type ServiceConfig struct {
	// Unique ID that identifies the service.
	ID string
	// Address that the service will listen for incoming RPCs on.
	Listen string
	// Directory where the service will store on-disk data.
	StorageDir string
	// The gRPC Dial options a service will use when making RPCs to other services.
	DialOptions []grpc.DialOption
	// Logger that the service will use for logging.
	Log *slog.Logger
}

// Service is the chain service.
type Service struct {
	// The gRPC server.
	Server *transport.Server
	// Chain node implementation.
	Node *ChainNode
	// The configuration for this server.
	Config ServiceConfig
	// Storage for the chain node.
	store *storage.PersistentStorage
}

// NewService creates a new chain service.
func NewService(cfg ServiceConfig) (*Service, error) {
	tn, err := NewClient(cfg.DialOptions...)
	if err != nil {
		return nil, err
	}
	store, err := storage.NewPersistentStorage(cfg.StorageDir)
	if err != nil {
		return nil, err
	}
	node := NewChainNode(cfg.ID, cfg.Listen, store, tn, cfg.Log)
	srv := transport.NewServer(cfg.Listen, func(s *grpc.Server) { pb.RegisterChainServiceServer(s, node) },
		grpc.UnaryInterceptor(transport.UnaryServerErrorInterceptor(toGRPCError)))
	return &Service{Server: srv, Node: node, Config: cfg, store: store}, nil
}

// Run runs the service.
func (s *Service) Run(ctx context.Context) error {
	defer s.store.Close()
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		s.Node.Run(ctx)
		return nil
	})
	g.Go(func() error { return s.Server.Run(ctx) })
	s.Config.Log.InfoContext(ctx, "running chain service", "local-id", s.Config.ID, "listen", s.Config.Listen)
	return g.Wait()
}
