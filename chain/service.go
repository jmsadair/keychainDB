package chain

import (
	"context"
	"log/slog"

	"github.com/jmsadair/keychain/api/types"
	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/chain/storage"
	"github.com/jmsadair/keychain/internal/transport"
	pb "github.com/jmsadair/keychain/proto/chain"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Maps service errors to gRPC errors.
var errToGRPCError = map[error]error{
	node.ErrInvalidConfigVersion: types.ErrGRPCInvalidConfigVersion,
	node.ErrNotHead:              types.ErrGRPCNotHead,
	node.ErrNotMemberOfChain:     types.ErrNotMemberOfChain,
	node.ErrSyncing:              types.ErrGRPCSyncing,
	storage.ErrConflict:          types.ErrGRPCConflict,
	storage.ErrEmptyKey:          types.ErrGRPCEmptyKey,
	storage.ErrKeyNotFound:       types.ErrGRPCKeyNotFound,
	storage.ErrUncommittedRead:   types.ErrUncommittedRead,
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
	Node *node.ChainNode
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
	node := node.NewChainNode(cfg.ID, cfg.Listen, store, tn, cfg.Log)
	srv := transport.NewServer(cfg.Listen, func(s *grpc.Server) { pb.RegisterChainServiceServer(s, node) },
		grpc.UnaryInterceptor(transport.UnaryServerErrorInterceptor(errToGRPCError)))
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
