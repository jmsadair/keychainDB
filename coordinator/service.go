package coordinator

import (
	"context"
	"log/slog"

	chainclient "github.com/jmsadair/keychain/chain/client"
	"github.com/jmsadair/keychain/coordinator/node"
	"github.com/jmsadair/keychain/coordinator/raft"
	"github.com/jmsadair/keychain/coordinator/server"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// ServiceConfig contains the configurations for a coordinator service.
type ServiceConfig struct {
	// ID that uniquely identifies this service instance.
	ID string
	// Address that raft will advertise to other members of the cluster.
	RaftAdvertise string
	// Address that raft will listen for incoming requests on.
	RaftListen string
	// Address that the service will listen for incoming HTTP requests on.
	HTTPListen string
	// Address that the service will listen for incoming RPCs on.
	GRPCListen string
	// Path to where a service will store on-disk raft logs.
	StoragePath string
	// Path to where a service will store on-disk raft snapshots.
	SnapshotStoragePath string
	// Whether or not to bootstrap a new raft cluster.
	Bootstrap bool
	// gRPC dial options a service will use when making RPCs to other servers.
	DialOptions []grpc.DialOption
	// Logger that a server will use for logging.
	Log *slog.Logger
}

// Service is the coordinator service.
type Service struct {
	// HTTP server implementation.
	HTTPServer *server.HTTPServer
	// gRPC server implementation.
	GRPCServer *server.RPCServer
	// The raft consensus protocol implementation.
	Raft *raft.RaftBackend
	// The coordinator implementation.
	Coordinator *node.Coordinator
	// The configuration for this service.
	Config ServiceConfig
}

// NewService creates a new coordinator service.
func NewService(cfg ServiceConfig) (*Service, error) {
	rb, err := raft.NewRaftBackend(cfg.ID, cfg.RaftListen, cfg.RaftAdvertise, cfg.StoragePath, cfg.SnapshotStoragePath, cfg.Bootstrap)
	if err != nil {
		return nil, err
	}
	tn, err := chainclient.NewClient(cfg.DialOptions...)
	if err != nil {
		return nil, err
	}
	coordinator := node.NewCoordinator(cfg.ID, cfg.RaftAdvertise, tn, rb, cfg.Log)
	httpServer := &server.HTTPServer{Address: cfg.HTTPListen, GRPCAddress: cfg.GRPCListen, DialOptions: cfg.DialOptions}
	gRPCServer := server.NewServer(cfg.GRPCListen, coordinator)
	return &Service{HTTPServer: httpServer, GRPCServer: gRPCServer, Coordinator: coordinator, Raft: rb, Config: cfg}, nil
}

// Run runs the service.
func (s *Service) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.Coordinator.Run(ctx)
	})
	g.Go(func() error {
		return s.HTTPServer.Run(ctx)
	})
	g.Go(func() error {
		return s.GRPCServer.Run(ctx)
	})
	s.Config.Log.InfoContext(
		ctx,
		"running coordinator service",
		"local-id",
		s.Config.ID,
		"http-listen",
		s.Config.HTTPListen,
		"grpc-listen",
		s.Config.HTTPListen,
		"raft-listen",
		s.Config.HTTPListen,
	)
	return g.Wait()
}
