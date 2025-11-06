package coordinator

import (
	"context"
	"errors"
	"log/slog"

	"github.com/hashicorp/raft"
	"github.com/jmsadair/keychainDB/api/types"
	"github.com/jmsadair/keychainDB/chain"
	"github.com/jmsadair/keychainDB/coordinator/node"
	"github.com/jmsadair/keychainDB/internal/transport"
	coordinatorpb "github.com/jmsadair/keychainDB/proto/coordinator"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// toGRPCError maps a service error to its gRPC equivalent.
// If no equivalent exists, this function will return nil.
func toGRPCError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, node.ErrConfigurationUpdateFailed):
		return types.ErrGRPCConfigurationUpdateFailed
	case errors.Is(err, node.ErrEnqueueTimeout):
		return types.ErrGRPCEnqueueTimeout
	case errors.Is(err, node.ErrLeader):
		return types.ErrGRPCLeader
	case errors.Is(err, node.ErrNodeExists):
		return types.ErrGRPCNodeExists
	case errors.Is(err, node.ErrLeadershipLost):
		return types.ErrGRPCLeadershipLost
	case errors.Is(err, node.ErrNotLeader):
		return types.ErrGRPCNotLeader
	}

	// No known gRPC equivalent.
	return nil
}

// ServiceConfig contains the configurations for a coordinator service.
type ServiceConfig struct {
	// ID that uniquely identifies this service instance.
	ID string
	// Address that will be advertised to other members of the cluster.
	Advertise string
	// Address that the service will listen for incoming RPCs on. This is shared by raft and the coordinator.
	Listen string
	// Address that the service will listen for incoming HTTP requests on.
	HTTPListen string
	// Directory where raft will store logs and snapshots.
	StorageDir string
	// Whether or not to bootstrap a new raft cluster.
	Bootstrap bool
	// gRPC dial options a service will use when making RPCs to other servers.
	DialOptions []grpc.DialOption
	// Logger that a server will use for logging.
	Log *slog.Logger
}

// Service is the coordinator service.
type Service struct {
	// HTTP gateway for translating HTTP requests to gRPC.
	Gateway *transport.HTTPGateway
	// gRPC server implementation.
	Server *transport.Server
	// The coordinator implementation.
	Coordinator *node.Coordinator
	// The raft implementation.
	Raft *node.RaftBackend
	// The configuration for this service.
	Config ServiceConfig
}

// NewService creates a new coordinator service.
func NewService(cfg ServiceConfig) (*Service, error) {
	chainClient, err := chain.NewClient(cfg.DialOptions...)
	if err != nil {
		return nil, err
	}

	coordinatorClient, err := NewClient(cfg.DialOptions...)
	if err != nil {
		return nil, err
	}

	// Create a raft node and bootstrap it if requested.
	rb, err := node.NewRaftBackend(cfg.ID, cfg.Advertise, cfg.StorageDir, cfg.DialOptions...)
	if err != nil {
		return nil, err
	}
	if cfg.Bootstrap {
		clusterConfig := raft.Configuration{
			Servers: []raft.Server{{ID: raft.ServerID(cfg.ID), Address: raft.ServerAddress(cfg.Advertise)}},
		}
		if err := rb.Bootstrap(clusterConfig); err != nil {
			return nil, err
		}
	}

	coordinatorNode := node.NewCoordinator(cfg.ID, cfg.Advertise, chainClient, coordinatorClient, rb, cfg.Log)
	srv := transport.NewServer(cfg.Listen, func(grpcServer *grpc.Server) {
		coordinatorpb.RegisterCoordinatorServiceServer(grpcServer, coordinatorNode)
		rb.Register(grpcServer)
		grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
	}, grpc.UnaryInterceptor(transport.UnaryServerErrorInterceptor(toGRPCError)))
	gw := transport.NewHTTPGateway(
		cfg.HTTPListen,
		cfg.Listen,
		coordinatorpb.RegisterCoordinatorServiceHandlerFromEndpoint,
		cfg.DialOptions...,
	)

	return &Service{
		Gateway:     gw,
		Server:      srv,
		Coordinator: coordinatorNode,
		Config:      cfg,
		Raft:        rb,
	}, nil
}

// Run runs the service.
func (s *Service) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		s.Coordinator.Run(ctx)
		return nil
	})
	g.Go(func() error {
		return s.Gateway.Run(ctx)
	})
	g.Go(func() error {
		return s.Server.Run(ctx)
	})

	s.Config.Log.InfoContext(
		ctx,
		"running coordinator service",
		"local-id",
		s.Config.ID,
		"http-listen",
		s.Config.HTTPListen,
		"listen",
		s.Config.Listen,
		"advertise",
		s.Config.Advertise,
	)

	return g.Wait()
}
