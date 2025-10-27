package coordinator

import (
	"context"
	"log/slog"

	"github.com/hashicorp/raft"
	"github.com/jmsadair/keychain/api/types"
	chainclient "github.com/jmsadair/keychain/chain/client"
	"github.com/jmsadair/keychain/coordinator/node"
	"github.com/jmsadair/keychain/internal/transport"
	coordinatorpb "github.com/jmsadair/keychain/proto/coordinator"
	raftclient "github.com/jmsadair/keychain/raft/client"
	"github.com/jmsadair/keychain/raft/network"
	"github.com/jmsadair/keychain/raft/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var errToGRPCError = map[error]error{
	node.ErrConfigurationUpdateFailed: types.ErrGRPCConfigurationUpdateFailed,
	node.ErrEnqueueTimeout:            types.ErrGRPCEnqueueTimeout,
	node.ErrLeader:                    types.ErrGRPCLeader,
	node.ErrNodeExists:                types.ErrGRPCNodeExists,
	node.ErrLeadershipLost:            types.ErrGRPCLeadershipLost,
	node.ErrNotLeader:                 types.ErrGRPCNotLeader,
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
	// HTTP server implementation.
	HTTPServer *HTTPServer
	// gRPC server implementation.
	Server *transport.Server
	// The coordinator implementation.
	Coordinator *node.Coordinator
	// The configuration for this service.
	Config ServiceConfig
	// The raft implementation.
	Raft *raft.Raft

	store *storage.PersistentStorage
}

// NewService creates a new coordinator service.
func NewService(cfg ServiceConfig) (*Service, error) {
	logStore, err := storage.NewPersistentStorage(cfg.StorageDir)
	if err != nil {
		return nil, err
	}
	snapshotStore, err := storage.NewSnapshotStorage(cfg.StorageDir)
	if err != nil {
		return nil, err
	}
	raftClient, err := raftclient.NewClient(cfg.DialOptions...)
	if err != nil {
		return nil, err
	}
	chainClient, err := chainclient.NewClient(cfg.DialOptions...)
	if err != nil {
		return nil, err
	}

	netLayer := network.NewNetwork(cfg.Advertise, raftClient)
	fsm := node.NewFSM()
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.ID)
	consensus, err := raft.NewRaft(raftCfg, fsm, logStore, logStore, snapshotStore, netLayer.Transport())
	if err != nil {
		return nil, err
	}

	if cfg.Bootstrap {
		clusterConfig := raft.Configuration{
			Servers: []raft.Server{{ID: raftCfg.LocalID, Address: netLayer.Transport().LocalAddr()}},
		}
		future := consensus.BootstrapCluster(clusterConfig)
		if err := future.Error(); err != nil {
			return nil, err
		}
	}

	rb := node.NewRaftBackend(consensus, fsm)
	coordinatorNode := node.NewCoordinator(cfg.ID, cfg.Advertise, chainClient, rb, cfg.Log)
	srv := transport.NewServer(cfg.Listen, func(grpcServer *grpc.Server) {
		coordinatorpb.RegisterCoordinatorServiceServer(grpcServer, coordinatorNode)
		netLayer.Register(grpcServer)
		grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
	}, grpc.UnaryInterceptor(transport.UnaryServerErrorInterceptor(errToGRPCError)))
	httpSrv := &HTTPServer{Listen: cfg.HTTPListen, GRPCListen: cfg.Listen, DialOptions: cfg.DialOptions}
	return &Service{
		HTTPServer:  httpSrv,
		Server:      srv,
		Coordinator: coordinatorNode,
		Config:      cfg,
		Raft:        consensus,
		store:       logStore,
	}, nil
}

// Run runs the service.
func (s *Service) Run(ctx context.Context) error {
	defer s.store.Close()
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.Coordinator.Run(ctx)
	})
	g.Go(func() error {
		return s.HTTPServer.Run(ctx)
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

	if err := g.Wait(); err != nil {
		return err
	}
	future := s.Raft.Shutdown()
	return future.Error()
}
