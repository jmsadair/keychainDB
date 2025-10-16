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
	Advertise string
	// Address that the service will listen for incoming RPCs on.
	Listen string
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
	// gRPC server implementation.
	GRPCServer *server.RPCServer
	// The raft consensus protocol implementation.
	Raft *raft.RaftBackend
	// The configuration for this service.
	Config ServiceConfig
}

// NewService creates a new coordinator service.
func NewService(cfg ServiceConfig) (*Service, error) {

}

// Run runs the service.
func (s *Service) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return s.GRPCServer.Run(ctx) })
	s.Config.Log.InfoContext(
		ctx,
		"running coservice",
		"local-id",
		s.Config.ID,
		"listen",
		s.Config.Listen,
		"advertise",
		s.Config.Advertise,
	)
	return g.Wait()
}
