package coordinator

import (
	"context"

	chainclient "github.com/jmsadair/keychain/chain/client"
	"github.com/jmsadair/keychain/coordinator/node"
	"github.com/jmsadair/keychain/coordinator/raft"
	"github.com/jmsadair/keychain/coordinator/server"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

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
}

// NewService creates a new coordinator service.
func NewService(
	id string,
	httpAddr string,
	grpcAddr string,
	raftAddr string,
	storePath string,
	snapshotStorePath string,
	bootstrap bool,
	dialOpts ...grpc.DialOption,
) (*Service, error) {
	rb, err := raft.NewRaftBackend(id, raftAddr, storePath, snapshotStorePath, bootstrap)
	if err != nil {
		return nil, err
	}
	tn, err := chainclient.NewClient(dialOpts...)
	if err != nil {
		return nil, err
	}
	coordinator := node.NewCoordinator(httpAddr, tn, rb)
	httpServer := &server.HTTPServer{Address: httpAddr, GRPCAddress: grpcAddr, DialOptions: dialOpts}
	gRPCServer := server.NewServer(grpcAddr, coordinator)
	return &Service{HTTPServer: httpServer, GRPCServer: gRPCServer, Coordinator: coordinator, Raft: rb}, nil
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
	return g.Wait()
}
