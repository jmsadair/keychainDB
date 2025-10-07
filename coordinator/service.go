package coordinator

import (
	"context"

	chaingrpc "github.com/jmsadair/keychain/chain/grpc"
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
	RPCServer *server.RPCServer
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
	tn, err := chaingrpc.NewClient(dialOpts...)
	if err != nil {
		return nil, err
	}
	coordinator := node.NewCoordinator(httpAddr, tn, rb)
	httpSrv := &server.HTTPServer{Address: httpAddr, GRPCAddress: grpcAddr, DialOptions: dialOpts}
	grpcSrv := server.NewServer(grpcAddr, coordinator)
	return &Service{HTTPServer: httpSrv, RPCServer: grpcSrv, Coordinator: coordinator, Raft: rb}, nil
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
		return s.RPCServer.Run(ctx)
	})
	return g.Wait()
}
