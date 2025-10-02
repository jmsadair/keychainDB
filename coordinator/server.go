package coordinator

import (
	"context"

	chaingrpc "github.com/jmsadair/keychain/chain/grpc"
	coordinatorgrpc "github.com/jmsadair/keychain/coordinator/grpc"
	coordinatorhttp "github.com/jmsadair/keychain/coordinator/http"
	"github.com/jmsadair/keychain/coordinator/node"
	"github.com/jmsadair/keychain/coordinator/raft"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Server is the coordinator service.
type Server struct {
	// HTTP server that exposes public API.
	HTTPServer *coordinatorhttp.Server
	// gRPC server used by internal clients.
	GRPCServer *coordinatorgrpc.Server
	// The raft consensus protocol implementation.
	Raft *raft.RaftBackend
	// The coordinator implementation.
	Coordinator *node.Coordinator
}

// NewServer creates a new server.
func NewServer(
	id string,
	httpAddr string,
	grpcAddr string,
	raftAddr string,
	storePath string,
	snapshotStorePath string,
	bootstrap bool,
	dialOpts ...grpc.DialOption,
) (*Server, error) {
	rb, err := raft.NewRaftBackend(id, raftAddr, storePath, snapshotStorePath, bootstrap)
	if err != nil {
		return nil, err
	}
	tn, err := chaingrpc.NewClient(dialOpts...)
	if err != nil {
		return nil, err
	}
	coordinator := node.NewCoordinator(httpAddr, tn, rb)
	httpSrv := &coordinatorhttp.Server{Address: httpAddr, GRPCAddress: grpcAddr, DialOptions: dialOpts}
	grpcSrv := coordinatorgrpc.NewServer(grpcAddr, coordinator)
	return &Server{HTTPServer: httpSrv, GRPCServer: grpcSrv, Coordinator: coordinator, Raft: rb}, nil
}

// Run runs the server.
func (s *Server) Run(ctx context.Context) error {
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
