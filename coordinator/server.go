package coordinator

import (
	"context"

	coordinatorhttp "github.com/jmsadair/keychain/coordinator/http"
	"github.com/jmsadair/keychain/coordinator/node"
	"github.com/jmsadair/keychain/coordinator/raft"
	"golang.org/x/sync/errgroup"
)

// Server represents the coordinator service. It exposes a public HTTP API for managing cluster and chain membership.
type Server struct {
	// HTTP server that exposes the API for the coordinator.
	HTTPServer *coordinatorhttp.Server
	// Raft implementation for maintaining chain configuration.
	Raft *raft.RaftBackend
	// The implementation of the chain coordinator.
	Coordinator *node.Coordinator
}

// NewServer will creates a new coordinator server.
func NewServer(
	id string,
	httpAddr string,
	raftAddr string,
	tn node.Transport,
	storePath string,
	snapshotStorePath string,
	bootstrap bool,
) (*Server, error) {
	rb, err := raft.NewRaftBackend(id, raftAddr, storePath, snapshotStorePath, bootstrap)
	if err != nil {
		return nil, err
	}
	coordinator := node.NewCoordinator(httpAddr, tn, rb)
	srv := &coordinatorhttp.Server{Address: httpAddr, Coordinator: coordinator}
	return &Server{HTTPServer: srv, Coordinator: coordinator, Raft: rb}, nil
}

// Run runs this server. This is a blocking call.
func (s *Server) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.Coordinator.Run(ctx)
	})
	g.Go(func() error {
		return s.HTTPServer.Run(ctx)
	})
	return g.Wait()
}
