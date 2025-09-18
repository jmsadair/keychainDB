package coordinator

import (
	"context"

	coordinatorhttp "github.com/jmsadair/keychain/coordinator/http"
	"github.com/jmsadair/keychain/coordinator/node"
	"github.com/jmsadair/keychain/coordinator/raft"
)

type Server struct {
	HTTP        *coordinatorhttp.Server
	HTTPAddr    string
	RaftAddr    string
	Coordinator *node.Coordinator
	Raft        *raft.RaftBackend
	ID          string
}

func NewServer(
	ID string,
	HTTPAddr string,
	RaftAddr string,
	tn node.Transport,
	storePath string,
	snapshotStorePath string,
	bootstrap bool,
) (*Server, error) {
	rb, err := raft.NewRaftBackend(ID, RaftAddr, storePath, snapshotStorePath, bootstrap)
	if err != nil {
		return nil, err
	}
	coordinator := node.NewCoordinator(HTTPAddr, tn, rb)
	srv := &coordinatorhttp.Server{Address: HTTPAddr, Coordinator: coordinator}
	return &Server{ID: ID, HTTPAddr: HTTPAddr, RaftAddr: RaftAddr, HTTP: srv, Coordinator: coordinator, Raft: rb}, nil
}

func (s *Server) Run(ctx context.Context) error {
	coordinatorErrCh := make(chan error)
	go func() {
		coordinatorErrCh <- s.Coordinator.Run(ctx)
	}()
	httpServerErrCh := make(chan error)
	go func() {
		httpServerErrCh <- s.HTTP.Run(ctx)
	}()

	if err := <-httpServerErrCh; err != nil {
		return err
	}
	return <-coordinatorErrCh
}
