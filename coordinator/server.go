package coordinator

import (
	"context"
	"sync"

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
	var wg sync.WaitGroup
	errCh := make(chan error)
	wg.Add(2)

	go func() {
		s.Coordinator.Run(ctx)
		wg.Done()
	}()
	go func() {
		errCh <- s.HTTP.Run(ctx)
		wg.Done()
	}()

	err := <-errCh
	wg.Wait()
	return err
}
