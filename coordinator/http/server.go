package http

import (
	"context"
	"net/http"
	"time"

	"github.com/jmsadair/keychain/coordinator/node"
)

const defaultShutdownTimeout = 500 * time.Millisecond

type Server struct {
	Address     string
	Coordinator *node.Coordinator
}

func (s *Server) Run(ctx context.Context) error {
	http.HandleFunc("/cluster/join", s.handleJoinCluster)
	http.HandleFunc("/cluster/remove", s.handleRemoveFromCluster)
	http.HandleFunc("/chain/add", s.handleAddChainMember)
	http.HandleFunc("/chain/remove", s.handleRemoveChainMember)
	httpServer := &http.Server{Addr: s.Address}

	errCh := make(chan error)
	go func() {
		<-ctx.Done()
		ctx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer cancel()
		errCh <- httpServer.Shutdown(ctx)
	}()

	if err := http.ListenAndServe(s.Address, nil); err != http.ErrServerClosed {
		return err
	}

	return <-errCh
}
