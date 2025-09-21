package http

import (
	"context"
	"net/http"
	"time"

	"github.com/jmsadair/keychain/coordinator/node"
	"golang.org/x/sync/errgroup"
)

const defaultShutdownTimeout = 500 * time.Millisecond

type Server struct {
	Address     string
	Coordinator *node.Coordinator
}

func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/cluster/join", s.handleJoinCluster)
	mux.HandleFunc("/cluster/remove", s.handleRemoveFromCluster)
	mux.HandleFunc("/cluster/status", s.handleClusterStatus)
	mux.HandleFunc("/chain/add", s.handleAddChainMember)
	mux.HandleFunc("/chain/remove", s.handleRemoveChainMember)
	mux.HandleFunc("/chain/configuration", s.handleChainConfiguration)
	mux.HandleFunc("/healthz", s.handleHealth)
	httpServer := &http.Server{Addr: s.Address, Handler: mux}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		<-ctx.Done()
		ctx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer cancel()
		return httpServer.Shutdown(ctx)
	})
	g.Go(func() error {
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	return g.Wait()
}
