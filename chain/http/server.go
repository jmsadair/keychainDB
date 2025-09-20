package http

import (
	"context"
	"net/http"
	"time"

	"github.com/jmsadair/keychain/chain/node"
	"golang.org/x/sync/errgroup"
)

const defaultShutdownTimeout = 500 * time.Millisecond

type Server struct {
	Address string
	Node    *node.ChainNode
}

func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/get", s.handleGet)
	mux.HandleFunc("/set", s.handleSet)
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
