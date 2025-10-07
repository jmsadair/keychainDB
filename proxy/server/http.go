package server

import (
	"context"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	gw "github.com/jmsadair/keychain/proto/proxy"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const defaultShutdownTimeout = 500 * time.Millisecond

// HTTPServer is the HTTP proxy server.
type HTTPServer struct {
	GRPCAddress string
	Address     string
	DialOptions []grpc.DialOption
}

// Run runs the server.
func (s *HTTPServer) Run(ctx context.Context) error {
	cc, err := grpc.NewClient(s.GRPCAddress, s.DialOptions...)
	if err != nil {
		return err
	}
	mux := runtime.NewServeMux(runtime.WithHealthzEndpoint(grpc_health_v1.NewHealthClient(cc)))
	if err := gw.RegisterProxyServiceHandlerFromEndpoint(ctx, mux, s.GRPCAddress, s.DialOptions); err != nil {
		return err
	}

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
