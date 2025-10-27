package coordinator

import (
	"context"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	gw "github.com/jmsadair/keychain/proto/coordinator"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const defaultShutdownTimeout = 500 * time.Millisecond

type HTTPServer struct {
	GRPCListen  string
	Listen      string
	DialOptions []grpc.DialOption
}

func (s *HTTPServer) Run(ctx context.Context) error {
	cc, err := grpc.NewClient(s.GRPCListen, s.DialOptions...)
	if err != nil {
		return err
	}
	mux := runtime.NewServeMux(runtime.WithHealthzEndpoint(grpc_health_v1.NewHealthClient(cc)))
	if err := gw.RegisterCoordinatorServiceHandlerFromEndpoint(ctx, mux, s.GRPCListen, s.DialOptions); err != nil {
		return err
	}

	httpServer := &http.Server{Addr: s.Listen, Handler: mux}
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
