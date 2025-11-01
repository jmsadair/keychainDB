package transport

import (
	"context"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const defaultShutdownTimeout = 500 * time.Millisecond

// ServiceHandlerRegistry registers the handlers for the service.
type ServiceHandlerRegistry = func(context.Context, *runtime.ServeMux, string, []grpc.DialOption) error

// HTTPGateway is a gateway that translates a RESTful HTTP API into gRPC.
type HTTPGateway struct {
	// Address of the gRPC service.
	GRPCAddress string
	// The listen address for the gateway.
	Listen string
	// The dial options that are passed to the client that calls the gRPC service.
	dialOpts []grpc.DialOption
	registry ServiceHandlerRegistry
}

// NewHTTPGateway creates a new HTTP gateway for a given listen address, gRPC service address, and registration function.
func NewHTTPGateway(listen string, gRPCAddr string, registry ServiceHandlerRegistry, dialOpts ...grpc.DialOption) *HTTPGateway {
	return &HTTPGateway{
		GRPCAddress: gRPCAddr,
		Listen:      listen,
		registry:    registry,
		dialOpts:    dialOpts,
	}
}

// Run runs the gateway.
func (s *HTTPGateway) Run(ctx context.Context) error {
	cc, err := grpc.NewClient(s.GRPCAddress, s.dialOpts...)
	if err != nil {
		return err
	}

	mux := runtime.NewServeMux(runtime.WithHealthzEndpoint(grpc_health_v1.NewHealthClient(cc)))
	if err := s.registry(ctx, mux, s.GRPCAddress, s.dialOpts); err != nil {
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
