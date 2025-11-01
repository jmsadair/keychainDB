package transport

import (
	"context"
	"errors"
	"net"
	"sync"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryServerErrorInterceptor maps server errors to their gRPC equivalent.
func UnaryServerErrorInterceptor(errToGRPCErrorMapping map[error]error) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			if _, ok := status.FromError(err); ok {
				return nil, err
			}
			grpcErr, ok := errToGRPCErrorMapping[err]
			if !ok {
				grpcErr := status.New(codes.Internal, err.Error())
				return nil, grpcErr.Err()
			}
			return nil, grpcErr
		}
		return resp, nil
	}
}

// StreamServerErrorInterceptor maps server errors to their gRPC equivalent.
func StreamServerErrorInterceptor(errToGRPCErrorMapping map[error]error) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		if err != nil {
			if _, ok := status.FromError(err); ok {
				return err
			}
			grpcErr, ok := errToGRPCErrorMapping[err]
			if !ok {
				grpcErr := status.New(codes.Internal, err.Error())
				return grpcErr.Err()
			}
			return grpcErr
		}
		return nil
	}
}

// ServiceRegistrar is a function that registers a service on a grpc.Server.
type ServiceRegistrar func(server *grpc.Server)

// Server is a reusable gRPC server wrapper.
type Server struct {
	Address  string
	register ServiceRegistrar
	opts     []grpc.ServerOption
}

// NewServer creates a new gRPC server for a given address and registration function.
func NewServer(address string, register ServiceRegistrar, opts ...grpc.ServerOption) *Server {
	return &Server{
		Address:  address,
		register: register,
		opts:     opts,
	}
}

// Run starts the server.
func (s *Server) Run(ctx context.Context) error {
	resolved, err := net.ResolveTCPAddr("tcp", s.Address)
	if err != nil {
		return err
	}

	listener, err := net.Listen(resolved.Network(), resolved.String())
	if err != nil {
		return err
	}

	srv := grpc.NewServer(s.opts...)
	s.register(srv)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := srv.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()
		srv.GracefulStop()
		return nil
	})

	return g.Wait()
}

type cachedClient[T any] struct {
	client T
	conn   *grpc.ClientConn
	mu     sync.RWMutex
}

// ClientFactory is a function that wraps a grpc.ClientConn into a typed gRPC client.
type ClientFactory[T any] func(conn grpc.ClientConnInterface) T

// ClientCache manages a cache of gRPC clients of a specific type.
type ClientCache[T any] struct {
	mu       sync.Mutex
	clients  map[string]*cachedClient[T]
	dialOpts []grpc.DialOption
	factory  ClientFactory[T]
}

// NewClientCache creates a new ClientCache.
func NewClientCache[T any](factory ClientFactory[T], dialOpts ...grpc.DialOption) *ClientCache[T] {
	return &ClientCache[T]{
		clients:  make(map[string]*cachedClient[T]),
		dialOpts: dialOpts,
		factory:  factory,
	}
}

// GetOrCreate returns an existing client if one is cached.
// Otherwise, it will create one and it add it to the cache.
func (c *ClientCache[T]) GetOrCreate(address string) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cc, ok := c.clients[address]; ok {
		return cc.client, nil
	}

	conn, err := grpc.NewClient(address, c.dialOpts...)
	if err != nil {
		var zero T
		return zero, err
	}

	client := c.factory(conn)
	c.clients[address] = &cachedClient[T]{conn: conn, client: client}
	return client, nil
}

// Close closes all connections managed by this cache.
func (c *ClientCache[T]) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	closeErrs := make([]error, 0, len(c.clients))
	for _, cc := range c.clients {
		closeErrs = append(closeErrs, cc.conn.Close())
	}

	return errors.Join(closeErrs...)
}
