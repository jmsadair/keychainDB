package transport

import (
	"context"
	"net"
	"sync"

	"github.com/jmsadair/keychain/internal/lru"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

const defaultCacheCapacity = 50

// ServiceRegistrar is a function that registers a service on a grpc.Server.
type ServiceRegistrar func(server *grpc.Server)

// Server is a reusable gRPC server wrapper.
type Server struct {
	Address  string
	register ServiceRegistrar
}

// NewServer creates a new gRPC server for a given address and registration function.
func NewServer(address string, register ServiceRegistrar) *Server {
	return &Server{
		Address:  address,
		register: register,
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

	srv := grpc.NewServer()
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
}

// ClientFactory is a function that wraps a grpc.ClientConn into a typed gRPC client.
type ClientFactory[T any] func(conn grpc.ClientConnInterface) T

// ClientCache manages a cache of gRPC clients of a specific type. The cache employs and LRU eviction strategy.
// When a client is evicted from the cache, the underlying connection will be closed.
type ClientCache[T any] struct {
	mu       sync.Mutex
	clients  *lru.LruCache[string, *cachedClient[T]]
	dialOpts []grpc.DialOption
	factory  ClientFactory[T]
}

// NewClientCache creates a new client cache with the provided dial options and client factory.
func NewClientCache[T any](factory ClientFactory[T], dialOpts ...grpc.DialOption) *ClientCache[T] {
	closeOnEvict := func(_ string, cc *cachedClient[T]) {
		cc.conn.Close()
	}
	return &ClientCache[T]{
		clients:  lru.NewLruCache(defaultCacheCapacity, closeOnEvict),
		dialOpts: dialOpts,
		factory:  factory,
	}
}

// GetOrCreate returns an existing client or creates a new one if not cached.
func (c *ClientCache[T]) GetOrCreate(address string) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if cc, ok := c.clients.Get(address); ok {
		return cc.client, nil
	}

	conn, err := grpc.NewClient(address, c.dialOpts...)
	if err != nil {
		var zero T
		return zero, err
	}

	client := c.factory(conn)
	c.clients.Set(address, &cachedClient[T]{conn: conn, client: client})
	return client, nil
}
