package kv

import (
	"context"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	"github.com/jmsadair/keychainDB/api/types"
	"github.com/jmsadair/keychainDB/internal/transport"
	apipb "github.com/jmsadair/keychainDB/proto/api"
	proxypb "github.com/jmsadair/keychainDB/proto/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

var defaultCallOps = []grpc.CallOption{grpc.WaitForReady(true)}

type Config struct {
	Endpoint    string
	Credentials credentials.TransportCredentials
	MaxRetries  int
}

// Client exposes the main keychainDB API.
type Client struct {
	config      Config
	clientCache *transport.ClientCache[proxypb.ProxyServiceClient]
}

// NewClient creates a new client.
func NewClient(cfg Config) (*Client, error) {
	client := transport.NewClientCache(
		proxypb.NewProxyServiceClient,
		grpc.WithTransportCredentials(cfg.Credentials),
		grpc.WithUnaryInterceptor(
			retry.UnaryClientInterceptor(
				retry.WithMax(uint(cfg.MaxRetries)),
				retry.WithCodes(codes.Aborted, codes.Unavailable),
			),
		),
	)
	return &Client{config: cfg, clientCache: client}, nil
}

// Set sets the value of a key.
func (c *Client) Set(ctx context.Context, key string, value []byte) error {
	client, err := c.clientCache.GetOrCreate(c.config.Endpoint)
	if err != nil {
		return types.Error(err)
	}
	req := &apipb.SetRequest{Key: key, Value: value}
	_, err = client.Set(ctx, req, defaultCallOps...)
	if err != nil {
		return types.Error(err)
	}
	return nil
}

// Get gets the value of a key.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	client, err := c.clientCache.GetOrCreate(c.config.Endpoint)
	if err != nil {
		return nil, types.Error(err)
	}
	req := &apipb.GetRequest{Key: key}
	resp, err := client.Get(ctx, req, defaultCallOps...)
	if err != nil {
		fmt.Println(err.Error())
		return nil, types.Error(err)
	}
	return resp.GetValue(), nil
}
