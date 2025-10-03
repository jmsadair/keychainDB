package api

import (
	"context"

	"github.com/jmsadair/keychain/internal/transport"
	pb "github.com/jmsadair/keychain/proto/proxy"
	"google.golang.org/grpc"
)

// Client exposes the main keychain API.
type Client struct {
	endpoint string
	cache    *transport.ClientCache[pb.ProxyServiceClient]
}

// NewClient creates a new client.
func NewClient(endpoint string, dialOpts ...grpc.DialOption) (*Client, error) {
	return &Client{endpoint: endpoint, cache: transport.NewClientCache(pb.NewProxyServiceClient, dialOpts...)}, nil
}

// Set sets the value of a key.
func (c *Client) Set(ctx context.Context, key string, value []byte) error {
	client, err := c.cache.GetOrCreate(c.endpoint)
	if err != nil {
		return err
	}
	req := &pb.SetRequest{Key: key, Value: value}
	_, err = client.Set(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

// Get gets the value of a key.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	client, err := c.cache.GetOrCreate(c.endpoint)
	if err != nil {
		return nil, err
	}
	req := &pb.GetRequest{Key: key}
	resp, err := client.Get(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.GetValue(), nil
}
