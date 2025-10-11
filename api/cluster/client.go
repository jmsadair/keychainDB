package cluster

import (
	"context"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	"github.com/jmsadair/keychain/coordinator/client"
	pb "github.com/jmsadair/keychain/proto/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

func tryAll[S any, T any](
	ctx context.Context,
	endpoints []string,
	request S,
	fn func(context.Context, string, S) (T, error),
) (T, error) {

	var wg sync.WaitGroup
	var mu sync.Mutex
	var lastErr error
	var successResp T
	var success bool

	wg.Add(len(endpoints))
	for _, endpoint := range endpoints {
		go func() {
			defer wg.Done()
			resp, err := fn(ctx, endpoint, request)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				lastErr = err
				return
			}
			success = true
			successResp = resp
		}()
	}

	wg.Wait()

	var zero T
	if !success {
		return zero, lastErr
	}

	return successResp, nil
}

// Config is used to configure the client.
type Config struct {
	Credentials credentials.TransportCredentials
	Endpoints   []string
	MaxRetries  int
}

// Client exposes the API for interacting with the coordinator cluster.
type Client struct {
	config Config
	client *client.Client
}

// NewClient creates a new client.
func NewClient(cfg Config) (*Client, error) {
	client, err := client.NewClient(
		grpc.WithTransportCredentials(cfg.Credentials),
		grpc.WithUnaryInterceptor(
			retry.UnaryClientInterceptor(
				retry.WithMax(uint(cfg.MaxRetries)),
				retry.WithCodes(codes.Aborted, codes.Unavailable),
			),
		),
	)
	if err != nil {
		return nil, err
	}
	return &Client{config: cfg, client: client}, nil
}

// GetMembers reads the chain membership configuration.
func (c *Client) GetMembers(ctx context.Context, request *pb.GetMembersRequest) (*pb.GetMembersResponse, error) {
	return tryAll(ctx, c.config.Endpoints, request, c.client.GetMembers)
}

// JoinCluster adds a coordinator to the cluster.
func (c *Client) JoinCluster(ctx context.Context, request *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
	return tryAll(ctx, c.config.Endpoints, request, c.client.JoinCluster)
}

// RemoveFromCluster removes a coordinator from the cluster.
func (c *Client) RemoveFromCluster(ctx context.Context, request *pb.RemoveFromClusterRequest) (*pb.RemoveFromClusterResponse, error) {
	return tryAll(ctx, c.config.Endpoints, request, c.client.RemoveFromCluster)
}

// AddMember adds a a node to a chain.
func (c *Client) AddMember(ctx context.Context, request *pb.AddMemberRequest) (*pb.AddMemberResponse, error) {
	return tryAll(ctx, c.config.Endpoints, request, c.client.AddMember)
}

// RemoveMember removes a node from a chain.
func (c *Client) RemoveMember(ctx context.Context, request *pb.RemoveMemberRequest) (*pb.RemoveMemberResponse, error) {
	return tryAll(ctx, c.config.Endpoints, request, c.client.RemoveMember)
}

// ClusterStatus gets the status of the coordinator cluster.
func (c *Client) ClusterStatus(ctx context.Context, request *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	return tryAll(ctx, c.config.Endpoints, request, c.client.ClusterStatus)
}
