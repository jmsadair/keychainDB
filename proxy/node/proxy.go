package node

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/jmsadair/keychain/api/types"
	chainnode "github.com/jmsadair/keychain/chain/node"
	apipb "github.com/jmsadair/keychain/proto/api"
	chainpb "github.com/jmsadair/keychain/proto/chain"
	coordinatorpb "github.com/jmsadair/keychain/proto/coordinator"
	proxypb "github.com/jmsadair/keychain/proto/proxy"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

var (
	// ErrNoMembers is returned when the proxy is unable to perform an operation due to there not
	// being any chain nodes.
	ErrNoMembers = errors.New("proxyserver: chain has no members")
	// ErrCoordinatorUnavailable is returned when the proxy is unable to contact the coordinator.
	ErrCoordinatorUnavailable = errors.New("proxyserver: failed to read chain configuration from coordinator")
)

func forwardToLeader[T any](clusterMembers []string, fn func(target string) (T, error)) (T, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var success bool
	var successResp T

	// Try all of the coordinators - one of them should be the leader.
	wg.Add(len(clusterMembers))
	for _, m := range clusterMembers {
		go func() {
			defer wg.Done()
			resp, err := fn(m)
			if err != nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if success {
				return
			}
			success = true
			successResp = resp
		}()
	}

	wg.Wait()

	var zero T
	if !success {
		return zero, ErrCoordinatorUnavailable
	}

	return successResp, nil
}

// ChainClient defines the interface for the proxy to communicate with chain nodes.
type ChainClient interface {
	// Read sends the appropriate RPC to the target node.
	Read(ctx context.Context, target string, request *chainpb.ReadRequest) (*chainpb.ReadResponse, error)
	// Replicate sends the appropriate RPC to the target node.
	Replicate(ctx context.Context, target string, request *chainpb.ReplicateRequest) (*chainpb.ReplicateResponse, error)
	// Close closes all connections managed by this client.
	Close() error
}

// CoordinatorClient defines the interface for the proxy to communicate with coordinator nodes.
type CoordinatorClient interface {
	// GetMembers sends the appropriate RPC to the target node.
	GetMembers(ctx context.Context, address string, request *coordinatorpb.GetMembersRequest) (*coordinatorpb.GetMembersResponse, error)
	// Close closes all connections managed by this client.
	Close() error
}

// Proxy is a proxy that sits between clients and chain nodes.
// It is responsible for determing which chain nodes requests shouldbe routed to.
type Proxy struct {
	proxypb.UnimplementedProxyServiceServer
	chainClient       ChainClient
	coordinatorClient CoordinatorClient
	raftMembers       []string
	chainConfig       atomic.Pointer[chainnode.Configuration]
	log               *slog.Logger
}

// NewProxy creates a new proxy nodes.
func NewProxy(raftMembers []string, coordinatorClient CoordinatorClient, chainClient ChainClient, log *slog.Logger) *Proxy {
	return &Proxy{chainClient: chainClient, coordinatorClient: coordinatorClient, raftMembers: raftMembers, log: log}
}

// Shutdown cleans up any resources consumed by this node.
func (p *Proxy) Shutdown() error {
	return errors.Join(p.chainClient.Close(), p.coordinatorClient.Close())
}

// Get is used to get the value for a key.
func (p *Proxy) Get(ctx context.Context, request *apipb.GetRequest) (*apipb.GetResponse, error) {
	config, err := p.getChainMembership(ctx, false)
	if err != nil {
		return nil, err
	}
	tail := config.Tail()
	if tail == nil {
		return nil, ErrNoMembers
	}

	readReq := &chainpb.ReadRequest{Key: request.GetKey(), ConfigVersion: config.Version}
	readResp, err := p.chainClient.Read(ctx, tail.Address, readReq)
	if err != nil && errors.Is(err, types.ErrGRPCInvalidConfigVersion) {
		p.log.WarnContext(ctx, "proxy configuration version does not match chain configuration version")
		config, err := p.getChainMembership(ctx, true)
		if err != nil {
			return nil, err
		}
		tail := config.Tail()
		if tail == nil {
			return nil, ErrNoMembers
		}
		readReq.ConfigVersion = config.Version
		readResp, err = p.chainClient.Read(ctx, tail.Address, readReq)
		if err != nil {
			return nil, err
		}
		return &apipb.GetResponse{Value: readResp.GetValue()}, nil
	}
	if err != nil {
		return nil, err
	}

	return &apipb.GetResponse{Value: readResp.GetValue()}, nil
}

// Set is used to set the value for a key.
func (p *Proxy) Set(ctx context.Context, request *apipb.SetRequest) (*apipb.SetResponse, error) {
	config, err := p.getChainMembership(ctx, false)
	if err != nil {
		return nil, err
	}
	head := config.Head()
	if head == nil {
		return nil, ErrNoMembers
	}

	replicateReq := &chainpb.ReplicateRequest{Key: request.GetKey(), Value: request.GetValue(), ConfigVersion: config.Version}
	_, err = p.chainClient.Replicate(ctx, head.Address, replicateReq)
	if err != nil && errors.Is(err, chainnode.ErrInvalidConfigVersion) {
		p.log.WarnContext(ctx, "proxy configuration version does not match chain configuration version")
		config, err := p.getChainMembership(ctx, true)
		if err != nil {
			return nil, err
		}
		head := config.Head()
		if head == nil {
			return nil, ErrNoMembers
		}
		replicateReq.ConfigVersion = config.Version
		if _, err := p.chainClient.Replicate(ctx, head.Address, replicateReq); err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}

	return &apipb.SetResponse{}, nil
}

// Check implements the gRPC health checking protocol.
func (p *Proxy) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// Watch implements the gRPC health checking protocol.
func (p *Proxy) Watch(req *grpc_health_v1.HealthCheckRequest, _ grpc_health_v1.Health_WatchServer) error {
	return status.Error(codes.Unimplemented, "unimplemented")
}

func (p *Proxy) getChainMembership(ctx context.Context, forceRefresh bool) (*chainnode.Configuration, error) {
	config := p.chainConfig.Load()
	if !forceRefresh && config != nil {
		return config, nil
	}

	resp, err := forwardToLeader(p.raftMembers, func(target string) (*coordinatorpb.GetMembersResponse, error) {
		var req coordinatorpb.GetMembersRequest
		return p.coordinatorClient.GetMembers(ctx, target, &req)
	})
	if err != nil {
		p.log.ErrorContext(ctx, "failed to contact coordinator")
		return nil, err
	}

	config = chainnode.NewConfigurationFromProto(resp.GetConfiguration())
	p.chainConfig.Store(config)

	return config, nil
}
