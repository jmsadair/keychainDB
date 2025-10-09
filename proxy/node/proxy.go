package node

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	chainnode "github.com/jmsadair/keychain/chain/node"
	apipb "github.com/jmsadair/keychain/proto/api"
	chainpb "github.com/jmsadair/keychain/proto/chain"
	coordinatorpb "github.com/jmsadair/keychain/proto/coordinator"
)

var (
	ErrNoMembers         = errors.New("chain has no members")
	ErrConfigReadFailure = errors.New("failed to read chain configuration from coordinator")
)

type ChainTransport interface {
	Read(ctx context.Context, target string, request *chainpb.ReadRequest) (*chainpb.ReadResponse, error)
	Replicate(ctx context.Context, target string, request *chainpb.ReplicateRequest) (*chainpb.ReplicateResponse, error)
}

type CoordinatorTransport interface {
	GetMembers(
		ctx context.Context,
		address string,
		request *coordinatorpb.GetMembersRequest,
	) (*coordinatorpb.GetMembersResponse, error)
}

type Proxy struct {
	chainTn       ChainTransport
	coordinatorTn CoordinatorTransport
	raftMembers   []string
	chainConfig   atomic.Pointer[chainnode.Configuration]
	log           *slog.Logger
}

func NewProxy(raftMembers []string, coordinatorTn CoordinatorTransport, chainTn ChainTransport, log *slog.Logger) *Proxy {
	return &Proxy{chainTn: chainTn, coordinatorTn: coordinatorTn, raftMembers: raftMembers, log: log}
}

func (p *Proxy) Get(ctx context.Context, request *apipb.GetRequest) (*apipb.GetResponse, error) {
	config, err := p.getChainConfiguration(ctx, false)
	if err != nil {
		return nil, err
	}
	tail := config.Tail()
	if tail == nil {
		return nil, ErrNoMembers
	}

	readReq := &chainpb.ReadRequest{Key: request.GetKey(), ConfigVersion: config.Version}
	readResp, err := p.chainTn.Read(ctx, tail.Address, readReq)
	if err != nil && errors.Is(err, chainnode.ErrInvalidConfigVersion) {
		p.log.WarnContext(ctx, "proxy configuration version does not match chain configuration version")
		config, err := p.getChainConfiguration(ctx, true)
		if err != nil {
			return nil, err
		}
		tail := config.Tail()
		if tail == nil {
			return nil, ErrNoMembers
		}
		readReq.ConfigVersion = config.Version
		readResp, err = p.chainTn.Read(ctx, tail.Address, readReq)
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

func (p *Proxy) Set(ctx context.Context, request *apipb.SetRequest) (*apipb.SetResponse, error) {
	config, err := p.getChainConfiguration(ctx, false)
	if err != nil {
		return nil, err
	}
	head := config.Head()
	if head == nil {
		return nil, ErrNoMembers
	}

	replicateReq := &chainpb.ReplicateRequest{Key: request.GetKey(), Value: request.GetValue(), ConfigVersion: config.Version}
	_, err = p.chainTn.Replicate(ctx, head.Address, replicateReq)
	if err != nil && errors.Is(err, chainnode.ErrInvalidConfigVersion) {
		p.log.WarnContext(ctx, "proxy configuration version does not match chain configuration version")
		config, err := p.getChainConfiguration(ctx, true)
		if err != nil {
			return nil, err
		}
		head := config.Head()
		if head == nil {
			return nil, ErrNoMembers
		}
		replicateReq.ConfigVersion = config.Version
		if _, err := p.chainTn.Replicate(ctx, head.Address, replicateReq); err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}

	return &apipb.SetResponse{}, nil
}

func (p *Proxy) getChainConfiguration(ctx context.Context, forceRefresh bool) (*chainnode.Configuration, error) {
	config := p.chainConfig.Load()
	if !forceRefresh && config != nil {
		return config, nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	memberToConfig := make(map[string]*chainnode.Configuration, len(p.raftMembers))
	wg.Add(len(p.raftMembers))
	for _, member := range p.raftMembers {
		go func() {
			defer wg.Done()
			var req coordinatorpb.GetMembersRequest
			resp, err := p.coordinatorTn.GetMembers(ctx, member, &req)
			mu.Lock()
			memberToConfig[member] = nil
			if err == nil {
				memberToConfig[member] = chainnode.NewConfigurationFromProto(resp.GetConfiguration())
			}
			mu.Unlock()
		}()
	}

	wg.Wait()
	for _, config := range memberToConfig {
		if config == nil {
			continue
		}
		p.chainConfig.Store(config)
		return config, nil
	}

	p.log.ErrorContext(ctx, "failed to contact coordinator")
	return nil, ErrConfigReadFailure
}
