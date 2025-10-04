package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	chainnode "github.com/jmsadair/keychain/chain/node"
	chainpb "github.com/jmsadair/keychain/proto/chain"
	coordinatorpb "github.com/jmsadair/keychain/proto/coordinator"
	proxypb "github.com/jmsadair/keychain/proto/proxy"
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
	ReadChainConfiguration(
		ctx context.Context,
		address string,
		request *coordinatorpb.ReadChainConfigurationRequest,
	) (*coordinatorpb.ReadChainConfigurationResponse, error)
}

type Proxy struct {
	chainTn       ChainTransport
	coordinatorTn CoordinatorTransport
	raftMembers   []string
	chainConfig   atomic.Pointer[chainnode.Configuration]
}

func NewProxy(raftMembers []string, coordinatorTn CoordinatorTransport, chainTn ChainTransport) *Proxy {
	return &Proxy{chainTn: chainTn, coordinatorTn: coordinatorTn, raftMembers: raftMembers}
}

func (p *Proxy) Get(ctx context.Context, request *proxypb.GetRequest) (*proxypb.GetResponse, error) {
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
		return &proxypb.GetResponse{Value: readResp.GetValue()}, nil
	}
	if err != nil {
		return nil, err
	}

	return &proxypb.GetResponse{Value: readResp.GetValue()}, nil
}

func (p *Proxy) Set(ctx context.Context, request *proxypb.SetRequest) (*proxypb.SetResponse, error) {
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

	return &proxypb.SetResponse{}, nil
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
			var req coordinatorpb.ReadChainConfigurationRequest
			resp, err := p.coordinatorTn.ReadChainConfiguration(ctx, member, &req)
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

	return nil, ErrConfigReadFailure
}
