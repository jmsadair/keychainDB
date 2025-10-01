package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	chainnode "github.com/jmsadair/keychain/chain/node"
	coordinatornode "github.com/jmsadair/keychain/coordinator/node"
)

var (
	ErrNoMembers         = errors.New("chain has no members")
	ErrConfigReadFailure = errors.New("failed to read chain configuration from coordinator")
)

type ChainTransport interface {
	Read(ctx context.Context, target string, request *chainnode.ReadRequest, response *chainnode.ReadResponse) error
	Replicate(ctx context.Context, target string, request *chainnode.ReplicateRequest, response *chainnode.ReplicateResponse) error
}

type CoordinatorTransport interface {
	ReadChainConfiguration(
		ctx context.Context,
		address string,
		request *coordinatornode.ReadChainConfigurationRequest,
		response *coordinatornode.ReadChainConfigurationResponse,
	) error
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

func (p *Proxy) GetValue(ctx context.Context, key string) ([]byte, error) {
	config, err := p.getChainConfiguration(ctx, false)
	if err != nil {
		return nil, err
	}
	tail := config.Tail()
	if tail == nil {
		return nil, ErrNoMembers
	}

	req := &chainnode.ReadRequest{Key: key, ConfigVersion: config.Version}
	var resp chainnode.ReadResponse
	err = p.chainTn.Read(ctx, tail.Address, req, &resp)
	if err != nil && errors.Is(err, chainnode.ErrInvalidConfigVersion) {
		fmt.Println(err.Error())
		config, err := p.getChainConfiguration(ctx, true)
		if err != nil {
			return nil, err
		}
		tail := config.Tail()
		if tail == nil {
			return nil, ErrNoMembers
		}
		req.ConfigVersion = config.Version
		err = p.chainTn.Read(ctx, tail.Address, req, &resp)
		if err != nil {
			fmt.Println(err.Error())
			return nil, err
		}
		return resp.Value, nil
	}
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	return resp.Value, nil
}

func (p *Proxy) SetValue(ctx context.Context, key string, value []byte) error {
	config, err := p.getChainConfiguration(ctx, false)
	if err != nil {
		return err
	}
	head := config.Head()
	if head == nil {
		return ErrNoMembers
	}

	req := &chainnode.ReplicateRequest{Key: key, Value: value, ConfigVersion: config.Version}
	var resp chainnode.ReplicateResponse
	err = p.chainTn.Replicate(ctx, head.Address, req, &resp)
	if err != nil && errors.Is(err, chainnode.ErrInvalidConfigVersion) {
		config, err := p.getChainConfiguration(ctx, true)
		if err != nil {
			return err
		}
		head := config.Head()
		if head == nil {
			return ErrNoMembers
		}
		req.ConfigVersion = config.Version
		return p.chainTn.Replicate(ctx, head.Address, req, &resp)
	}

	return err
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
			var req coordinatornode.ReadChainConfigurationRequest
			var resp coordinatornode.ReadChainConfigurationResponse
			err := p.coordinatorTn.ReadChainConfiguration(ctx, member, &req, &resp)
			mu.Lock()
			memberToConfig[member] = nil
			if err == nil {
				memberToConfig[member] = resp.Configuration
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
