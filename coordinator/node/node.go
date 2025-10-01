package node

import (
	"context"
	"sync"
	"time"

	chainnode "github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/coordinator/raft"
	"golang.org/x/sync/errgroup"
)

const (
	heartbeatInterval   = 250 * time.Millisecond
	chainFailureTimeout = 5 * time.Second
)

type Transport interface {
	UpdateConfiguration(ctx context.Context, address string, request *chainnode.UpdateConfigurationRequest, response *chainnode.UpdateConfigurationResponse) error
	Ping(ctx context.Context, address string, request *chainnode.PingRequest, response *chainnode.PingResponse) error
}

type RaftProtocol interface {
	AddChainMember(ctx context.Context, id, address string) (*chainnode.Configuration, error)
	RemoveChainMember(ctx context.Context, id string) (*chainnode.Configuration, *chainnode.ChainMember, error)
	ReadChainConfiguration(ctx context.Context) (*chainnode.Configuration, error)
	LeaderCh() <-chan bool
	ChainConfiguration() *chainnode.Configuration
	JoinCluster(ctx context.Context, id, address string) error
	RemoveFromCluster(ctx context.Context, id string) error
	ClusterStatus() (*raft.Status, error)
	Shutdown() error
}

type memberState struct {
	lastContact   time.Time
	status        chainnode.Status
	configVersion uint64
}

type Coordinator struct {
	Raft                RaftProtocol
	Address             string
	tn                  Transport
	memberStates        map[string]*memberState
	isLeader            bool
	leadershipChangeCh  <-chan bool
	failedChainMemberCh chan any
	configSyncCh        chan any
	mu                  sync.Mutex
}

func NewCoordinator(address string, tn Transport, raft RaftProtocol) *Coordinator {
	return &Coordinator{
		Address:             address,
		tn:                  tn,
		Raft:                raft,
		leadershipChangeCh:  raft.LeaderCh(),
		memberStates:        make(map[string]*memberState),
		failedChainMemberCh: make(chan any),
		configSyncCh:        make(chan any),
	}
}

func (c *Coordinator) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		c.heartbeatLoop(ctx)
		return nil
	})
	g.Go(func() error {
		c.failedChainMemberLoop(ctx)
		return nil
	})
	g.Go(func() error {
		c.leadershipChangeLoop(ctx)
		return nil
	})
	g.Go(func() error {
		c.configSyncLoop(ctx)
		return nil
	})

	g.Wait()
	return c.Raft.Shutdown()
}

func (c *Coordinator) AddMember(ctx context.Context, id, address string) error {
	config, err := c.Raft.AddChainMember(ctx, id, address)
	if err != nil {
		return err
	}
	return c.updateChainMemberConfigurations(ctx, config, nil)
}

func (c *Coordinator) RemoveMember(ctx context.Context, id string) error {
	config, removed, err := c.Raft.RemoveChainMember(ctx, id)
	if err != nil {
		return err
	}
	return c.updateChainMemberConfigurations(ctx, config, removed)
}

func (c *Coordinator) ReadMembershipConfiguration(ctx context.Context) (*chainnode.Configuration, error) {
	return c.Raft.ReadChainConfiguration(ctx)
}

func (c *Coordinator) updateChainMemberConfigurations(ctx context.Context, config *chainnode.Configuration, removed *chainnode.ChainMember) error {
	g, ctx := errgroup.WithContext(ctx)
	members := config.Members()
	if removed != nil {
		members = append(members, removed)
	}
	for _, member := range members {
		g.Go(func() error {
			req := &chainnode.UpdateConfigurationRequest{Configuration: config}
			var resp chainnode.UpdateConfigurationResponse
			return c.tn.UpdateConfiguration(ctx, member.Address, req, &resp)
		})
	}

	return g.Wait()
}

func (c *Coordinator) heartbeatLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(heartbeatInterval):
			c.onHeartbeat(ctx)
		}
	}
}

func (c *Coordinator) failedChainMemberLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.failedChainMemberCh:
			c.onFailedChainMember(ctx)
		}
	}
}

func (c *Coordinator) leadershipChangeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case isLeader := <-c.leadershipChangeCh:
			c.onLeadershipChange(isLeader)
		}
	}
}

func (c *Coordinator) configSyncLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.configSyncCh:
			c.onConfigSync(ctx)
		}
	}
}

func (c *Coordinator) onConfigSync(ctx context.Context) error {
	c.mu.Lock()
	if !c.isLeader {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	config, err := c.ReadMembershipConfiguration(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	needSync := []string{}
	for memberID, state := range c.memberStates {
		if state.configVersion != config.Version {
			needSync = append(needSync, memberID)
		}
	}
	c.mu.Unlock()

	g, ctx := errgroup.WithContext(ctx)
	for _, memberID := range needSync {
		member := config.Member(memberID)
		if member == nil {
			continue
		}
		req := chainnode.UpdateConfigurationRequest{Configuration: config}
		var resp chainnode.UpdateConfigurationResponse
		g.Go(func() error { return c.tn.UpdateConfiguration(ctx, member.Address, &req, &resp) })
	}

	return g.Wait()
}

func (c *Coordinator) onLeadershipChange(isLeader bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.isLeader = isLeader
	c.memberStates = map[string]*memberState{}
}

func (c *Coordinator) onFailedChainMember(ctx context.Context) error {
	var toRemove []string

	c.mu.Lock()
	if !c.isLeader {
		c.mu.Unlock()
		return nil
	}
	for memberID, state := range c.memberStates {
		if time.Since(state.lastContact) > chainFailureTimeout {
			toRemove = append(toRemove, memberID)
		}
	}
	c.mu.Unlock()

	g, ctx := errgroup.WithContext(ctx)
	for _, memberID := range toRemove {
		g.Go(func() error { return c.RemoveMember(ctx, memberID) })
	}

	return g.Wait()
}

func (c *Coordinator) onHeartbeat(ctx context.Context) error {
	c.mu.Lock()
	if !c.isLeader {
		c.mu.Unlock()
		return nil
	}
	config := c.Raft.ChainConfiguration()
	for memberID := range c.memberStates {
		if !config.IsMemberByID(memberID) {
			delete(c.memberStates, memberID)
		}
	}
	for _, member := range config.Members() {
		_, ok := c.memberStates[member.ID]
		if !ok {
			c.memberStates[member.ID] = &memberState{lastContact: time.Now()}
		}
	}
	c.mu.Unlock()

	err := c.sendHeartbeats(ctx, config)
	if err != nil {
		select {
		case c.failedChainMemberCh <- struct{}{}:
		default:
		}
	}

	return err
}

func (c *Coordinator) sendHeartbeats(ctx context.Context, config *chainnode.Configuration) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, member := range config.Members() {
		g.Go(func() error {
			req := &chainnode.PingRequest{}
			var resp chainnode.PingResponse
			err := c.tn.Ping(ctx, member.Address, req, &resp)
			if err == nil {
				c.mu.Lock()
				state, ok := c.memberStates[member.ID]
				if ok {
					state.lastContact = time.Now()
					state.configVersion = resp.Version
					state.status = resp.Status
				}
				c.mu.Unlock()
				if resp.Version != config.Version {
					select {
					case c.configSyncCh <- struct{}{}:
					default:
					}
				}

			}
			return err
		})
	}

	return g.Wait()
}
