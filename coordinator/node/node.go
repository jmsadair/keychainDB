package node

import (
	"context"
	"sync"
	"time"

	chainnode "github.com/jmsadair/keychain/chain/node"
	"golang.org/x/sync/errgroup"
)

const (
	heartbeatInterval   = 250 * time.Millisecond
	chainFailureTimeout = 5 * time.Second
)

type Transport interface {
	UpdateConfiguration(ctx context.Context, address string, config *chainnode.Configuration) error
	Ping(ctx context.Context, address string) error
}

type Raft interface {
	AddChainMember(ctx context.Context, id, address string) (*chainnode.Configuration, error)
	RemoveChainMember(ctx context.Context, id string) (*chainnode.Configuration, error)
	ReadChainConfiguration(ctx context.Context) (*chainnode.Configuration, error)
	LeaderCh() <-chan bool
	ChainConfiguration() *chainnode.Configuration
}

type Coordinator struct {
	address             string
	tn                  Transport
	raft                Raft
	lastContacted       map[string]time.Time
	isLeader            bool
	leadershipChangeCh  <-chan bool
	failedChainMemberCh chan any
	mu                  sync.Mutex
}

func NewCoordinator(address string, tn Transport, raft Raft) *Coordinator {
	return &Coordinator{
		address:             address,
		tn:                  tn,
		raft:                raft,
		leadershipChangeCh:  raft.LeaderCh(),
		lastContacted:       make(map[string]time.Time),
		failedChainMemberCh: make(chan any),
	}
}

func (c *Coordinator) Run(ctx context.Context) {
	go c.heartbeatLoop(ctx)
	go c.failedChainMemberLoop(ctx)
	go c.leadershipChangeLoop(ctx)
}

func (c *Coordinator) AddMember(ctx context.Context, id, address string) error {
	config, err := c.raft.AddChainMember(ctx, id, address)
	if err != nil {
		return err
	}
	return c.updateChainMemberConfigurations(ctx, config)
}

func (c *Coordinator) RemoveMember(ctx context.Context, id string) error {
	config, err := c.raft.RemoveChainMember(ctx, id)
	if err != nil {
		return err
	}
	return c.updateChainMemberConfigurations(ctx, config)
}

func (c *Coordinator) ReadMembershipConfiguration(ctx context.Context) (*chainnode.Configuration, error) {
	return c.raft.ReadChainConfiguration(ctx)
}

func (c *Coordinator) updateChainMemberConfigurations(ctx context.Context, config *chainnode.Configuration) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, member := range config.Members() {
		member := member // capture loop variable
		g.Go(func() error {
			return c.tn.UpdateConfiguration(ctx, member.Address, config)
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

func (c *Coordinator) onLeadershipChange(isLeader bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.isLeader = isLeader
	c.lastContacted = map[string]time.Time{}
}

func (c *Coordinator) onFailedChainMember(ctx context.Context) error {
	var toRemove []string

	c.mu.Lock()
	if !c.isLeader {
		c.mu.Unlock()
		return nil
	}
	for memberID, lastContact := range c.lastContacted {
		if time.Since(lastContact) > chainFailureTimeout {
			toRemove = append(toRemove, memberID)
		}
	}
	c.mu.Unlock()

	g, ctx := errgroup.WithContext(ctx)
	for _, memberID := range toRemove {
		memberID := memberID // capture loop variable
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
	config := c.raft.ChainConfiguration()
	for memberID := range c.lastContacted {
		if !config.IsMemberByID(memberID) {
			delete(c.lastContacted, memberID)
		}
	}
	for _, member := range config.Members() {
		_, ok := c.lastContacted[member.ID]
		if !ok {
			c.lastContacted[member.ID] = time.Now()
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
		member := member // capture loop variable
		g.Go(func() error {
			err := c.tn.Ping(ctx, member.Address)
			c.mu.Lock()
			if err == nil {
				c.lastContacted[member.ID] = time.Now()
			}
			c.mu.Unlock()
			return err
		})
	}

	return g.Wait()
}
