package coordinator

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/jmsadair/keychain/chain"
	"golang.org/x/sync/errgroup"
)

const (
	heartbeatInterval   = 250 * time.Millisecond
	chainFailureTimeout = 5 * time.Second
)

type Transport interface {
	UpdateConfiguration(ctx context.Context, address net.Addr, config *chain.Configuration) error
	Ping(ctx context.Context, address net.Addr) error
}

type Raft interface {
	AddChainMember(ctx context.Context, member net.Addr) (*chain.Configuration, error)
	RemoveChainMember(ctx context.Context, member net.Addr) (*chain.Configuration, error)
	ReadChainConfiguration(ctx context.Context) (*chain.Configuration, error)
	LeaderCh() <-chan bool
	ChainConfiguration() *chain.Configuration
}

type Coordinator struct {
	address             net.Addr
	tn                  Transport
	raft                Raft
	lastContacted       map[string]time.Time
	isLeader            bool
	leadershipChangeCh  <-chan bool
	failedChainMemberCh chan any
	mu                  sync.Mutex
}

func NewCoordinator(address net.Addr, tn Transport, raft Raft) *Coordinator {
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

func (c *Coordinator) AddMember(ctx context.Context, member net.Addr) error {
	config, err := c.raft.AddChainMember(ctx, member)
	if err != nil {
		return err
	}
	return c.updateChainMemberConfigurations(ctx, config)
}

func (c *Coordinator) RemoveMember(ctx context.Context, member net.Addr) error {
	config, err := c.raft.RemoveChainMember(ctx, member)
	if err != nil {
		return err
	}
	return c.updateChainMemberConfigurations(ctx, config)
}

func (c *Coordinator) ReadMembershipConfiguration(ctx context.Context) (*chain.Configuration, error) {
	return c.raft.ReadChainConfiguration(ctx)
}

func (c *Coordinator) updateChainMemberConfigurations(ctx context.Context, config *chain.Configuration) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, member := range config.Members() {
		g.Go(func() error {
			return c.tn.UpdateConfiguration(ctx, member, config)
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
	var toRemove []net.Addr

	c.mu.Lock()
	if !c.isLeader {
		c.mu.Unlock()
		return nil
	}
	for member, lastContact := range c.lastContacted {
		if time.Since(lastContact) > chainFailureTimeout {
			addr, err := net.ResolveTCPAddr("tcp", member)
			if err != nil {
				c.mu.Unlock()
				return err
			}
			toRemove = append(toRemove, addr)
		}
	}
	c.mu.Unlock()

	g, ctx := errgroup.WithContext(ctx)
	for _, member := range toRemove {
		g.Go(func() error { return c.RemoveMember(ctx, member) })
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
	for member := range c.lastContacted {
		addr, err := net.ResolveTCPAddr("tcp", member)
		if err != nil {
			c.mu.Unlock()
			return err
		}
		if !config.IsMember(addr) {
			delete(c.lastContacted, member)
		}
	}
	for _, member := range config.Members() {
		_, ok := c.lastContacted[member.String()]
		if !ok {
			c.lastContacted[member.String()] = time.Now()
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

func (c *Coordinator) sendHeartbeats(ctx context.Context, config *chain.Configuration) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, member := range config.Members() {
		g.Go(func() error {
			err := c.tn.Ping(ctx, member)
			c.mu.Lock()
			if err == nil {
				c.lastContacted[member.String()] = time.Now()
			}
			c.mu.Unlock()
			return err
		})
	}

	return g.Wait()
}
