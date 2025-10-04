package node

import (
	"context"
	"sync"
	"time"

	chainnode "github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/coordinator/raft"
	chainpb "github.com/jmsadair/keychain/proto/chain"
	pb "github.com/jmsadair/keychain/proto/coordinator"
	"golang.org/x/sync/errgroup"
)

const (
	heartbeatInterval   = 250 * time.Millisecond
	chainFailureTimeout = 5 * time.Second
)

type Transport interface {
	UpdateConfiguration(ctx context.Context, address string, request *chainpb.UpdateConfigurationRequest) (*chainpb.UpdateConfigurationResponse, error)
	Ping(ctx context.Context, address string, request *chainpb.PingRequest) (*chainpb.PingResponse, error)
}

type RaftProtocol interface {
	AddChainMember(ctx context.Context, id, address string) (*chainnode.Configuration, error)
	RemoveChainMember(ctx context.Context, id string) (*chainnode.Configuration, *chainnode.ChainMember, error)
	ReadChainConfiguration(ctx context.Context) (*chainnode.Configuration, error)
	LeaderCh() <-chan bool
	ChainConfiguration() *chainnode.Configuration
	JoinCluster(ctx context.Context, id, address string) error
	RemoveFromCluster(ctx context.Context, id string) error
	ClusterStatus() (raft.Status, error)
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

func (c *Coordinator) AddMember(ctx context.Context, request *pb.AddMemberRequest) (*pb.AddMemberResponse, error) {
	config, err := c.Raft.AddChainMember(ctx, request.GetId(), request.GetAddress())
	if err != nil {
		return nil, err
	}
	return &pb.AddMemberResponse{}, c.updateChainMemberConfigurations(ctx, config, nil)
}

func (c *Coordinator) RemoveMember(ctx context.Context, request *pb.RemoveMemberRequest) (*pb.RemoveMemberResponse, error) {
	config, removed, err := c.Raft.RemoveChainMember(ctx, request.GetId())
	if err != nil {
		return nil, err
	}
	return &pb.RemoveMemberResponse{}, c.updateChainMemberConfigurations(ctx, config, removed)
}

func (c *Coordinator) ReadMembershipConfiguration(ctx context.Context, request *pb.ReadChainConfigurationRequest) (*pb.ReadChainConfigurationResponse, error) {
	config, err := c.Raft.ReadChainConfiguration(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.ReadChainConfigurationResponse{Configuration: config.Proto()}, nil
}

func (c *Coordinator) JoinCluster(ctx context.Context, request *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
	if err := c.Raft.JoinCluster(ctx, request.GetId(), request.GetAddress()); err != nil {
		return nil, err
	}
	return &pb.JoinClusterResponse{}, nil
}

func (c *Coordinator) RemoveFromCluster(ctx context.Context, request *pb.RemoveFromClusterRequest) (*pb.RemoveFromClusterResponse, error) {
	if err := c.Raft.RemoveFromCluster(ctx, request.GetId()); err != nil {
		return nil, err
	}
	return &pb.RemoveFromClusterResponse{}, nil
}

func (c *Coordinator) ClusterStatus(ctx context.Context, request *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	status, err := c.Raft.ClusterStatus()
	if err != nil {
		return nil, err
	}
	return &pb.ClusterStatusResponse{Leader: status.Leader, Members: status.Members}, nil
}

func (c *Coordinator) updateChainMemberConfigurations(ctx context.Context, config *chainnode.Configuration, removed *chainnode.ChainMember) error {
	g, ctx := errgroup.WithContext(ctx)
	members := config.Members()
	if removed != nil {
		members = append(members, removed)
	}
	for _, member := range members {
		g.Go(func() error {
			req := &chainpb.UpdateConfigurationRequest{Configuration: config.Proto()}
			_, err := c.tn.UpdateConfiguration(ctx, member.Address, req)
			if removed != nil && member.Address == removed.Address {
				return nil
			}
			return err
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

	req := &pb.ReadChainConfigurationRequest{}
	resp, err := c.ReadMembershipConfiguration(ctx, req)
	if err != nil {
		return err
	}
	config := chainnode.NewConfigurationFromProto(resp.GetConfiguration())

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
		req := &chainpb.UpdateConfigurationRequest{Configuration: config.Proto()}
		g.Go(func() error {
			_, err := c.tn.UpdateConfiguration(ctx, member.Address, req)
			return err
		})
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
		g.Go(func() error {
			req := &pb.RemoveMemberRequest{Id: memberID}
			_, err := c.RemoveMember(ctx, req)
			return err
		})
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
			req := &chainpb.PingRequest{}
			resp, err := c.tn.Ping(ctx, member.Address, req)
			if err == nil {
				c.mu.Lock()
				state, ok := c.memberStates[member.ID]
				if ok {
					state.lastContact = time.Now()
					state.configVersion = resp.GetConfigVersion()
					state.status = chainnode.Status(resp.GetStatus())
				}
				c.mu.Unlock()
				if resp.GetConfigVersion() != config.Version {
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
