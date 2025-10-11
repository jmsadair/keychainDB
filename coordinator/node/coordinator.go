package node

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	chainnode "github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/coordinator/raft"
	chainpb "github.com/jmsadair/keychain/proto/chain"
	pb "github.com/jmsadair/keychain/proto/coordinator"
	"golang.org/x/sync/errgroup"
)

var (
	ErrConfigurationUpdateFailed = errors.New("coordinator: failed to update configuration for one or more chain members")
	ErrNoLeader                  = errors.New("coordinator: no leader elected")
)

const (
	heartbeatInterval   = 250 * time.Millisecond
	chainFailureTimeout = 5 * time.Second
)

func forwardToLeader[T any, S any](ctx context.Context, leaderAddr string, request T, fn func(context.Context, string, T) (S, error)) (S, error) {
	var zero S
	if leaderAddr == "" {
		return zero, ErrNoLeader
	}
	return fn(ctx, leaderAddr, request)
}

type ChainTransport interface {
	UpdateConfiguration(ctx context.Context, address string, request *chainpb.UpdateConfigurationRequest) (*chainpb.UpdateConfigurationResponse, error)
	Ping(ctx context.Context, address string, request *chainpb.PingRequest) (*chainpb.PingResponse, error)
}

type CoordinatorTransport interface {
	GetMembers(ctx context.Context, address string, request *pb.GetMembersRequest) (*pb.GetMembersResponse, error)
	JoinCluster(ctx context.Context, address string, request *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error)
	RemoveFromCluster(ctx context.Context, address string, request *pb.RemoveFromClusterRequest) (*pb.RemoveFromClusterResponse, error)
	AddMember(ctx context.Context, address string, request *pb.AddMemberRequest) (*pb.AddMemberResponse, error)
	RemoveMember(ctx context.Context, address string, request *pb.RemoveMemberRequest) (*pb.RemoveMemberResponse, error)
	ClusterStatus(ctx context.Context, address string, request *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error)
}

type RaftProtocol interface {
	AddMember(ctx context.Context, id, address string) (*chainnode.Configuration, error)
	RemoveMember(ctx context.Context, id string) (*chainnode.Configuration, *chainnode.ChainMember, error)
	GetMembers(ctx context.Context) (*chainnode.Configuration, error)
	LeaderCh() <-chan bool
	LeaderAddressAndID() (string, string)
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
	ID                  string
	Address             string
	raft                RaftProtocol
	chainTn             ChainTransport
	coordinatorTn       CoordinatorTransport
	memberStates        map[string]*memberState
	isLeader            bool
	leadershipChangeCh  <-chan bool
	failedChainMemberCh chan any
	configSyncCh        chan any
	log                 *slog.Logger
	mu                  sync.Mutex
}

func NewCoordinator(
	id string,
	address string,
	coordinatorTn CoordinatorTransport,
	chainTn ChainTransport,
	raft RaftProtocol,
	log *slog.Logger,
) *Coordinator {
	return &Coordinator{
		Address:             address,
		ID:                  id,
		raft:                raft,
		chainTn:             chainTn,
		coordinatorTn:       coordinatorTn,
		leadershipChangeCh:  raft.LeaderCh(),
		memberStates:        make(map[string]*memberState),
		failedChainMemberCh: make(chan any),
		configSyncCh:        make(chan any),
		log:                 log.With("local-id", id),
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
	return c.raft.Shutdown()
}

func (c *Coordinator) AddMember(ctx context.Context, request *pb.AddMemberRequest) (*pb.AddMemberResponse, error) {
	if leaderAddr, leaderID := c.raft.LeaderAddressAndID(); leaderID != c.ID {
		return forwardToLeader(ctx, leaderAddr, request, c.coordinatorTn.AddMember)
	}
	config, err := c.raft.AddMember(ctx, request.GetId(), request.GetAddress())
	if err != nil {
		return nil, err
	}
	return &pb.AddMemberResponse{}, c.updateChainMemberConfigurations(ctx, config, nil)
}

func (c *Coordinator) RemoveMember(ctx context.Context, request *pb.RemoveMemberRequest) (*pb.RemoveMemberResponse, error) {
	if leaderAddr, leaderID := c.raft.LeaderAddressAndID(); leaderID != c.ID {
		return forwardToLeader(ctx, leaderAddr, request, c.coordinatorTn.RemoveMember)
	}
	config, removed, err := c.raft.RemoveMember(ctx, request.GetId())
	if err != nil {
		return nil, err
	}
	return &pb.RemoveMemberResponse{}, c.updateChainMemberConfigurations(ctx, config, removed)
}

func (c *Coordinator) GetMembers(ctx context.Context, request *pb.GetMembersRequest) (*pb.GetMembersResponse, error) {
	if leaderAddr, leaderID := c.raft.LeaderAddressAndID(); leaderID != c.ID {
		return forwardToLeader(ctx, leaderAddr, request, c.coordinatorTn.GetMembers)
	}
	config, err := c.raft.GetMembers(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.GetMembersResponse{Configuration: config.Proto()}, nil
}

func (c *Coordinator) JoinCluster(ctx context.Context, request *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
	if leaderAddr, leaderID := c.raft.LeaderAddressAndID(); leaderID != c.ID {
		return forwardToLeader(ctx, leaderAddr, request, c.coordinatorTn.JoinCluster)
	}
	if err := c.raft.JoinCluster(ctx, request.GetId(), request.GetAddress()); err != nil {
		return nil, err
	}
	return &pb.JoinClusterResponse{}, nil
}

func (c *Coordinator) RemoveFromCluster(ctx context.Context, request *pb.RemoveFromClusterRequest) (*pb.RemoveFromClusterResponse, error) {
	if leaderAddr, leaderID := c.raft.LeaderAddressAndID(); leaderID != c.ID {
		return forwardToLeader(ctx, leaderAddr, request, c.coordinatorTn.RemoveFromCluster)
	}
	if err := c.raft.RemoveFromCluster(ctx, request.GetId()); err != nil {
		return nil, err
	}
	return &pb.RemoveFromClusterResponse{}, nil
}

func (c *Coordinator) ClusterStatus(ctx context.Context, request *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	if leaderAddr, leaderID := c.raft.LeaderAddressAndID(); leaderID != c.ID {
		return forwardToLeader(ctx, leaderAddr, request, c.coordinatorTn.ClusterStatus)
	}
	status, err := c.raft.ClusterStatus()
	if err != nil {
		return nil, err
	}
	return &pb.ClusterStatusResponse{Leader: status.Leader, Members: status.Members}, nil
}

func (c *Coordinator) updateChainMemberConfigurations(ctx context.Context, config *chainnode.Configuration, removed *chainnode.ChainMember) error {
	members := config.Members()
	if removed != nil {
		members = append(members, removed)
	}

	var updateFailed atomic.Bool
	var wg sync.WaitGroup
	wg.Add(len(members))
	for _, member := range members {
		go func() {
			defer wg.Done()
			req := &chainpb.UpdateConfigurationRequest{Configuration: config.Proto()}
			_, err := c.chainTn.UpdateConfiguration(ctx, member.Address, req)
			if err != nil {
				c.log.ErrorContext(
					ctx,
					"failed to update chain member configuration",
					"error",
					err.Error(),
					"member-id",
					member.ID,
					"member-address",
					member.Address,
				)
				updateFailed.Store(true)
			}
		}()
	}

	wg.Wait()
	if updateFailed.Load() {
		return ErrConfigurationUpdateFailed
	}
	return nil
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

func (c *Coordinator) onConfigSync(ctx context.Context) {
	c.mu.Lock()
	if !c.isLeader {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	config, err := c.raft.GetMembers(ctx)
	if err != nil {
		return
	}

	c.mu.Lock()
	needSync := []string{}
	for memberID, state := range c.memberStates {
		if state.configVersion != config.Version {
			needSync = append(needSync, memberID)
			c.log.WarnContext(
				ctx,
				"detected chain member with invalid configuration, attempting to sync configuration",
				"member-id",
				memberID,
				"member-config-version",
				state.configVersion,
				"coordinator-config-version",
				config.Version,
			)
		}
	}
	c.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(len(needSync))
	for _, memberID := range needSync {
		go func() {
			defer wg.Done()
			member := config.Member(memberID)
			if member == nil {
				return
			}
			req := &chainpb.UpdateConfigurationRequest{Configuration: config.Proto()}
			c.chainTn.UpdateConfiguration(ctx, member.Address, req)
		}()
	}

	wg.Wait()
}

func (c *Coordinator) onLeadershipChange(isLeader bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.isLeader = isLeader
	c.memberStates = map[string]*memberState{}
}

func (c *Coordinator) onFailedChainMember(ctx context.Context) {
	c.mu.Lock()
	if !c.isLeader {
		c.mu.Unlock()
		return
	}
	var toRemove []string
	for memberID, state := range c.memberStates {
		if time.Since(state.lastContact) > chainFailureTimeout {
			toRemove = append(toRemove, memberID)
			c.log.WarnContext(
				ctx,
				"detected failed chain member, attempting to remove from chain",
				"member-id",
				memberID,
			)
		}
	}
	c.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(len(toRemove))
	for _, memberID := range toRemove {
		go func() {
			defer wg.Done()
			config, removed, err := c.raft.RemoveMember(ctx, memberID)
			if err != nil {
				return
			}
			c.updateChainMemberConfigurations(ctx, config, removed)
		}()
	}

	wg.Wait()
}

func (c *Coordinator) onHeartbeat(ctx context.Context) {
	c.mu.Lock()
	if !c.isLeader {
		c.mu.Unlock()
		return
	}
	config := c.raft.ChainConfiguration()
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

	c.sendHeartbeats(ctx, config)
}

func (c *Coordinator) sendHeartbeats(ctx context.Context, config *chainnode.Configuration) {
	var wg sync.WaitGroup
	wg.Add(len(config.Members()))

	for _, member := range config.Members() {
		go func() {
			defer wg.Done()
			req := &chainpb.PingRequest{}
			resp, err := c.chainTn.Ping(ctx, member.Address, req)
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
			} else {
				c.log.ErrorContext(
					ctx,
					"chain member heartbeat failed",
					"error",
					err.Error(),
					"member-id",
					member.ID,
					"member-address",
					member.Address,
				)
				select {
				case c.failedChainMemberCh <- struct{}{}:
				default:
				}
			}
		}()
	}

	wg.Wait()
}
