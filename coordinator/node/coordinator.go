package node

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	chainnode "github.com/jmsadair/keychainDB/chain/node"
	chainpb "github.com/jmsadair/keychainDB/proto/chain"
	pb "github.com/jmsadair/keychainDB/proto/coordinator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// ErrConfigurationUpdateFailed is returned when a member is added or removed from the chain configuration
// but the coordinator was unable to apply it to all chain nodes.
var ErrConfigurationUpdateFailed = errors.New("coordinator: failed to update configuration for chain member")

const (
	heartbeatInterval   = 250 * time.Millisecond
	chainFailureTimeout = 5 * time.Second
)

// ChainClient defines the interface for chain node communication.
type ChainClient interface {
	// UpdateConfiguration sends the appropriate RPC to the target node.
	UpdateConfiguration(ctx context.Context, address string, request *chainpb.UpdateConfigurationRequest) (*chainpb.UpdateConfigurationResponse, error)
	// Ping sends the appropriate RPC to the target node.
	Ping(ctx context.Context, address string, request *chainpb.PingRequest) (*chainpb.PingResponse, error)
	// Close closes all connections managed by this client.
	Close() error
}

// RaftProtocol defines the interface for interacting with the underlying consensus protocol.
type RaftProtocol interface {
	// AddMember is used to add a new member to the chain membership configuration.
	AddMember(ctx context.Context, id, address string) (*chainnode.Configuration, error)
	// RemoveMember is used to remove a member from the chain membership configuration.
	RemoveMember(ctx context.Context, id string) (*chainnode.Configuration, *chainnode.ChainMember, error)
	// GetMembers is used to list the members of the chain membership configuration.
	GetMembers(ctx context.Context) (*chainnode.Configuration, error)
	// LeaderCh is used to get a channel which delivers signals on acquiring or losing leadership.
	// It sends true if this node gains leadership and false if it loses leadership.
	LeaderCh() <-chan bool
	// ChainConfiguration is used to read the local chain membership configuration this node has.
	// This operation does not require quorum. Hence, the value read may be stale
	ChainConfiguration() *chainnode.Configuration
	// JoinCluster is used to add a node to the cluster.
	JoinCluster(ctx context.Context, id, address string) error
	// RemoveFromCluster is used to remove a node from the cluster.
	RemoveFromCluster(ctx context.Context, id string) error
	// ClusterStatus returns the status of the cluster.
	ClusterStatus() (Status, error)
	// Shutdown shuts down the protocol.
	Shutdown() error
}

type memberState struct {
	lastContact   time.Time
	status        chainnode.Status
	configVersion uint64
}

// Coordinator is a node that manages the chain configuration.
// It is responsible for managing the chain membership configuration.
type Coordinator struct {
	pb.UnimplementedCoordinatorServiceServer
	ID                  string
	Address             string
	raft                RaftProtocol
	chainClient         ChainClient
	memberStates        map[string]*memberState
	isLeader            bool
	leadershipChangeCh  <-chan bool
	failedChainMemberCh chan any
	configSyncCh        chan any
	log                 *slog.Logger
	mu                  sync.Mutex
}

// NewCoordinator creates a new coordinator node.
func NewCoordinator(
	id string,
	address string,
	chainClient ChainClient,
	raft RaftProtocol,
	log *slog.Logger,
) *Coordinator {
	return &Coordinator{
		Address:             address,
		ID:                  id,
		raft:                raft,
		chainClient:         chainClient,
		leadershipChangeCh:  raft.LeaderCh(),
		memberStates:        make(map[string]*memberState),
		failedChainMemberCh: make(chan any),
		configSyncCh:        make(chan any),
		log:                 log.With("local-id", id),
	}
}

// Run runs this node.
func (c *Coordinator) Run(ctx context.Context) {
	defer c.chainClient.Close()
	defer c.raft.Shutdown()

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		c.heartbeatLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		c.failedChainMemberLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		c.leadershipChangeLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		c.configSyncLoop(ctx)
	}()

	<-ctx.Done()
	wg.Wait()
}

// AddMember is used to add a chain node to a chain. The chain node must be reachable at the address provicded in
// the request and must be ready to start serving RPCs immediately. It is possible that the configuration with the
// new member is successfully replicated across the coordinator cluster but the update is not successfully applied
// to all nodes in the chain. An error will be returned in this case, but the coordinator will continue to attempt
// to apply the configuration update until all nodes have it.
func (c *Coordinator) AddMember(ctx context.Context, request *pb.AddMemberRequest) (*pb.AddMemberResponse, error) {
	config, err := c.raft.AddMember(ctx, request.GetId(), request.GetAddress())
	if err != nil {
		return nil, err
	}
	return &pb.AddMemberResponse{}, c.updateChainMemberConfigurations(ctx, config, nil)
}

// RemoveMember is used to remove a chain node from a chain. It is possible that the configuration with the
// member removed is successfully replicated across the coordinator cluster but the update is not successfully applied
// to all nodes in the chain. An error will be returned in this case, but the coordinator will continue to attempt
// to apply the configuration update until all nodes have it.
func (c *Coordinator) RemoveMember(ctx context.Context, request *pb.RemoveMemberRequest) (*pb.RemoveMemberResponse, error) {
	config, removed, err := c.raft.RemoveMember(ctx, request.GetId())
	if err != nil {
		return nil, err
	}
	return &pb.RemoveMemberResponse{}, c.updateChainMemberConfigurations(ctx, config, removed)
}

// GetMembers is used to list the members of a chain.
func (c *Coordinator) GetMembers(ctx context.Context, request *pb.GetMembersRequest) (*pb.GetMembersResponse, error) {
	config, err := c.raft.GetMembers(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.GetMembersResponse{Configuration: config.Proto()}, nil
}

// JoinCluster is used to add a coordinator to the cluster. The node must be reachable at the address provided in
// the request and must be ready to start serving RPCs immediately. There must not be a node with the same address
// or the same ID that is already a member of the cluster.
func (c *Coordinator) JoinCluster(ctx context.Context, request *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
	if err := c.raft.JoinCluster(ctx, request.GetId(), request.GetAddress()); err != nil {
		return nil, err
	}
	return &pb.JoinClusterResponse{}, nil
}

// RemoveFromCluster is used to remove a coordinator from the cluster.
func (c *Coordinator) RemoveFromCluster(ctx context.Context, request *pb.RemoveFromClusterRequest) (*pb.RemoveFromClusterResponse, error) {
	if err := c.raft.RemoveFromCluster(ctx, request.GetId()); err != nil {
		return nil, err
	}
	return &pb.RemoveFromClusterResponse{}, nil
}

// ClusterStatus is used to get the current members of the cluster and the leader.
func (c *Coordinator) ClusterStatus(ctx context.Context, request *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	status, err := c.raft.ClusterStatus()
	if err != nil {
		return nil, err
	}
	return &pb.ClusterStatusResponse{Leader: status.Leader, Members: status.Members}, nil
}

// Check implements the gRPC health checking protocol.
func (c *Coordinator) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// Watch implements the gRPC health checking protocol.
func (c *Coordinator) Watch(req *grpc_health_v1.HealthCheckRequest, _ grpc_health_v1.Health_WatchServer) error {
	return status.Error(codes.Unimplemented, "unimplemented")
}

// updateChainMemberConfigurations is used to apply a new configuration to the nodes in the chain.
// This function will return an error if the configuration is not applied to one or more members of the chain.
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
			_, err := c.chainClient.UpdateConfiguration(ctx, member.Address, req)
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

// heartbeatLoop runs in the background and sends heartbeats to the members of the chain.
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

// failedChainMemberLoop runs in the background and is responsible for removing members of the chain
// that the coordinator has been unable to contact.
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

// leadershipChangeLoop runs in the background and listens for cluster leadership changes.
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

// configSyncLoop runs in the background and applies configuration updates to chain members that have
// an out-of-date configuration.
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

// onConfigSync attempts to apply the current configuration to chain nodes that
// have an out-of-date configuration.
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

	// Identify the members of the chain that have a stale configuration.
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

	// Attempt to push a configuration update to those nodes.
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
			c.chainClient.UpdateConfiguration(ctx, member.Address, req)
		}()
	}

	wg.Wait()
}

// onLeadershipChange clears the locally stored state used to track the status of chain nodes.
func (c *Coordinator) onLeadershipChange(isLeader bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.isLeader = isLeader
	c.memberStates = map[string]*memberState{}
}

// onFailedChainMember will remove chain nodes from the chain if they have been unreachable
// for a predetermined amount of time.
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

// onHeartbeat sends a heartbeat to all members of the chain.
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
			resp, err := c.chainClient.Ping(ctx, member.Address, req)

			if err == nil {
				c.mu.Lock()
				state, ok := c.memberStates[member.ID]
				if ok {
					state.lastContact = time.Now()
					state.configVersion = resp.GetConfigVersion()
					state.status = chainnode.Status(resp.GetStatus())
				}
				c.mu.Unlock()

				// Notify the configuration sync loop that a node potentially
				// has an out-of-date configuration. This is handled in a separate
				// routine to avoid blocking heartbeats.
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

				// If the heartbeat failed, notify the failed chain member loop
				// that this member may need to be removed. This is handled in a
				// separate routine to avoid blocking heartbeats.
				select {
				case c.failedChainMemberCh <- struct{}{}:
				default:
				}
			}
		}()
	}

	wg.Wait()
}
