package node

import (
	"context"
	"os"
	"time"

	"github.com/hashicorp/raft"
	chainnode "github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/coordinator/raftnet"
	"github.com/jmsadair/keychain/coordinator/raftstore"
	"google.golang.org/grpc"
)

const (
	// The default timeout for applying an operation to the raft cluster
	// if no other timeout is provided.
	defaultApplyTimeout = 1 * time.Second
	// The maximum number of snapshots to retain.
	numSnapshotsToRetain = 10
)

// timeoutFromContext derives a timeout from the context deadline if it has one otherwise
// it will return the provided default timeout.
func timeoutFromContext(ctx context.Context, defaultTimeout time.Duration) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 {
			return remaining
		}
	}
	return defaultTimeout
}

type operation interface {
	Bytes() ([]byte, error)
}

// Status represents the status of the raft cluster.
type Status struct {
	// Maps member ID to member address.
	Members map[string]string
	// The current leader of the cluster. This value is empty if there is no leader.
	Leader string
}

// RaftBackend provides an API for the coordinator to interact with the raft cluster.
type RaftBackend struct {
	// The ID of the raft node.
	ID string
	// The advertised address of the raft node.
	Address string
	// Network layer used for cluster communication.
	nw *raftnet.Network
	// The underlying consensus mechanism.
	consensus *raft.Raft
	// The state that is replicated across the cluster.
	fsm *FSM
	// Storage for logs.
	store *logstore.PersistentStorage
	// Storage for snapshots.
	snapshotStore *raft.FileSnapshotStore
}

// NewRaftBackend creates a new raft backend.
func NewRaftBackend(id string, address string, storageDir string, dialOpts ...grpc.DialOption) (*RaftBackend, error) {
	store, err := logstore.NewPersistentStorage(storageDir)
	if err != nil {
		return nil, err
	}
	snapshotStore, err := raft.NewFileSnapshotStore(storageDir, numSnapshotsToRetain, os.Stderr)
	if err != nil {
		return nil, err
	}

	netLayer, err := raftnet.NewNetwork(address, dialOpts...)
	if err != nil {
		return nil, err
	}
	fsm := NewFSM()
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(id)
	consensus, err := raft.NewRaft(raftCfg, fsm, store, store, snapshotStore, netLayer.Transport())
	if err != nil {
		return nil, err
	}

	return &RaftBackend{
		ID:            id,
		Address:       address,
		nw:            netLayer,
		consensus:     consensus,
		store:         store,
		snapshotStore: snapshotStore,
		fsm:           fsm,
	}, nil
}

// Register registers this node with a gRPC server.
func (r *RaftBackend) Register(s grpc.ServiceRegistrar) {
	r.nw.Register(s)
}

// Bootstrap is used to bootstrap a raft cluster.
func (r *RaftBackend) Bootstrap(config raft.Configuration) error {
	future := r.consensus.BootstrapCluster(config)
	return future.Error()
}

// Shutdown shuts down this node.
func (r *RaftBackend) Shutdown() error {
	defer r.store.Close()
	future := r.consensus.Shutdown()
	return future.Error()
}

// AddMember adds a new chain node to the chain configuration.
func (r *RaftBackend) AddMember(ctx context.Context, id, address string) (*chainnode.Configuration, error) {
	op := &AddMemberOperation{ID: id, Address: address}
	applied, err := r.apply(ctx, op)
	if err != nil {
		return nil, handleError(err)
	}
	opResult := applied.(*AddMemberResult)
	return opResult.Config, nil
}

// RemoveMember removes a chain node from the chain configuration.
func (r *RaftBackend) RemoveMember(ctx context.Context, id string) (*chainnode.Configuration, *chainnode.ChainMember, error) {
	op := &RemoveMemberOperation{ID: id}
	applied, err := r.apply(ctx, op)
	if err != nil {
		return nil, nil, handleError(err)
	}
	opResult := applied.(*RemoveMemberResult)
	return opResult.Config, opResult.Removed, nil
}

// GetMembers is used to read the chain configuration. This operation requires cluster quorum.
// If strong consistency is not required, then ChainConfiguration should be used instead.
func (r *RaftBackend) GetMembers(ctx context.Context) (*chainnode.Configuration, error) {
	op := &ReadMembershipOperation{}
	applied, err := r.apply(ctx, op)
	if err != nil {
		return nil, handleError(err)
	}
	opResult := applied.(*ReadMembershipResult)
	return opResult.Config, nil
}

// JoinCluster is used to add a node to the raft cluster.
func (r *RaftBackend) JoinCluster(ctx context.Context, nodeID string, address string) error {
	configFuture := r.consensus.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return handleError(err)
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(address) {
			if srv.Address == raft.ServerAddress(address) && srv.ID == raft.ServerID(nodeID) {
				return nil
			}
			return ErrNodeExists
		}
	}

	indexFuture := r.consensus.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(address), 0, timeoutFromContext(ctx, defaultApplyTimeout))
	return handleError(indexFuture.Error())
}

// RemoveFromCluster is used to remove a node from the raft cluster.
func (r *RaftBackend) RemoveFromCluster(ctx context.Context, nodeID string) error {
	configFuture := r.consensus.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return handleError(err)
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) {
			indexFuture := r.consensus.RemoveServer(raft.ServerID(nodeID), 0, timeoutFromContext(ctx, defaultApplyTimeout))
			return handleError(indexFuture.Error())
		}
	}

	return nil
}

// ClusterStatus returns the status of the raft cluster. The status includes the members and leader if there is one.
func (r *RaftBackend) ClusterStatus() (Status, error) {
	_, leaderID := r.consensus.LeaderWithID()

	configFuture := r.consensus.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return Status{}, handleError(err)
	}
	nodeIDToAddr := make(map[string]string, len(configFuture.Configuration().Servers))
	for _, srv := range configFuture.Configuration().Servers {
		nodeIDToAddr[string(srv.ID)] = string(srv.Address)
	}

	return Status{Members: nodeIDToAddr, Leader: string(leaderID)}, nil
}

// LeaderCh is used to get a channel which delivers signals on acquiring or losing leadership.
// It sends true if this node gains leadership and false if it loses leadership.
func (r *RaftBackend) LeaderCh() <-chan bool {
	return r.consensus.LeaderCh()
}

// LeaderWithID is used to return the current leader address and ID of the cluster.
// It may return empty strings if there is no current leader or the leader is unknown.
func (r *RaftBackend) LeaderWithID() (string, string) {
	addr, id := r.consensus.LeaderWithID()
	return string(addr), string(id)
}

// ChainConfiguration is used to read the current chain configuration from this nodes perspective.
// This operation does not require quorum hence the value read may be stale
func (r *RaftBackend) ChainConfiguration() *chainnode.Configuration {
	return r.fsm.ChainConfiguration()
}

func (r *RaftBackend) apply(ctx context.Context, op operation) (any, error) {
	b, err := op.Bytes()
	if err != nil {
		return nil, err
	}
	future := r.consensus.Apply(b, timeoutFromContext(ctx, defaultApplyTimeout))
	if err := future.Error(); err != nil {
		return nil, err
	}
	return future.Response(), nil
}
