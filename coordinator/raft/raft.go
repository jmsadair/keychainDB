package raft

import (
	"context"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
	chainnode "github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/coordinator/storage"
)

const (
	defaultApplyTimeout  = 500 * time.Millisecond
	transportTimeout     = 10 * time.Second
	maxPool              = 5
	numSnapshotsToRetain = 10
)

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

type Status struct {
	Members map[string]string
	Leader  string
}

type RaftBackend struct {
	ID               string
	AdvertiseAddress string
	BindAddress      string
	raft             *raft.Raft
	fsm              *FSM
	store            *storage.PersistentStorage
}

func NewRaftBackend(
	id string,
	bindAddr string,
	advertiseAddr string,
	storePath string,
	snapshotStorePath string,
	bootstrap bool,
) (*RaftBackend, error) {
	store, err := storage.NewPersistentStorage(storePath)
	if err != nil {
		return nil, err
	}
	advertise, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		return nil, err
	}
	tn, err := raft.NewTCPTransport(bindAddr, advertise, maxPool, transportTimeout, os.Stderr)
	if err != nil {
		return nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(snapshotStorePath, numSnapshotsToRetain, os.Stderr)
	if err != nil {
		return nil, err
	}

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(id)
	fsm := NewFSM()
	r, err := raft.NewRaft(config, fsm, store, store, snapshotStore, tn)
	if err != nil {
		return nil, err
	}

	if bootstrap {
		clusterConfig := raft.Configuration{Servers: []raft.Server{{ID: config.LocalID, Address: tn.LocalAddr()}}}
		future := r.BootstrapCluster(clusterConfig)
		if err := future.Error(); err != nil {
			return nil, err
		}
	}

	return &RaftBackend{ID: id, AdvertiseAddress: advertiseAddr, BindAddress: bindAddr, raft: r, fsm: fsm, store: store}, nil
}

func (rb *RaftBackend) Shutdown() error {
	defer rb.store.Close()
	future := rb.raft.Shutdown()
	return future.Error()
}

func (rb *RaftBackend) AddMember(ctx context.Context, id, address string) (*chainnode.Configuration, error) {
	op := &AddMemberOperation{ID: id, Address: address}
	applied, err := rb.apply(ctx, op)
	if err != nil {
		return nil, handleError(err)
	}
	opResult := applied.(*AddMemberResult)
	return opResult.Config, nil
}

func (rb *RaftBackend) RemoveMember(ctx context.Context, id string) (*chainnode.Configuration, *chainnode.ChainMember, error) {
	op := &RemoveMemberOperation{ID: id}
	applied, err := rb.apply(ctx, op)
	if err != nil {
		return nil, nil, handleError(err)
	}
	opResult := applied.(*RemoveMemberResult)
	return opResult.Config, opResult.Removed, nil
}

func (rb *RaftBackend) GetMembers(ctx context.Context) (*chainnode.Configuration, error) {
	op := &ReadMembershipOperation{}
	applied, err := rb.apply(ctx, op)
	if err != nil {
		return nil, handleError(err)
	}
	opResult := applied.(*ReadMembershipResult)
	return opResult.Config, nil
}

func (rb *RaftBackend) JoinCluster(ctx context.Context, nodeID string, address string) error {
	configFuture := rb.raft.GetConfiguration()
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

	indexFuture := rb.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(address), 0, timeoutFromContext(ctx, defaultApplyTimeout))
	return handleError(indexFuture.Error())
}

func (rb *RaftBackend) RemoveFromCluster(ctx context.Context, nodeID string) error {
	configFuture := rb.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return handleError(err)
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) {
			indexFuture := rb.raft.RemoveServer(raft.ServerID(nodeID), 0, timeoutFromContext(ctx, defaultApplyTimeout))
			return handleError(indexFuture.Error())
		}
	}

	return nil
}

func (rb *RaftBackend) ClusterStatus() (Status, error) {
	_, leaderID := rb.raft.LeaderWithID()

	configFuture := rb.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return Status{}, handleError(err)
	}
	nodeIDToAddr := make(map[string]string, len(configFuture.Configuration().Servers))
	for _, srv := range configFuture.Configuration().Servers {
		nodeIDToAddr[string(srv.ID)] = string(srv.Address)
	}

	return Status{Members: nodeIDToAddr, Leader: string(leaderID)}, nil
}

func (rb *RaftBackend) LeadershipCh() <-chan bool {
	return rb.raft.LeaderCh()
}

func (rb *RaftBackend) LeaderCh() <-chan bool {
	return rb.raft.LeaderCh()
}

func (rb *RaftBackend) LeaderAddressAndID() (string, string) {
	addr, id := rb.raft.LeaderWithID()
	return string(addr), string(id)
}

func (rb *RaftBackend) ChainConfiguration() *chainnode.Configuration {
	return rb.fsm.ChainConfiguration()
}

func (rb *RaftBackend) apply(ctx context.Context, op operation) (any, error) {
	b, err := op.Bytes()
	if err != nil {
		return nil, err
	}
	future := rb.raft.Apply(b, timeoutFromContext(ctx, defaultApplyTimeout))
	if err := future.Error(); err != nil {
		return nil, err
	}
	return future.Response(), nil
}
