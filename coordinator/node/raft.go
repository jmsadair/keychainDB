package node

import (
	"context"
	"time"

	hraft "github.com/hashicorp/raft"
	chainnode "github.com/jmsadair/keychain/chain/node"
)

const defaultApplyTimeout = 1 * time.Millisecond

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

type ChainConfigurationReader interface {
	ChainConfiguration() *chainnode.Configuration
}

type Status struct {
	Members map[string]string
	Leader  string
}

type RaftBackend struct {
	chainConfigReader ChainConfigurationReader
	consensus         *hraft.Raft
}

func NewRaftBackend(consensus *hraft.Raft, chainConfigReader ChainConfigurationReader) *RaftBackend {
	return &RaftBackend{consensus: consensus, chainConfigReader: chainConfigReader}
}

func (r *RaftBackend) AddMember(ctx context.Context, id, address string) (*chainnode.Configuration, error) {
	op := &AddMemberOperation{ID: id, Address: address}
	applied, err := r.apply(ctx, op)
	if err != nil {
		return nil, handleError(err)
	}
	opResult := applied.(*AddMemberResult)
	return opResult.Config, nil
}

func (r *RaftBackend) RemoveMember(ctx context.Context, id string) (*chainnode.Configuration, *chainnode.ChainMember, error) {
	op := &RemoveMemberOperation{ID: id}
	applied, err := r.apply(ctx, op)
	if err != nil {
		return nil, nil, handleError(err)
	}
	opResult := applied.(*RemoveMemberResult)
	return opResult.Config, opResult.Removed, nil
}

func (r *RaftBackend) GetMembers(ctx context.Context) (*chainnode.Configuration, error) {
	op := &ReadMembershipOperation{}
	applied, err := r.apply(ctx, op)
	if err != nil {
		return nil, handleError(err)
	}
	opResult := applied.(*ReadMembershipResult)
	return opResult.Config, nil
}

func (r *RaftBackend) JoinCluster(ctx context.Context, nodeID string, address string) error {
	configFuture := r.consensus.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return handleError(err)
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == hraft.ServerID(nodeID) || srv.Address == hraft.ServerAddress(address) {
			if srv.Address == hraft.ServerAddress(address) && srv.ID == hraft.ServerID(nodeID) {
				return nil
			}
			return ErrNodeExists
		}
	}

	indexFuture := r.consensus.AddVoter(hraft.ServerID(nodeID), hraft.ServerAddress(address), 0, timeoutFromContext(ctx, defaultApplyTimeout))
	return handleError(indexFuture.Error())
}

func (r *RaftBackend) RemoveFromCluster(ctx context.Context, nodeID string) error {
	configFuture := r.consensus.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return handleError(err)
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == hraft.ServerID(nodeID) {
			indexFuture := r.consensus.RemoveServer(hraft.ServerID(nodeID), 0, timeoutFromContext(ctx, defaultApplyTimeout))
			return handleError(indexFuture.Error())
		}
	}

	return nil
}

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

func (r *RaftBackend) LeadershipCh() <-chan bool {
	return r.consensus.LeaderCh()
}

func (r *RaftBackend) LeaderCh() <-chan bool {
	return r.consensus.LeaderCh()
}

func (r *RaftBackend) ChainConfiguration() *chainnode.Configuration {
	return r.chainConfigReader.ChainConfiguration()
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
