package coordinator

import (
	"context"
	"net"
	"time"

	"github.com/hashicorp/raft"
	"github.com/jmsadair/keychain/chain"
)

const defaultApplyTimeout = 500 * time.Millisecond

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

type RaftBackend struct {
	raft *raft.Raft
	fsm  *FSM
}

func NewRaftBackend(raft *raft.Raft, fsm *FSM) *RaftBackend {
	return &RaftBackend{raft: raft, fsm: fsm}
}

func (rb *RaftBackend) AddChainMember(ctx context.Context, member net.Addr) (*chain.Configuration, error) {
	op := &AddMemberOperation{Member: member}
	result, err := rb.apply(ctx, op)
	if err != nil {
		return nil, err
	}
	return result.(*chain.Configuration), nil
}

func (rb *RaftBackend) RemoveChainMember(ctx context.Context, member net.Addr) (*chain.Configuration, error) {
	op := &RemoveMemberOperation{Member: member}
	result, err := rb.apply(ctx, op)
	if err != nil {
		return nil, err
	}
	return result.(*chain.Configuration), nil
}

func (rb *RaftBackend) ReadChainConfiguration(ctx context.Context) (*chain.Configuration, error) {
	op := &ReadMembershipOperation{}
	result, err := rb.apply(ctx, op)
	if err != nil {
		return nil, err
	}
	return result.(*chain.Configuration), nil
}

func (rb *RaftBackend) LeadershipCh() <-chan bool {
	return rb.raft.LeaderCh()
}

func (rb *RaftBackend) ChainConfiguration() *chain.Configuration {
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
