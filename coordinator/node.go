package coordinator

import (
	"context"
	"net"
	"time"

	"github.com/hashicorp/raft"
	"github.com/jmsadair/keychain/chain"
)

type Coordinator struct {
	address  net.Addr
	protocol *raft.Raft
}

func NewCoordinator(address net.Addr, protocol *raft.Raft) *Coordinator {
	return &Coordinator{address: address, protocol: protocol}
}

func (c *Coordinator) AddMember(ctx context.Context, member net.Addr) error {
	op := &AddMemberOperation{Member: member}
	b, err := op.Bytes()
	if err != nil {
		return err
	}
	future := c.protocol.Apply(b, 1*time.Second)
	return future.Error()
}

func (c *Coordinator) RemoveMember(ctx context.Context, member net.Addr) error {
	op := &RemoveMemberOperation{Member: member}
	b, err := op.Bytes()
	if err != nil {
		return err
	}
	future := c.protocol.Apply(b, 1*time.Second)
	return future.Error()
}

func (c *Coordinator) ReadMembershipConfiguration(ctx context.Context) (*chain.ChainConfiguration, error) {
	op := &ReadMembershipOperation{}
	b, err := op.Bytes()
	if err != nil {
		return nil, err
	}
	future := c.protocol.Apply(b, 1*time.Second)
	if err := future.Error(); err != nil {
		return nil, err
	}
	return future.Response().(*chain.ChainConfiguration), nil
}
