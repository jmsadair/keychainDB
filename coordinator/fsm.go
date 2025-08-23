package coordinator

import (
	"io"
	"net"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/jmsadair/keychain/chain"
	pb "github.com/jmsadair/keychain/proto/pbcoordinator"
	"google.golang.org/protobuf/proto"
)

type MembershipChangeOpType int

const (
	Add MembershipChangeOpType = iota
	Remove
)

type MembershipChangeOperation struct {
	Member net.Addr
	OpType MembershipChangeOpType
}

func NewMembershipChangeOperationFromBytes(b []byte) (*MembershipChangeOperation, error) {
	pbOp := &pb.MembershipChangeOperation{}
	if err := proto.Unmarshal(b, pbOp); err != nil {
		return nil, err
	}
	member, err := net.ResolveTCPAddr("tcp", pbOp.GetMember())
	if err != nil {
		return nil, err
	}

	op := &MembershipChangeOperation{Member: member}
	switch pbOp.GetOp() {
	case pb.MembershipChangeOpType_ADD:
		op.OpType = Add
	case pb.MembershipChangeOpType_REMOVE:
		op.OpType = Remove
	}

	return op, nil
}

func (r *MembershipChangeOperation) Bytes() ([]byte, error) {
	op := &pb.MembershipChangeOperation{Member: r.Member.String()}
	switch r.OpType {
	case Add:
		op.Op = pb.MembershipChangeOpType_ADD
	case Remove:
		op.Op = pb.MembershipChangeOpType_REMOVE
	}
	return proto.Marshal(op)
}

func (r *MembershipChangeOperation) Apply(config *chain.ChainConfiguration) *chain.ChainConfiguration {
	switch r.OpType {
	case Add:
		return config.AddMember(r.Member)
	case Remove:
		return config.RemoveMember(r.Member)
	}
	return nil
}

type Snapshot struct {
	ChainConfiguration *chain.ChainConfiguration
}

func NewSnapshot(config *chain.ChainConfiguration) *Snapshot {
	return &Snapshot{ChainConfiguration: config}
}

func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	b, err := s.ChainConfiguration.Bytes()
	if err != nil {
		sink.Cancel()
		return err
	}
	_, err = sink.Write(b)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *Snapshot) Release() {}

type FSM struct {
	chainConfiguration *chain.ChainConfiguration
	mu                 sync.Mutex
}

func NewFSM() *FSM {
	return &FSM{chainConfiguration: chain.EmptyChain}
}

func (f *FSM) Apply(log *raft.Log) any {
	op, err := NewMembershipChangeOperationFromBytes(log.Data)
	if err != nil {
		panic(err)
	}

	f.mu.Lock()
	f.chainConfiguration = op.Apply(f.chainConfiguration)
	f.mu.Unlock()

	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return NewSnapshot(f.chainConfiguration.Copy()), nil
}

func (f *FSM) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()
	b, err := io.ReadAll(snapshot)
	if err != nil {
		return err
	}
	config, err := chain.NewChainConfigurationFromBytes(b)
	if err != nil {
		return err
	}

	f.mu.Lock()
	f.chainConfiguration = config
	f.mu.Unlock()

	return nil
}
