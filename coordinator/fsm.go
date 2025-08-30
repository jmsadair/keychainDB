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

type AddMemberOperation struct {
	Member net.Addr
}

func (op *AddMemberOperation) Bytes() ([]byte, error) {
	opProto := &pb.ReplicatedOperation{
		Operation: &pb.ReplicatedOperation_AddMember{
			AddMember: &pb.AddMemberOperation{
				Member: op.Member.String(),
			},
		},
	}
	return proto.Marshal(opProto)
}

type RemoveMemberOperation struct {
	Member net.Addr
}

func (op *RemoveMemberOperation) Bytes() ([]byte, error) {
	opProto := &pb.ReplicatedOperation{
		Operation: &pb.ReplicatedOperation_RemoveMember{
			RemoveMember: &pb.RemoveMemberOperation{
				Member: op.Member.String(),
			},
		},
	}
	return proto.Marshal(opProto)
}

type ReadMembershipOperation struct{}

func (op *ReadMembershipOperation) Bytes() ([]byte, error) {
	opProto := &pb.ReplicatedOperation{
		Operation: &pb.ReplicatedOperation_ReadMembership{
			ReadMembership: &pb.ReadMembershipOperation{},
		},
	}
	return proto.Marshal(opProto)
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
	op := &pb.ReplicatedOperation{}
	if err := proto.Unmarshal(log.Data, op); err != nil {
		panic(err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	switch v := op.Operation.(type) {
	case *pb.ReplicatedOperation_AddMember:
		member, err := net.ResolveTCPAddr("tcp", v.AddMember.GetMember())
		if err != nil {
			panic(err)
		}
		f.chainConfiguration = f.chainConfiguration.AddMember(member)
	case *pb.ReplicatedOperation_RemoveMember:
		member, err := net.ResolveTCPAddr("tcp", v.RemoveMember.GetMember())
		if err != nil {
			panic(err)
		}
		f.chainConfiguration = f.chainConfiguration.RemoveMember(member)
	}

	return f.chainConfiguration.Copy()
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
