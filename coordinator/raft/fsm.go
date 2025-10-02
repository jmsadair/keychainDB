package raft

import (
	"io"
	"sync"

	"github.com/hashicorp/raft"
	chainnode "github.com/jmsadair/keychain/chain/node"
	pb "github.com/jmsadair/keychain/proto/coordinator"
	"google.golang.org/protobuf/proto"
)

type AddMemberOperation struct {
	ID      string
	Address string
}

type AddMemberResult struct {
	Config *chainnode.Configuration
}

func (op *AddMemberOperation) Bytes() ([]byte, error) {
	opProto := &pb.ReplicatedOperation{
		Operation: &pb.ReplicatedOperation_AddMember{
			AddMember: &pb.AddMemberOperation{
				Id:      op.ID,
				Address: op.Address,
			},
		},
	}
	return proto.Marshal(opProto)
}

type RemoveMemberOperation struct {
	ID string
}

type RemoveMemberResult struct {
	Config  *chainnode.Configuration
	Removed *chainnode.ChainMember
}

func (op *RemoveMemberOperation) Bytes() ([]byte, error) {
	opProto := &pb.ReplicatedOperation{
		Operation: &pb.ReplicatedOperation_RemoveMember{
			RemoveMember: &pb.RemoveMemberOperation{
				Id: op.ID,
			},
		},
	}
	return proto.Marshal(opProto)
}

type ReadMembershipOperation struct{}

type ReadMembershipResult struct {
	Config *chainnode.Configuration
}

func (op *ReadMembershipOperation) Bytes() ([]byte, error) {
	opProto := &pb.ReplicatedOperation{
		Operation: &pb.ReplicatedOperation_ReadMembership{
			ReadMembership: &pb.ReadMembershipOperation{},
		},
	}
	return proto.Marshal(opProto)
}

type Snapshot struct {
	Configuration *chainnode.Configuration
}

func NewSnapshot(config *chainnode.Configuration) *Snapshot {
	return &Snapshot{Configuration: config}
}

func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	b, err := s.Configuration.Bytes()
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
	configuration *chainnode.Configuration
	mu            sync.Mutex
}

func NewFSM() *FSM {
	return &FSM{configuration: chainnode.EmptyChain}
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
		f.configuration = f.configuration.AddMember(v.AddMember.GetId(), v.AddMember.GetAddress())
		return &AddMemberResult{Config: f.configuration.Copy()}
	case *pb.ReplicatedOperation_RemoveMember:
		removed := f.configuration.Member(v.RemoveMember.GetId())
		f.configuration = f.configuration.RemoveMember(v.RemoveMember.GetId())
		return &RemoveMemberResult{Config: f.configuration.Copy(), Removed: removed}
	}

	return &ReadMembershipResult{Config: f.configuration.Copy()}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return NewSnapshot(f.configuration.Copy()), nil
}

func (f *FSM) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()
	b, err := io.ReadAll(snapshot)
	if err != nil {
		return err
	}
	config, err := chainnode.NewConfigurationFromBytes(b)
	if err != nil {
		return err
	}

	f.mu.Lock()
	f.configuration = config
	f.mu.Unlock()

	return nil
}

func (f *FSM) ChainConfiguration() *chainnode.Configuration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.configuration.Copy()
}
