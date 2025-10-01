package raft

import (
	"io"
	"testing"

	"github.com/hashicorp/raft"
	chainnode "github.com/jmsadair/keychain/chain/node"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockSnapshotReader struct {
	mock.Mock
}

func (m *mockSnapshotReader) Read(p []byte) (n int, err error) {
	args := m.MethodCalled("Read")
	data := args.Get(0).([]byte)
	return copy(p, data), args.Error(1)
}

func (m *mockSnapshotReader) Close() error {
	args := m.MethodCalled("Close")
	return args.Error(0)
}

type mockSnapshotSync struct {
	mock.Mock
}

func (m *mockSnapshotSync) ID() string {
	args := m.MethodCalled("ID")
	return args.String(0)
}

func (m *mockSnapshotSync) Write(p []byte) (n int, err error) {
	args := m.MethodCalled("Write", p)
	return args.Int(0), args.Error(1)
}

func (m *mockSnapshotSync) Close() error {
	args := m.MethodCalled("Close")
	return args.Error(0)
}

func (m *mockSnapshotSync) Cancel() error {
	args := m.MethodCalled("Cancel")
	return args.Error(0)
}

func TestNewFSM(t *testing.T) {
	fsm := NewFSM()
	require.NotNil(t, fsm)
	require.Equal(t, chainnode.EmptyChain, fsm.configuration)
}

func TestApply(t *testing.T) {
	fsm := NewFSM()
	memberID := "member-1"
	memberAddr := "127.0.0.1:8080"

	readMembershipOp := &ReadMembershipOperation{}
	readMembershipOpBytes, err := readMembershipOp.Bytes()
	require.NoError(t, err)
	log := raft.Log{Data: readMembershipOpBytes}
	readMembershipOpResult, ok := fsm.Apply(&log).(*ReadMembershipResult)
	require.True(t, ok)
	require.True(t, chainnode.EmptyChain.Equal(readMembershipOpResult.Config))

	addOp := &AddMemberOperation{ID: memberID, Address: memberAddr}
	addOpBytes, err := addOp.Bytes()
	require.NoError(t, err)
	log = raft.Log{Data: addOpBytes}
	addMemberOpResult, ok := fsm.Apply(&log).(*AddMemberResult)
	require.True(t, ok)
	require.True(t, addMemberOpResult.Config.Equal(
		chainnode.NewConfiguration([]*chainnode.ChainMember{{ID: memberID, Address: memberAddr}}, 1),
	))

	removeOp := &RemoveMemberOperation{ID: memberID}
	removeOpBytes, err := removeOp.Bytes()
	require.NoError(t, err)
	log = raft.Log{Data: removeOpBytes}
	removeMemberOpResult, ok := fsm.Apply(&log).(*RemoveMemberResult)
	require.True(t, ok)
	require.NotNil(t, removeMemberOpResult.Removed)
	require.Equal(t, memberID, removeMemberOpResult.Removed.ID)
	require.Equal(t, memberAddr, removeMemberOpResult.Removed.Address)
	require.True(t, removeMemberOpResult.Config.Equal(
		chainnode.NewConfiguration([]*chainnode.ChainMember{}, 2),
	))
}

func TestSnapshotRestore(t *testing.T) {
	fsm := NewFSM()
	member := &chainnode.ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	config := chainnode.NewConfiguration([]*chainnode.ChainMember{member}, 0)

	// Create a snapshot from the FSM state and ensure its encoded state is correct.
	fsm.configuration = config
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)
	b, err := config.Bytes()
	require.NoError(t, err)
	snapshotSink := new(mockSnapshotSync)
	snapshotSink.On("Write", b).Return(len(b), nil).Once()
	snapshotSink.On("Close").Return(nil).Once()
	require.NoError(t, snapshot.Persist(snapshotSink))
	snapshotSink.AssertExpectations(t)

	// Restore the FSM from a snapshot and ensure its state is correct.
	fsm.configuration = nil
	snapshotReader := new(mockSnapshotReader)
	snapshotReader.On("Read").Return(b, io.EOF).Once()
	snapshotReader.On("Close").Return(nil).Once()
	require.NoError(t, fsm.Restore(snapshotReader))
	require.True(t, config.Equal(fsm.configuration))
	snapshotReader.AssertExpectations(t)
}
