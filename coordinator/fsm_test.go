package coordinator

import (
	"io"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/jmsadair/keychainDB/chain"
	"github.com/stretchr/testify/require"
)

func TestNewFSM(t *testing.T) {
	fsm := NewFSM()
	require.NotNil(t, fsm)
	require.Equal(t, chain.EmptyChain, fsm.configuration)
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
	require.True(t, chain.EmptyChain.Equal(readMembershipOpResult.Config))

	addOp := &AddMemberOperation{ID: memberID, Address: memberAddr}
	addOpBytes, err := addOp.Bytes()
	require.NoError(t, err)
	log = raft.Log{Data: addOpBytes}
	addMemberOpResult, ok := fsm.Apply(&log).(*AddMemberResult)
	require.True(t, ok)
	require.True(t, addMemberOpResult.Config.Equal(
		chain.NewConfiguration([]*chain.ChainMember{{ID: memberID, Address: memberAddr}}, 1),
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
		chain.NewConfiguration([]*chain.ChainMember{}, 2),
	))
}

func TestSnapshotRestore(t *testing.T) {
	fsm := NewFSM()
	member := &chain.ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	config := chain.NewConfiguration([]*chain.ChainMember{member}, 0)

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
