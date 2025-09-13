package node

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEqual(t *testing.T) {
	member1 := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	member2 := &ChainMember{ID: "member-2", Address: "127.0.0.2:8080"}
	member3 := &ChainMember{ID: "member-3", Address: "127.0.0.3:8080"}

	config1, err := NewConfiguration([]*ChainMember{member1, member2, member3}, 0)
	require.NoError(t, err)
	config2, err := NewConfiguration([]*ChainMember{member2, member1, member3}, 0)
	require.NoError(t, err)
	config3, err := NewConfiguration([]*ChainMember{member1, member2}, 0)
	require.NoError(t, err)
	config4, err := NewConfiguration([]*ChainMember{member1, member2, member3}, 1)
	require.NoError(t, err)
	config5, err := NewConfiguration([]*ChainMember{member1, member2, member3}, 0)
	require.NoError(t, err)

	// Same members of the chain but different order.
	require.False(t, config1.Equal(config2))
	// Same order but missing the last member of the chain.
	require.False(t, config1.Equal(config3))
	// Same members of the chain in the same order but different version.
	require.False(t, config1.Equal(config4))
	// Same members of the chain in the same order.
	require.True(t, config1.Equal(config5))
}

func TestCopy(t *testing.T) {
	member1 := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	version := uint64(1)

	config, err := NewConfiguration([]*ChainMember{member1}, version)
	require.NoError(t, err)
	require.Len(t, config.members, 1)
	require.Len(t, config.addressToMemberIndex, 1)
	require.Contains(t, config.addressToMemberIndex, member1.Address)
	require.Equal(t, member1.Address, config.members[0].Address)
	require.Equal(t, member1.ID, config.members[0].ID)

	// Create a copy of the original configuration and mutate it.
	configCopy := config.Copy()
	configCopy.addressToMemberIndex = nil
	configCopy.members = nil

	// Ensure the original configuration as not been modified.
	require.Equal(t, version, config.Version)
	require.Len(t, config.members, 1)
	require.Len(t, config.addressToMemberIndex, 1)
	require.Contains(t, config.addressToMemberIndex, member1.Address)
	require.Equal(t, member1.Address, config.members[0].Address)
	require.Equal(t, member1.ID, config.members[0].ID)
}

func TestAddMemberRemoveMember(t *testing.T) {
	config, err := NewConfiguration([]*ChainMember{}, 0)
	require.NoError(t, err)
	config = config.AddMember("member-1", "127.0.0.1:8080")
	require.True(t, config.IsMemberByID("member-1"))
	require.True(t, config.IsMemberByAddress("127.0.0.1:8080"))
	require.True(t, config.IsHead("member-1"))
	require.True(t, config.IsTail("member-1"))
	require.Equal(t, uint64(1), config.Version)

	config = config.AddMember("member-2", "127.0.0.2:8080")
	require.True(t, config.IsMemberByID("member-1"))
	require.True(t, config.IsMemberByID("member-2"))
	require.True(t, config.IsHead("member-1"))
	require.True(t, config.IsTail("member-2"))
	require.Equal(t, uint64(2), config.Version)

	config = config.AddMember("member-3", "127.0.0.3:8080")
	require.True(t, config.IsMemberByID("member-1"))
	require.True(t, config.IsMemberByID("member-2"))
	require.True(t, config.IsMemberByID("member-3"))
	require.True(t, config.IsHead("member-1"))
	require.True(t, config.IsTail("member-3"))
	require.Equal(t, uint64(3), config.Version)

	config = config.RemoveMember("member-2")
	require.True(t, config.IsMemberByID("member-1"))
	require.False(t, config.IsMemberByID("member-2"))
	require.True(t, config.IsMemberByID("member-3"))
	require.True(t, config.IsHead("member-1"))
	require.True(t, config.IsTail("member-3"))
	require.Equal(t, uint64(4), config.Version)

	config = config.RemoveMember("member-1")
	require.False(t, config.IsMemberByID("member-1"))
	require.False(t, config.IsMemberByID("member-2"))
	require.True(t, config.IsMemberByID("member-3"))
	require.True(t, config.IsHead("member-3"))
	require.True(t, config.IsTail("member-3"))
	require.Equal(t, uint64(5), config.Version)

	config = config.RemoveMember("member-3")
	require.False(t, config.IsMemberByID("member-1"))
	require.False(t, config.IsMemberByID("member-2"))
	require.False(t, config.IsMemberByID("member-3"))
	require.Equal(t, uint64(6), config.Version)
}

func TestPredecessorSuccessor(t *testing.T) {
	head := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	middle := &ChainMember{ID: "member-2", Address: "127.0.0.2:8080"}
	tail := &ChainMember{ID: "member-3", Address: "127.0.0.3:8080"}

	config, err := NewConfiguration([]*ChainMember{head, middle, tail}, 0)
	require.NoError(t, err)

	pred := config.Predecessor(head.ID)
	require.Nil(t, pred)
	succ := config.Successor(head.ID)
	require.Equal(t, middle.Address, succ.Address)
	require.Equal(t, middle.ID, succ.ID)

	pred = config.Predecessor(middle.ID)
	require.Equal(t, head.Address, pred.Address)
	require.Equal(t, head.ID, pred.ID)
	succ = config.Successor(middle.ID)
	require.Equal(t, tail.Address, succ.Address)
	require.Equal(t, tail.ID, succ.ID)

	pred = config.Predecessor(tail.ID)
	require.Equal(t, middle.Address, pred.Address)
	require.Equal(t, middle.ID, pred.ID)
	succ = config.Successor(tail.ID)
	require.Nil(t, succ)
}

func TestChainBytes(t *testing.T) {
	head := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	middle := &ChainMember{ID: "member-2", Address: "127.0.0.2:8080"}
	tail := &ChainMember{ID: "member-3", Address: "127.0.0.3:8080"}

	config, err := NewConfiguration([]*ChainMember{head, middle, tail}, 10)
	require.NoError(t, err)

	b, err := config.Bytes()
	require.NoError(t, err)
	newChainConfig, err := NewConfigurationFromBytes(b)
	require.NoError(t, err)
	require.True(t, config.Equal(newChainConfig))
}

func TestIsHeadIsTail(t *testing.T) {
	head := &ChainMember{ID: "member-1", Address: "127.0.0.1:8080"}
	middle := &ChainMember{ID: "member-2", Address: "127.0.0.2:8080"}
	tail := &ChainMember{ID: "member-3", Address: "127.0.0.3:8080"}

	config, err := NewConfiguration([]*ChainMember{head, middle, tail}, 0)
	require.NoError(t, err)

	require.True(t, config.IsHead(head.ID))
	require.False(t, config.IsHead(middle.ID))
	require.False(t, config.IsHead(tail.ID))

	require.False(t, config.IsTail(head.ID))
	require.False(t, config.IsTail(middle.ID))
	require.True(t, config.IsTail(tail.ID))
}
