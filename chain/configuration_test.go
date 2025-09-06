package chain

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEqual(t *testing.T) {
	member1, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	member2, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	member3, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)

	config1, err := NewConfiguration([]net.Addr{member1, member2, member3})
	require.NoError(t, err)
	config2, err := NewConfiguration([]net.Addr{member2, member1, member3})
	require.NoError(t, err)
	config3, err := NewConfiguration([]net.Addr{member1, member2})
	require.NoError(t, err)
	config4, err := NewConfiguration([]net.Addr{member1, member2, member3})
	require.NoError(t, err)

	// Same members of the chain but different order.
	require.False(t, config1.Equal(config2))
	// Same order but missing the last member of the chain.
	require.False(t, config1.Equal(config3))
	// Same members of the chain in the same order.
	require.True(t, config1.Equal(config4))
}

func TestCopy(t *testing.T) {
	member1, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)

	config, err := NewConfiguration([]net.Addr{member1})
	require.NoError(t, err)
	require.Len(t, config.members, 1)
	require.Len(t, config.addressToMemberIndex, 1)
	require.Contains(t, config.addressToMemberIndex, member1.String())
	require.Equal(t, member1.String(), config.members[0].String())

	// Create a copy of the original configuration and mutate it.
	configCopy := config.Copy()
	configCopy.addressToMemberIndex = nil
	configCopy.members = nil

	// Ensure the original configuration as not been modified.
	require.Len(t, config.members, 1)
	require.Len(t, config.addressToMemberIndex, 1)
	require.Contains(t, config.addressToMemberIndex, member1.String())
	require.Equal(t, member1.String(), config.members[0].String())
}

func TestAddMemberRemoveMember(t *testing.T) {
	member1, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	member2, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	member3, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)

	config, err := NewConfiguration([]net.Addr{})
	require.NoError(t, err)
	config = config.AddMember(member1)
	require.True(t, config.IsMember(member1))
	require.True(t, config.IsHead(member1))
	require.True(t, config.IsTail(member1))

	config = config.AddMember(member2)
	require.True(t, config.IsMember(member1))
	require.True(t, config.IsMember(member2))
	require.True(t, config.IsHead(member1))
	require.True(t, config.IsTail(member2))

	config = config.AddMember(member3)
	require.True(t, config.IsMember(member1))
	require.True(t, config.IsMember(member2))
	require.True(t, config.IsMember(member3))
	require.True(t, config.IsHead(member1))
	require.True(t, config.IsTail(member3))

	config = config.RemoveMember(member2)
	require.True(t, config.IsMember(member1))
	require.False(t, config.IsMember(member2))
	require.True(t, config.IsMember(member3))
	require.True(t, config.IsHead(member1))
	require.True(t, config.IsTail(member3))

	config = config.RemoveMember(member1)
	require.False(t, config.IsMember(member1))
	require.False(t, config.IsMember(member2))
	require.True(t, config.IsMember(member3))
	require.True(t, config.IsHead(member3))
	require.True(t, config.IsTail(member3))

	config = config.RemoveMember(member3)
	require.False(t, config.IsMember(member1))
	require.False(t, config.IsMember(member2))
	require.False(t, config.IsMember(member3))
}

func TestPredecessorSuccessor(t *testing.T) {
	head, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	middle, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	tail, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)

	config, err := NewConfiguration([]net.Addr{head, middle, tail})
	require.NoError(t, err)

	pred := config.Predecessor(head)
	require.Nil(t, pred)
	succ := config.Successor(head)
	require.Equal(t, middle.String(), succ.String())

	pred = config.Predecessor(middle)
	require.Equal(t, head.String(), pred.String())
	succ = config.Successor(middle)
	require.Equal(t, tail.String(), succ.String())

	pred = config.Predecessor(tail)
	require.Equal(t, middle.String(), pred.String())
	succ = config.Successor(tail)
	require.Nil(t, succ)
}

func TestChainBytes(t *testing.T) {
	head, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	middle, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	tail, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)

	config, err := NewConfiguration([]net.Addr{head, middle, tail})
	require.NoError(t, err)

	b, err := config.Bytes()
	require.NoError(t, err)
	newChainConfig, err := NewConfigurationFromBytes(b)
	require.NoError(t, err)
	require.True(t, config.Equal(newChainConfig))
}

func TestIsHeadIsTail(t *testing.T) {
	head, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	middle, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	tail, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)

	config, err := NewConfiguration([]net.Addr{head, middle, tail})
	require.NoError(t, err)

	require.True(t, config.IsHead(head))
	require.False(t, config.IsHead(middle))
	require.False(t, config.IsHead(tail))

	require.False(t, config.IsTail(head))
	require.False(t, config.IsTail(middle))
	require.True(t, config.IsTail(tail))
}
