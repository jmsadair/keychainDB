package chain

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEqual(t *testing.T) {
	chainID1 := ChainID("chain-1")
	chainID2 := ChainID("chain-2")
	member1, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	member2, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	member3, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)

	chainConfig1, err := NewChainConfiguration(chainID1, []net.Addr{member1, member2, member3})
	require.NoError(t, err)
	chainConfig2, err := NewChainConfiguration(chainID1, []net.Addr{member2, member1, member3})
	require.NoError(t, err)
	chainConfig3, err := NewChainConfiguration(chainID1, []net.Addr{member1, member2})
	require.NoError(t, err)
	chainConfig4, err := NewChainConfiguration(chainID2, []net.Addr{member1, member2, member3})
	require.NoError(t, err)
	chainConfig5, err := NewChainConfiguration(chainID1, []net.Addr{member1, member2, member3})
	require.NoError(t, err)

	// Same members of the chain but different order.
	require.False(t, chainConfig1.Equal(chainConfig2))
	// Same order but missing the last member of the chain.
	require.False(t, chainConfig1.Equal(chainConfig3))
	// Same members of the chain in the same order, but different chain ID.
	require.False(t, chainConfig1.Equal(chainConfig4))
	// Same members of the chain in the same order with the same chain ID.
	require.True(t, chainConfig1.Equal(chainConfig5))
}

func TestCopy(t *testing.T) {
	chainID := ChainID("chain-1")
	member1, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)

	chainConfig, err := NewChainConfiguration(chainID, []net.Addr{member1})
	require.NoError(t, err)
	require.Len(t, chainConfig.members, 1)
	require.Len(t, chainConfig.addressToMemberIndex, 1)
	require.Contains(t, chainConfig.addressToMemberIndex, member1.String())
	require.Equal(t, member1.String(), chainConfig.members[0].String())

	// Create a copy of the original configuration and mutate it.
	chainConfigCopy := chainConfig.Copy()
	chainConfigCopy.addressToMemberIndex = nil
	chainConfigCopy.members = nil

	// Ensure the original configuration as not been modified.
	require.Len(t, chainConfig.members, 1)
	require.Len(t, chainConfig.addressToMemberIndex, 1)
	require.Contains(t, chainConfig.addressToMemberIndex, member1.String())
	require.Equal(t, member1.String(), chainConfig.members[0].String())
}

func TestAddMemberRemoveMember(t *testing.T) {
	chainID := ChainID("chain-1")
	member1, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	member2, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	member3, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)

	chainConfig, err := NewChainConfiguration(chainID, []net.Addr{})
	require.NoError(t, err)
	chainConfig = chainConfig.AddMember(member1)
	require.True(t, chainConfig.IsMember(member1))
	require.True(t, chainConfig.IsHead(member1))
	require.True(t, chainConfig.IsTail(member1))

	chainConfig = chainConfig.AddMember(member2)
	require.True(t, chainConfig.IsMember(member1))
	require.True(t, chainConfig.IsMember(member2))
	require.True(t, chainConfig.IsHead(member1))
	require.True(t, chainConfig.IsTail(member2))

	chainConfig = chainConfig.AddMember(member3)
	require.True(t, chainConfig.IsMember(member1))
	require.True(t, chainConfig.IsMember(member2))
	require.True(t, chainConfig.IsMember(member3))
	require.True(t, chainConfig.IsHead(member1))
	require.True(t, chainConfig.IsTail(member3))

	chainConfig = chainConfig.RemoveMember(member2)
	require.True(t, chainConfig.IsMember(member1))
	require.False(t, chainConfig.IsMember(member2))
	require.True(t, chainConfig.IsMember(member3))
	require.True(t, chainConfig.IsHead(member1))
	require.True(t, chainConfig.IsTail(member3))

	chainConfig = chainConfig.RemoveMember(member1)
	require.False(t, chainConfig.IsMember(member1))
	require.False(t, chainConfig.IsMember(member2))
	require.True(t, chainConfig.IsMember(member3))
	require.True(t, chainConfig.IsHead(member3))
	require.True(t, chainConfig.IsTail(member3))

	chainConfig = chainConfig.RemoveMember(member3)
	require.False(t, chainConfig.IsMember(member1))
	require.False(t, chainConfig.IsMember(member2))
	require.False(t, chainConfig.IsMember(member3))
}

func TestPredecessorSuccessor(t *testing.T) {
	chainID := ChainID("chain-1")
	head, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	middle, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	tail, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)

	chainConfig, err := NewChainConfiguration(chainID, []net.Addr{head, middle, tail})
	require.NoError(t, err)

	pred := chainConfig.Predecessor(head)
	require.Nil(t, pred)
	succ := chainConfig.Successor(head)
	require.Equal(t, middle.String(), succ.String())

	pred = chainConfig.Predecessor(middle)
	require.Equal(t, head.String(), pred.String())
	succ = chainConfig.Successor(middle)
	require.Equal(t, tail.String(), succ.String())

	pred = chainConfig.Predecessor(tail)
	require.Equal(t, middle.String(), pred.String())
	succ = chainConfig.Successor(tail)
	require.Nil(t, succ)
}

func TestChainBytes(t *testing.T) {
	chainID := ChainID("chain-1")
	head, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	middle, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	tail, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)

	chainConfig, err := NewChainConfiguration(chainID, []net.Addr{head, middle, tail})
	require.NoError(t, err)

	b, err := chainConfig.Bytes()
	require.NoError(t, err)
	newChainConfig, err := NewChainConfigurationFromBytes(b)
	require.NoError(t, err)
	require.True(t, chainConfig.Equal(newChainConfig))
}

func TestIsHeadIsTail(t *testing.T) {
	chainID := ChainID("chain-1")
	head, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	middle, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	tail, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)

	chainConfig, err := NewChainConfiguration(chainID, []net.Addr{head, middle, tail})
	require.NoError(t, err)

	require.True(t, chainConfig.IsHead(head))
	require.False(t, chainConfig.IsHead(middle))
	require.False(t, chainConfig.IsHead(tail))

	require.False(t, chainConfig.IsTail(head))
	require.False(t, chainConfig.IsTail(middle))
	require.True(t, chainConfig.IsTail(tail))
}
