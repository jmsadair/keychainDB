package chain

import (
	"net"
	"testing"

	pb "github.com/jmsadair/zebraos/proto/pbchain"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestPredecessorSuccessor(t *testing.T) {
	chainID := ChainID("test-chain")
	head, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	middle, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	tail, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)

	chainConfig, err := NewChainConfiguration(chainID, []net.Addr{head, middle, tail})
	require.NoError(t, err)

	pred, err := chainConfig.Predecessor(head)
	require.NoError(t, err)
	require.Nil(t, pred)
	succ, err := chainConfig.Successor(head)
	require.NoError(t, err)
	require.Equal(t, middle.String(), succ.String())

	pred, err = chainConfig.Predecessor(middle)
	require.NoError(t, err)
	require.Equal(t, head.String(), pred.String())
	succ, err = chainConfig.Successor(middle)
	require.NoError(t, err)
	require.Equal(t, tail.String(), succ.String())

	pred, err = chainConfig.Predecessor(tail)
	require.NoError(t, err)
	require.Equal(t, middle.String(), pred.String())
	succ, err = chainConfig.Successor(tail)
	require.NoError(t, err)
	require.Nil(t, succ)
}

func TestChainMetadataBytes(t *testing.T) {
	chainID := ChainID("test-chain")
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
	var chainConfigProto pb.ChainConfiguration
	expectedMembers := []string{head.String(), middle.String(), tail.String()}
	require.NoError(t, proto.Unmarshal(b, &chainConfigProto))
	require.Equal(t, string(chainID), chainConfigProto.GetChainId())
	require.Equal(t, expectedMembers, chainConfigProto.GetMembers())
}

func TestIsHeadIsTail(t *testing.T) {
	chainID := ChainID("test-chain")
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
