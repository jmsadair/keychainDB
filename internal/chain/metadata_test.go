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

	chainMetadata, err := NewChainMetadata(chainID, []net.Addr{head, middle, tail})
	require.NoError(t, err)

	pred, err := chainMetadata.Predecessor(head)
	require.NoError(t, err)
	require.Nil(t, pred)
	succ, err := chainMetadata.Successor(head)
	require.NoError(t, err)
	require.Equal(t, middle.String(), succ.String())

	pred, err = chainMetadata.Predecessor(middle)
	require.NoError(t, err)
	require.Equal(t, head.String(), pred.String())
	succ, err = chainMetadata.Successor(middle)
	require.NoError(t, err)
	require.Equal(t, tail.String(), succ.String())

	pred, err = chainMetadata.Predecessor(tail)
	require.NoError(t, err)
	require.Equal(t, middle.String(), pred.String())
	succ, err = chainMetadata.Successor(tail)
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

	chainMetadata, err := NewChainMetadata(chainID, []net.Addr{head, middle, tail})
	require.NoError(t, err)

	b, err := chainMetadata.Bytes()
	require.NoError(t, err)
	var chainMetadataProto pb.ChainMetadata
	expectedMembers := []string{head.String(), middle.String(), tail.String()}
	require.NoError(t, proto.Unmarshal(b, &chainMetadataProto))
	require.Equal(t, string(chainID), chainMetadataProto.GetChainId())
	require.Equal(t, expectedMembers, chainMetadataProto.GetMembers())
}

func TestIsHeadIsTail(t *testing.T) {
	chainID := ChainID("test-chain")
	head, err := net.ResolveTCPAddr("tcp", "127.0.0.1:8080")
	require.NoError(t, err)
	middle, err := net.ResolveTCPAddr("tcp", "127.0.0.2:8080")
	require.NoError(t, err)
	tail, err := net.ResolveTCPAddr("tcp", "127.0.0.3:8080")
	require.NoError(t, err)

	chainMetadata, err := NewChainMetadata(chainID, []net.Addr{head, middle, tail})
	require.NoError(t, err)

	require.True(t, chainMetadata.IsHead(head))
	require.False(t, chainMetadata.IsHead(middle))
	require.False(t, chainMetadata.IsHead(tail))

	require.False(t, chainMetadata.IsTail(head))
	require.False(t, chainMetadata.IsTail(middle))
	require.True(t, chainMetadata.IsTail(tail))
}
