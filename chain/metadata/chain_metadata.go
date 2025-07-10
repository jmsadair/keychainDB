package metadata

import (
	"errors"
	"net"

	pb "github.com/jmsadair/zebraos/proto/pbchain"
	"google.golang.org/protobuf/proto"
)

var ErrNotMemberOfChain = errors.New("not a member of the chain")

type ChainID string

type ChainMetadata struct {
	// The ID of the chain that this node belongs to.
	ID ChainID
	// All members of the chain ordered from head to tail.
	members []net.Addr
	// Maps member address to its index in the chain.
	addressToMemberIndex map[string]int
}

// NewChainMetadata creates a new ChainMetadata instance.
// The chainID argument is the ID of the chain that the members are associated with.
// The members argument is a list of all members that belong to the chain, ordered from head to tail.
func NewChainMetadata(chainID ChainID, members []net.Addr) (*ChainMetadata, error) {
	addressToMemberIndex := make(map[string]int, len(members))
	for i, member := range members {
		addressToMemberIndex[member.String()] = i
	}
	return &ChainMetadata{ID: chainID, members: members, addressToMemberIndex: addressToMemberIndex}, nil
}

// NewChainMetadataFromProto creates a new ChainMetadata instance from a protobuf message.
func NewChainMetadataFromProto(chainMetadataProto *pb.ChainMetadata) (*ChainMetadata, error) {
	members := make([]net.Addr, len(chainMetadataProto.GetMembers()))
	for i, address := range chainMetadataProto.GetMembers() {
		member, err := net.ResolveTCPAddr("tcp", address)
		if err != nil {
			return nil, err
		}
		members[i] = member
	}
	return NewChainMetadata(ChainID(chainMetadataProto.GetChainId()), members)
}

// Bytes converts the ChainMetadata instance into bytes.
func (cm *ChainMetadata) Bytes() ([]byte, error) {
	members := make([]string, len(cm.members))
	for i, member := range cm.members {
		members[i] = member.String()
	}
	chainMetadataProto := &pb.ChainMetadata{ChainId: string(cm.ID), Members: members}
	return proto.Marshal(chainMetadataProto)
}

// Head returns the address of the head of the chain.
func (cm *ChainMetadata) Head() net.Addr {
	return cm.members[0]
}

// Tail returns the address of the tail of the chain.
func (cm *ChainMetadata) Tail() net.Addr {
	return cm.members[len(cm.members)-1]
}

// Predecessor returns the predecessor of the provided member.
// If the member is the head of the chain, nil is returned.
// If the member does not exist in the chain, an error is returned.
func (cm *ChainMetadata) Predecessor(member net.Addr) (net.Addr, error) {
	memberIndex, ok := cm.addressToMemberIndex[member.String()]
	if !ok {
		return nil, ErrNotMemberOfChain
	}
	if memberIndex-1 < 0 {
		return nil, nil
	}
	return cm.members[memberIndex-1], nil
}

// Successor returns the successor of the provided member.
// If the member is the tail of the chain, nil is returned.
// If the member does not exist in the chain, an error is returned.
func (cm *ChainMetadata) Successor(member net.Addr) (net.Addr, error) {
	memberIndex, ok := cm.addressToMemberIndex[member.String()]
	if !ok {
		return nil, ErrNotMemberOfChain
	}
	if memberIndex+1 >= len(cm.members) {
		return nil, nil
	}
	return cm.members[memberIndex+1], nil
}

// IsHead returns a boolean value indicating whether the provided member is the head of the chain.
func (cm *ChainMetadata) IsHead(member net.Addr) bool {
	return cm.members[0].String() == member.String()
}

// IsTail returns a boolean value indicating whether the provided member is the tail of the chain.
func (cm *ChainMetadata) IsTail(member net.Addr) bool {
	return cm.members[len(cm.members)-1].String() == member.String()
}
