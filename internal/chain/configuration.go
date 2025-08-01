package chain

import (
	"errors"
	"net"

	pb "github.com/jmsadair/zebraos/proto/pbchain"
	"google.golang.org/protobuf/proto"
)

var ErrNotMemberOfChain = errors.New("not a member of the chain")

type ChainID string

type ChainConfiguration struct {
	// The ID of the chain that this node belongs to.
	ID ChainID
	// All members of the chain ordered from head to tail.
	members []net.Addr
	// Maps member address to its index in the chain.
	addressToMemberIndex map[string]int
}

// NewChainConfiguration creates a new ChainConfiguration instance.
// The chainID argument is the ID of the chain that the members are associated with.
// The members argument is a list of all members that belong to the chain, ordered from head to tail.
func NewChainConfiguration(chainID ChainID, members []net.Addr) (*ChainConfiguration, error) {
	addressToMemberIndex := make(map[string]int, len(members))
	for i, member := range members {
		addressToMemberIndex[member.String()] = i
	}
	return &ChainConfiguration{ID: chainID, members: members, addressToMemberIndex: addressToMemberIndex}, nil
}

// NewChainConfigurationFromProto creates a new ChainConfiguration instance from a protobuf message.
func NewChainConfigurationFromProto(chainConfigurationProto *pb.ChainConfiguration) (*ChainConfiguration, error) {
	members := make([]net.Addr, len(chainConfigurationProto.GetMembers()))
	for i, address := range chainConfigurationProto.GetMembers() {
		member, err := net.ResolveTCPAddr("tcp", address)
		if err != nil {
			return nil, err
		}
		members[i] = member
	}
	return NewChainConfiguration(ChainID(chainConfigurationProto.GetChainId()), members)
}

// Bytes converts the ChainConfiguration instance into bytes.
func (cm *ChainConfiguration) Bytes() ([]byte, error) {
	members := make([]string, len(cm.members))
	for i, member := range cm.members {
		members[i] = member.String()
	}
	chainConfigurationProto := &pb.ChainConfiguration{ChainId: string(cm.ID), Members: members}
	return proto.Marshal(chainConfigurationProto)
}

// Head returns the address of the head of the chain.
func (cm *ChainConfiguration) Head() net.Addr {
	return cm.members[0]
}

// Tail returns the address of the tail of the chain.
func (cm *ChainConfiguration) Tail() net.Addr {
	return cm.members[len(cm.members)-1]
}

// Predecessor returns the predecessor of the provided member.
// If the member is the head of the chain, nil is returned.
// If the member does not exist in the chain, an error is returned.
func (cm *ChainConfiguration) Predecessor(member net.Addr) (net.Addr, error) {
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
func (cm *ChainConfiguration) Successor(member net.Addr) (net.Addr, error) {
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
func (cm *ChainConfiguration) IsHead(member net.Addr) bool {
	return cm.members[0].String() == member.String()
}

// IsTail returns a boolean value indicating whether the provided member is the tail of the chain.
func (cm *ChainConfiguration) IsTail(member net.Addr) bool {
	return cm.members[len(cm.members)-1].String() == member.String()
}
