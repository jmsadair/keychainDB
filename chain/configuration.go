package chain

import (
	"errors"
	"net"

	pb "github.com/jmsadair/keychain/proto/pbchain"
	"google.golang.org/protobuf/proto"
)

var (
	// Indicates that a node is not a member of any chain.
	ErrNotMemberOfChain = errors.New("not a member of the chain")
	// An empty configuration. All chain nodes start with this configuration.
	EmptyChain = &ChainConfiguration{}
)

// ChainID is an identifier for a particular chain.
type ChainID string

// ChainConfiguration is a membership configuration for a particular chain.
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

// NewChainConfigurationFromBytes creates a new ChainConfiguration instance from bytes.
func NewChainConfigurationFromBytes(b []byte) (*ChainConfiguration, error) {
	chainConfigurationProto := &pb.ChainConfiguration{}
	if err := proto.Unmarshal(b, chainConfigurationProto); err != nil {
		return nil, err
	}
	return NewChainConfigurationFromProto(chainConfigurationProto)
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

// Equal returns a boolean value indicating whether this configuration is equal to the provided one.
// Two configurations are considered equal if and only if they have the same members in the same order
// and the same chain ID.
func (cm *ChainConfiguration) Equal(config *ChainConfiguration) bool {
	if len(cm.members) != len(config.members) {
		return false
	}
	if cm.ID != config.ID {
		return false
	}
	for i := range len(cm.members) {
		if cm.members[i].String() != config.members[i].String() {
			return false
		}
	}
	return true
}

// Copy creates a copy of the ChainConfiguration instance.
func (cm *ChainConfiguration) Copy() *ChainConfiguration {
	members := make([]net.Addr, len(cm.members))
	addrToMemberIndex := make(map[string]int, len(cm.addressToMemberIndex))
	for i, member := range cm.members {
		members[i] = member
		addrToMemberIndex[member.String()] = i
	}
	return &ChainConfiguration{ID: cm.ID, members: members, addressToMemberIndex: addrToMemberIndex}
}

// AddMember creates a new configuration with the member added at the tail if it is not already present.
func (cm *ChainConfiguration) AddMember(member net.Addr) *ChainConfiguration {
	newConfig := cm.Copy()
	if newConfig.IsMember(member) {
		return newConfig
	}
	newConfig.members = append(newConfig.members, member)
	newConfig.addressToMemberIndex[member.String()] = len(newConfig.members) - 1
	return newConfig
}

// RemoveMember creates a new configuration with the member removed.
func (cm *ChainConfiguration) RemoveMember(member net.Addr) *ChainConfiguration {
	newConfig := cm.Copy()
	i, ok := newConfig.addressToMemberIndex[member.String()]
	if !ok {
		return newConfig
	}
	members := make([]net.Addr, 0, len(newConfig.members)-1)
	members = append(members, newConfig.members[:i]...)
	members = append(members, newConfig.members[i+1:]...)
	newConfig.members = members
	delete(newConfig.addressToMemberIndex, member.String())
	return newConfig
}

// Members returns the members of the configuration. The members are ordered head to tail.
func (cm *ChainConfiguration) Members() []net.Addr {
	membersCopy := make([]net.Addr, len(cm.members))
	copy(cm.members, membersCopy)
	return membersCopy
}

// Head returns the address of the head of the chain.
// If the chain has no members, nil is returned.
func (cm *ChainConfiguration) Head() net.Addr {
	if len(cm.members) == 0 {
		return nil
	}
	return cm.members[0]
}

// Tail returns the address of the tail of the chain.
// If the chain has no members, nil is returned.
func (cm *ChainConfiguration) Tail() net.Addr {
	if len(cm.members) == 0 {
		return nil
	}
	return cm.members[len(cm.members)-1]
}

// Predecessor returns the predecessor of the provided address.
// If the address is the head of the chain or is not actually a member of the chain, nil is returned.
func (cm *ChainConfiguration) Predecessor(member net.Addr) net.Addr {
	memberIndex, ok := cm.addressToMemberIndex[member.String()]
	if !ok {
		return nil
	}
	if memberIndex-1 < 0 {
		return nil
	}
	return cm.members[memberIndex-1]
}

// Successor returns the successor of the provided address.
// If the address is the tail of the chain or is not actually a member of the chain, nil is returned.
func (cm *ChainConfiguration) Successor(member net.Addr) net.Addr {
	memberIndex, ok := cm.addressToMemberIndex[member.String()]
	if !ok {
		return nil
	}
	if memberIndex+1 >= len(cm.members) {
		return nil
	}
	return cm.members[memberIndex+1]
}

// IsHead returns a boolean value indicating whether the provided address is the head of the chain.
func (cm *ChainConfiguration) IsHead(address net.Addr) bool {
	if len(cm.members) == 0 {
		return false
	}
	return cm.members[0].String() == address.String()
}

// IsTail returns a boolean value indicating whether the provided address is the tail of the chain.
func (cm *ChainConfiguration) IsTail(address net.Addr) bool {
	if len(cm.members) == 0 {
		return false
	}
	return cm.members[len(cm.members)-1].String() == address.String()
}

// IsMember returns a boolean value indicating whether the provided address is a member of the chain.
func (cm *ChainConfiguration) IsMember(address net.Addr) bool {
	_, ok := cm.addressToMemberIndex[address.String()]
	return ok
}
