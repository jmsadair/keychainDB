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
	EmptyChain = &Configuration{}
)

// ChainID is an identifier for a particular chain.
type ChainID string

// Configuration is a membership configuration for a particular chain.
type Configuration struct {
	// The ID of the chain that this node belongs to.
	ID ChainID
	// All members of the chain ordered from head to tail.
	members []net.Addr
	// Maps member address to its index in the chain.
	addressToMemberIndex map[string]int
}

// NewConfiguration creates a new Configuration instance.
// The chainID argument is the ID of the chain that the members are associated with.
// The members argument is a list of all members that belong to the chain, ordered from head to tail.
func NewConfiguration(chainID ChainID, members []net.Addr) (*Configuration, error) {
	addressToMemberIndex := make(map[string]int, len(members))
	for i, member := range members {
		addressToMemberIndex[member.String()] = i
	}
	return &Configuration{ID: chainID, members: members, addressToMemberIndex: addressToMemberIndex}, nil
}

// NewConfigurationFromProto creates a new Configuration instance from a protobuf message.
func NewConfigurationFromProto(configurationProto *pb.Configuration) (*Configuration, error) {
	members := make([]net.Addr, len(configurationProto.GetMembers()))
	for i, address := range configurationProto.GetMembers() {
		member, err := net.ResolveTCPAddr("tcp", address)
		if err != nil {
			return nil, err
		}
		members[i] = member
	}
	return NewConfiguration(ChainID(configurationProto.GetChainId()), members)
}

// NewConfigurationFromBytes creates a new Configuration instance from bytes.
func NewConfigurationFromBytes(b []byte) (*Configuration, error) {
	configurationProto := &pb.Configuration{}
	if err := proto.Unmarshal(b, configurationProto); err != nil {
		return nil, err
	}
	return NewConfigurationFromProto(configurationProto)
}

// Bytes converts the Configuration instance into bytes.
func (c *Configuration) Bytes() ([]byte, error) {
	members := make([]string, len(c.members))
	for i, member := range c.members {
		members[i] = member.String()
	}
	configurationProto := &pb.Configuration{ChainId: string(c.ID), Members: members}
	return proto.Marshal(configurationProto)
}

// Equal returns a boolean value indicating whether this configuration is equal to the provided one.
// Two configurations are considered equal if and only if they have the same members in the same order
// and the same chain ID.
func (c *Configuration) Equal(config *Configuration) bool {
	if len(c.members) != len(config.members) {
		return false
	}
	if c.ID != config.ID {
		return false
	}
	for i := range len(c.members) {
		if c.members[i].String() != config.members[i].String() {
			return false
		}
	}
	return true
}

// Copy creates a copy of the Configuration instance.
func (c *Configuration) Copy() *Configuration {
	members := make([]net.Addr, len(c.members))
	addrToMemberIndex := make(map[string]int, len(c.addressToMemberIndex))
	for i, member := range c.members {
		members[i] = member
		addrToMemberIndex[member.String()] = i
	}
	return &Configuration{ID: c.ID, members: members, addressToMemberIndex: addrToMemberIndex}
}

// AddMember creates a new configuration with the member added at the tail if it is not already present.
func (c *Configuration) AddMember(member net.Addr) *Configuration {
	newConfig := c.Copy()
	if newConfig.IsMember(member) {
		return newConfig
	}
	newConfig.members = append(newConfig.members, member)
	newConfig.addressToMemberIndex[member.String()] = len(newConfig.members) - 1
	return newConfig
}

// RemoveMember creates a new configuration with the member removed.
func (c *Configuration) RemoveMember(member net.Addr) *Configuration {
	newConfig := c.Copy()
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
func (c *Configuration) Members() []net.Addr {
	membersCopy := make([]net.Addr, len(c.members))
	copy(membersCopy, c.members)
	return membersCopy
}

// Head returns the address of the head of the chain.
// If the chain has no members, nil is returned.
func (c *Configuration) Head() net.Addr {
	if len(c.members) == 0 {
		return nil
	}
	return c.members[0]
}

// Tail returns the address of the tail of the chain.
// If the chain has no members, nil is returned.
func (c *Configuration) Tail() net.Addr {
	if len(c.members) == 0 {
		return nil
	}
	return c.members[len(c.members)-1]
}

// Predecessor returns the predecessor of the provided address.
// If the address is the head of the chain or is not actually a member of the chain, nil is returned.
func (c *Configuration) Predecessor(member net.Addr) net.Addr {
	memberIndex, ok := c.addressToMemberIndex[member.String()]
	if !ok {
		return nil
	}
	if memberIndex-1 < 0 {
		return nil
	}
	return c.members[memberIndex-1]
}

// Successor returns the successor of the provided address.
// If the address is the tail of the chain or is not actually a member of the chain, nil is returned.
func (c *Configuration) Successor(member net.Addr) net.Addr {
	memberIndex, ok := c.addressToMemberIndex[member.String()]
	if !ok {
		return nil
	}
	if memberIndex+1 >= len(c.members) {
		return nil
	}
	return c.members[memberIndex+1]
}

// IsHead returns a boolean value indicating whether the provided address is the head of the chain.
func (c *Configuration) IsHead(address net.Addr) bool {
	if len(c.members) == 0 {
		return false
	}
	return c.members[0].String() == address.String()
}

// IsTail returns a boolean value indicating whether the provided address is the tail of the chain.
func (c *Configuration) IsTail(address net.Addr) bool {
	if len(c.members) == 0 {
		return false
	}
	return c.members[len(c.members)-1].String() == address.String()
}

// IsMember returns a boolean value indicating whether the provided address is a member of the chain.
func (c *Configuration) IsMember(address net.Addr) bool {
	_, ok := c.addressToMemberIndex[address.String()]
	return ok
}
