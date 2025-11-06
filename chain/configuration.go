package chain

import (
	pb "github.com/jmsadair/keychainDB/proto/chain"
	"google.golang.org/protobuf/proto"
)

// EmptyChain is an empty configuration. All chain nodes start with this configuration.
var EmptyChain = &Configuration{}

// ChainMember represents a member of the chain with an ID and address.
type ChainMember struct {
	// The ID of this chain member.
	ID string `json:"id"`
	// The address of this chain member.
	Address string `json:"address"`
}

// Equal returns true if this ChainMember is equal to another ChainMember.
// Two chain members are considered equal if and only if they have the same ID and address.
func (cm *ChainMember) Equal(other *ChainMember) bool {
	if cm == nil && other == nil {
		return true
	}
	if cm == nil || other == nil {
		return false
	}
	return cm.ID == other.ID && cm.Address == other.Address
}

// Configuration is a membership configuration for a particular chain.
type Configuration struct {
	// The current version of the configuration.
	Version uint64
	// All members of the chain ordered from head to tail.
	members []*ChainMember
	// Maps member ID to its index in the chain.
	idToMemberIndex map[string]int
	// Maps member address to its index in the chain.
	addressToMemberIndex map[string]int
}

// NewConfiguration creates a new Configuration instance.
// The provided members should include all members that belong to the chain, ordered from head to tail.
func NewConfiguration(members []*ChainMember, version uint64) *Configuration {
	idToMemberIndex := make(map[string]int, len(members))
	addressToMemberIndex := make(map[string]int, len(members))
	for i, member := range members {
		idToMemberIndex[member.ID] = i
		addressToMemberIndex[member.Address] = i
	}
	return &Configuration{
		Version:              version,
		members:              members,
		idToMemberIndex:      idToMemberIndex,
		addressToMemberIndex: addressToMemberIndex,
	}
}

// NewConfigurationFromProto creates a new Configuration instance from a protobuf message.
func NewConfigurationFromProto(configurationProto *pb.Configuration) *Configuration {
	members := make([]*ChainMember, len(configurationProto.GetMembers()))
	for i, pbMember := range configurationProto.GetMembers() {
		members[i] = &ChainMember{
			ID:      pbMember.GetId(),
			Address: pbMember.GetAddress(),
		}
	}
	return NewConfiguration(members, configurationProto.GetVersion())
}

// NewConfigurationFromBytes creates a new Configuration instance from bytes.
func NewConfigurationFromBytes(b []byte) (*Configuration, error) {
	configurationProto := &pb.Configuration{}
	if err := proto.Unmarshal(b, configurationProto); err != nil {
		return nil, err
	}
	return NewConfigurationFromProto(configurationProto), nil
}

// Bytes converts the Configuration instance into bytes.
func (c *Configuration) Bytes() ([]byte, error) {
	members := make([]*pb.ChainMember, len(c.members))
	for i, member := range c.members {
		members[i] = &pb.ChainMember{
			Id:      member.ID,
			Address: member.Address,
		}
	}
	configurationProto := &pb.Configuration{Members: members, Version: c.Version}
	return proto.Marshal(configurationProto)
}

// Equal returns a boolean value indicating whether this configuration is equal to the provided one.
// Two configurations are considered equal if and only if they have the same members in the same order
// as well as the same version.
func (c *Configuration) Equal(config *Configuration) bool {
	if len(c.members) != len(config.members) {
		return false
	}
	if c.Version != config.Version {
		return false
	}
	for i := range len(c.members) {
		if c.members[i].ID != config.members[i].ID || c.members[i].Address != config.members[i].Address {
			return false
		}
	}
	return true
}

// Copy creates a copy of the Configuration instance.
func (c *Configuration) Copy() *Configuration {
	members := make([]*ChainMember, len(c.members))
	idToMemberIndex := make(map[string]int, len(c.idToMemberIndex))
	addrToMemberIndex := make(map[string]int, len(c.addressToMemberIndex))
	for i, member := range c.members {
		members[i] = &ChainMember{ID: member.ID, Address: member.Address}
		idToMemberIndex[member.ID] = i
		addrToMemberIndex[member.Address] = i
	}
	return &Configuration{Version: c.Version, members: members, idToMemberIndex: idToMemberIndex, addressToMemberIndex: addrToMemberIndex}
}

// AddMember creates a new configuration with the member added at the tail if it is not already present.
func (c *Configuration) AddMember(id, address string) *Configuration {
	newConfig := c.Copy()
	if newConfig.IsMemberByID(id) {
		return newConfig
	}
	newConfig.Version++
	member := &ChainMember{ID: id, Address: address}
	newConfig.members = append(newConfig.members, member)
	index := len(newConfig.members) - 1
	newConfig.idToMemberIndex[id] = index
	newConfig.addressToMemberIndex[address] = index
	return newConfig
}

// RemoveMember creates a new configuration with the member removed by ID.
func (c *Configuration) RemoveMember(id string) *Configuration {
	newConfig := c.Copy()
	i, ok := newConfig.idToMemberIndex[id]
	if !ok {
		return newConfig
	}
	newConfig.Version++
	removedMember := newConfig.members[i]
	members := make([]*ChainMember, 0, len(newConfig.members)-1)
	members = append(members, newConfig.members[:i]...)
	members = append(members, newConfig.members[i+1:]...)
	newConfig.members = members
	delete(newConfig.idToMemberIndex, id)
	delete(newConfig.addressToMemberIndex, removedMember.Address)

	newConfig.idToMemberIndex = make(map[string]int, len(members))
	newConfig.addressToMemberIndex = make(map[string]int, len(members))
	for i, member := range members {
		newConfig.idToMemberIndex[member.ID] = i
		newConfig.addressToMemberIndex[member.Address] = i
	}

	return newConfig
}

// Members returns the members of the configuration. The members are ordered head to tail.
func (c *Configuration) Members() []*ChainMember {
	membersCopy := make([]*ChainMember, len(c.members))
	for i, member := range c.members {
		membersCopy[i] = &ChainMember{ID: member.ID, Address: member.Address}
	}
	return membersCopy
}

// Member gets the member by the member ID. If the member does not exist, nil is returned.
func (c *Configuration) Member(id string) *ChainMember {
	i, ok := c.idToMemberIndex[id]
	if !ok {
		return nil
	}
	return c.members[i]
}

// Head returns the head member of the chain.
// If the chain has no members, nil is returned.
func (c *Configuration) Head() *ChainMember {
	if len(c.members) == 0 {
		return nil
	}
	return &ChainMember{ID: c.members[0].ID, Address: c.members[0].Address}
}

// Tail returns the tail member of the chain.
// If the chain has no members, nil is returned.
func (c *Configuration) Tail() *ChainMember {
	if len(c.members) == 0 {
		return nil
	}
	last := c.members[len(c.members)-1]
	return &ChainMember{ID: last.ID, Address: last.Address}
}

// Predecessor returns the predecessor of the provided member ID.
// If the ID is the head of the chain or is not actually a member of the chain, nil is returned.
func (c *Configuration) Predecessor(id string) *ChainMember {
	memberIndex, ok := c.idToMemberIndex[id]
	if !ok {
		return nil
	}
	if memberIndex-1 < 0 {
		return nil
	}
	pred := c.members[memberIndex-1]
	return &ChainMember{ID: pred.ID, Address: pred.Address}
}

// Successor returns the successor of the provided member ID.
// If the ID is the tail of the chain or is not actually a member of the chain, nil is returned.
func (c *Configuration) Successor(id string) *ChainMember {
	memberIndex, ok := c.idToMemberIndex[id]
	if !ok {
		return nil
	}
	if memberIndex+1 >= len(c.members) {
		return nil
	}
	succ := c.members[memberIndex+1]
	return &ChainMember{ID: succ.ID, Address: succ.Address}
}

// IsHead returns a boolean value indicating whether the provided ID is the head of the chain.
func (c *Configuration) IsHead(id string) bool {
	if len(c.members) == 0 {
		return false
	}
	return c.members[0].ID == id
}

// IsTail returns a boolean value indicating whether the provided ID is the tail of the chain.
func (c *Configuration) IsTail(id string) bool {
	if len(c.members) == 0 {
		return false
	}
	return c.members[len(c.members)-1].ID == id
}

// IsMemberByID returns a boolean value indicating whether the provided ID is a member of the chain.
func (c *Configuration) IsMemberByID(id string) bool {
	_, ok := c.idToMemberIndex[id]
	return ok
}

// IsMemberByAddress returns a boolean value indicating whether the provided address is a member of the chain.
func (c *Configuration) IsMemberByAddress(address string) bool {
	_, ok := c.addressToMemberIndex[address]
	return ok
}

// Proto converts Configuration to protobuf Configuration.
func (c *Configuration) Proto() *pb.Configuration {
	members := make([]*pb.ChainMember, len(c.members))
	for i, member := range c.members {
		members[i] = &pb.ChainMember{Id: member.ID, Address: member.Address}
	}
	return &pb.Configuration{Version: c.Version, Members: members}
}
