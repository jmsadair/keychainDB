package chainnode

import (
	"errors"
	"net"
	"sync/atomic"

	"github.com/jmsadair/zebraos/chain/metadata"
	"github.com/jmsadair/zebraos/chain/storage"
)

var (
	ErrNotMemberOfChain = errors.New("node is not a member of a chain")
	ErrNotHead          = errors.New("writes must be initiated from the head of the chain")
)

type Storage interface {
	UncommittedWrite(key string, value []byte, version uint64) error
	UncommittedWriteNewVersion(key string, value []byte) (uint64, error)
	CommittedWrite(key string, value []byte, version uint64) error
	CommittedWriteNewVersion(key string, value []byte) (uint64, error)
	CommittedRead(key string) ([]byte, error)
	CommitVersion(key string, version uint64) error
	SendKeys(sendFunc func([]string) error, keyFilter storage.KeyFilter) error
}

type Client interface {
	Write(address net.Addr, key string, value []byte, version uint64) error
	Read(address net.Addr, key string) ([]byte, error)
}

type ChainNode struct {
	// The address of this node.
	address net.Addr
	// The local storage for this node.
	store Storage
	// A client for communicating with other nodes in the chain.
	client Client
	// Metadata for chain membership.
	membership atomic.Pointer[metadata.ChainMetadata]
}

func NewChainNode(address net.Addr, store Storage, client Client) *ChainNode {
	return &ChainNode{address: address, store: store, client: client}
}

func (c *ChainNode) WriteWithVersion(key string, value []byte, version uint64) error {
	// Ensure this node is a member of a chain.
	membership := c.membership.Load()
	if membership == nil {
		return ErrNotMemberOfChain
	}

	// If this node is the tail, the write can be committed immediately.
	// Otherwise, the write needs to be forwarded to the next node in the chain.
	succ, err := membership.Successor(c.address)
	if err != nil {
		return err
	}
	if succ == nil {
		return c.store.CommittedWrite(key, value, version)
	}
	if err := c.store.UncommittedWrite(key, value, version); err != nil {
		return err
	}
	if err := c.client.Write(succ, key, value, version); err != nil {
		return err
	}
	return c.store.CommitVersion(key, version)
}

func (c *ChainNode) InitiateReplicatedWrite(key string, value []byte) error {
	// Ensure this node is the head of the chain. Writes must always be initiated at the head.
	membership := c.membership.Load()
	if membership == nil {
		return ErrNotMemberOfChain
	}
	isHead := membership.IsHead(c.address)
	if !isHead {
		return ErrNotHead
	}

	// If this node is the tail, the write can be committed immediately.
	// Otherwise, the write needs to be forwarded to the next node in the chain.
	succ, err := membership.Successor(c.address)
	if err != nil {
		return err
	}
	if succ == nil {
		_, err := c.store.CommittedWriteNewVersion(key, value)
		return err
	}
	version, err := c.store.UncommittedWriteNewVersion(key, value)
	if err != nil {
		return err
	}
	if err := c.client.Write(succ, key, value, version); err != nil {
		return err
	}
	return c.store.CommitVersion(key, version)
}

func (c *ChainNode) Read(key string) ([]byte, error) {
	// Ensure this node is a member of a chain.
	membership := c.membership.Load()
	if membership == nil {
		return nil, ErrNotMemberOfChain
	}

	// If the key is committed, it's safe to perform a local read.
	// Otherwise, it needs to be read from the tail.
	value, err := c.store.CommittedRead(key)
	if errors.Is(err, storage.ErrDirtyRead) {
		tail := membership.Tail()
		return c.client.Read(tail, key)
	}
	return value, err
}
