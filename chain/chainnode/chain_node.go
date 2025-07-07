package chainnode

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/jmsadair/zebraos/chain/metadata"
)

type Storage interface {
	UncommittedWrite(key string, value []byte, version uint64) error
	UncommittedWriteNewVersion(key string, value []byte) (uint64, error)
	CommittedWrite(key string, value []byte, version uint64) error
	CommittedRead(key string) ([]byte, error)
}

type Client interface {
	Write(address net.Addr, key string, value []byte, version uint64) error
	Read(address net.Addr, key string) ([]byte, error)
}

type ChainNode struct {
	// The address of this node.
	address net.Addr
	// The metadata that describes the chain this node is a member of.
	// This value will be nil unless the node has been added to a chain.
	chainMetadata atomic.Pointer[metadata.ChainMetadata]
	// The local storage for this node.
	storage Storage
	// A client for communicating with other nodes in the chain.
	client Client

	mu sync.RWMutex
}

func NewChainNode(address net.Addr, storage Storage, client Client) *ChainNode {
	return &ChainNode{address: address, storage: storage, client: client}
}

func (c *ChainNode) WriteWithVersion(key string, value []byte, version uint64) error {
	metadata := c.chainMetadata.Load()
	if metadata == nil {
		return errors.New("not a member of a chain")
	}
	if metadata.IsTail(c.address) {
		return c.storage.CommittedWrite(key, value, version)
	}
	if err := c.storage.UncommittedWrite(key, value, version); err != nil {
		return err
	}

	successor, err := metadata.Successor(c.address)
	if err != nil {
		return err
	}
	return c.client.Write(successor, key, value, version)
}

func (c *ChainNode) ReplicatedWrite(key string, value []byte) error {
	metadata := c.chainMetadata.Load()
	if metadata == nil {
		return errors.New("not a member of a chain")
	}
	if !metadata.IsHead(c.address) {
		return errors.New("only the head of a chain can initiate replicated writes")
	}
	version, err := c.storage.UncommittedWriteNewVersion(key, value)
	if err != nil {
		return err
	}
	return c.client.Write(c.address, key, value, version)
}

func (c *ChainNode) Read(key string) ([]byte, error) {
	return nil, nil
}
