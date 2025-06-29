package chainnode

import (
	"net"
	"sync"
)

type Storage interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
}

type Client interface {
	Put(address net.Addr, key string, value []byte) error
	Delete(address net.Addr, key string) error
}

type ChainNode struct {
	// The address of this node.
	address net.Addr
	// The address of this node's predecessor in the chain.
	// This value is nil if this node is the head.
	pred net.Addr
	// The address of this node's successor in the chain.
	// This value is nil if this node is the tail.
	succ net.Addr
	// The local storage for this node.
	storage Storage
	// A client for communicating with other nodes in the chain.
	client Client

	mu sync.RWMutex
}

func NewChainNode(address net.Addr, predecessor net.Addr, successor net.Addr, storage Storage, client Client) *ChainNode {
	return &ChainNode{address: address, pred: predecessor, succ: successor, storage: storage, client: client}
}

func (c *ChainNode) Predecessor() net.Addr {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.pred
}

func (c *ChainNode) Successor() net.Addr {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.succ
}

func (c *ChainNode) SetPredecessor(predecessor net.Addr) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pred = predecessor
}

func (c *ChainNode) SetSuccessor(successor net.Addr) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.succ = successor
}

func (c *ChainNode) Put(key string, value []byte) error {
	if err := c.storage.Put(key, value); err != nil {
		return err
	}
	if c.Successor() == nil {
		return nil
	}
	return c.client.Put(c.Successor(), key, value)
}

func (c *ChainNode) Get(key string) ([]byte, error) {
	return c.storage.Get(key)
}

func (c *ChainNode) Delete(key string) error {
	if err := c.storage.Delete(key); err != nil {
		return err
	}
	if c.Successor() == nil {
		return nil
	}
	return c.client.Delete(c.Successor(), key)
}
