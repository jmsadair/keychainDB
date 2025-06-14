package node

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

type rawNode struct {
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

func newRawNode(address net.Addr, predecessor net.Addr, successor net.Addr, storage Storage, client Client) *rawNode {
	return &rawNode{address: address, pred: predecessor, succ: successor, storage: storage, client: client}
}

func (r *rawNode) predecessor() net.Addr {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pred
}

func (r *rawNode) successor() net.Addr {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.succ
}

func (r *rawNode) setPredecessor(predecessor net.Addr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pred = predecessor
}

func (r *rawNode) setSuccessor(successor net.Addr) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.succ = successor
}

func (r *rawNode) put(key string, value []byte) error {
	if err := r.storage.Put(key, value); err != nil {
		return err
	}
	if r.successor() == nil {
		return nil
	}
	return r.client.Put(r.successor(), key, value)
}

func (r *rawNode) get(key string) ([]byte, error) {
	return r.storage.Get(key)
}

func (r *rawNode) delete(key string) error {
	if err := r.storage.Delete(key); err != nil {
		return err
	}
	if r.successor() == nil {
		return nil
	}
	return r.client.Delete(r.successor(), key)
}
