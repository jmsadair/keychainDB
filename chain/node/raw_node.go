package node

import (
	"net"
	"sync"
)

type Storage interface {
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
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

	mu sync.RWMutex
}

func newRawNode(address net.Addr, predecessor net.Addr, successor net.Addr, storage Storage) *rawNode {
	return &rawNode{address: address, pred: predecessor, succ: successor, storage: storage}
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

func (r *rawNode) set(key string, value []byte) error {
	return r.storage.Set(key, value)
}

func (r *rawNode) get(key string) ([]byte, error) {
	return r.storage.Get(key)
}
