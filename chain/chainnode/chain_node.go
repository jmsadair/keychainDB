package chainnode

import (
	"net"

	"github.com/jmsadair/zebraos/chain/storage"
)

type Storage interface {
	UncommittedWrite(key string, value []byte, version uint64) error
	UncommittedWriteNewVersion(key string, value []byte) (uint64, error)
	CommittedWrite(key string, value []byte, version uint64) error
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
	storage Storage
	// A client for communicating with other nodes in the chain.
	client Client
}

func NewChainNode(address net.Addr, storage Storage, client Client) *ChainNode {
	return &ChainNode{address: address, storage: storage, client: client}
}

func (c *ChainNode) WriteWithVersion(key string, value []byte, version uint64) error {
	return nil
}

func (c *ChainNode) ReplicatedWrite(key string, value []byte) error {
	return nil
}

func (c *ChainNode) Read(key string) ([]byte, error) {
	return nil, nil
}
