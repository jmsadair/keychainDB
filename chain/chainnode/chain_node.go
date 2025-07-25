package chainnode

import (
	"context"
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
	SendKeyValuePairs(ctx context.Context, sendFunc func(ctx context.Context, kvPairs []storage.KeyValuePair) error, keyFilter storage.KeyFilter) error
}

type Client interface {
	Write(ctx context.Context, address net.Addr, key string, value []byte, version uint64) error
	Read(ctx context.Context, address net.Addr, key string) ([]byte, error)
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

func (c *ChainNode) WriteWithVersion(ctx context.Context, key string, value []byte, version uint64) error {
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
	if err := c.client.Write(ctx, succ, key, value, version); err != nil {
		return err
	}
	return c.store.CommitVersion(key, version)
}

func (c *ChainNode) InitiateReplicatedWrite(ctx context.Context, key string, value []byte) error {
	membership := c.membership.Load()
	if membership == nil {
		return ErrNotMemberOfChain
	}
	isHead := membership.IsHead(c.address)
	if !isHead {
		return ErrNotHead
	}

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
	if err := c.client.Write(ctx, succ, key, value, version); err != nil {
		return err
	}
	return c.store.CommitVersion(key, version)
}

func (c *ChainNode) Read(ctx context.Context, key string) ([]byte, error) {
	membership := c.membership.Load()
	if membership == nil {
		return nil, ErrNotMemberOfChain
	}

	value, err := c.store.CommittedRead(key)
	if err != nil && errors.Is(err, storage.ErrDirtyRead) {
		tail := membership.Tail()
		return c.client.Read(ctx, tail, key)
	}

	return value, err
}

func (c *ChainNode) ListKeyValuePairs(ctx context.Context, sendFunc func(ctx context.Context, kvPairs []storage.KeyValuePair) error) error {
	return c.store.SendKeyValuePairs(ctx, sendFunc, storage.AllKeys)
}
