package chain

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/jmsadair/zebraos/internal/storage"
)

const (
	msgChSize          = 256
	numOnCommitWorkers = 16
)

var ErrNotHead = errors.New("writes must be initiated from the head of the chain")

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
	Commit(ctx context.Context, address net.Addr, key string, version uint64) error
}

type OnCommitMessage struct {
	Key     string
	Version uint64
}

type ChainNode struct {
	// The address of this node.
	address net.Addr
	// The local storage for this node.
	store Storage
	// A client for communicating with other nodes in the chain.
	client Client
	// Channel used to send messages to background routine when a key is committed.
	onCommitCh chan OnCommitMessage
	// Chain membership information for this node.
	chainConfig atomic.Pointer[ChainConfiguration]
}

func NewChainNode(address net.Addr, store Storage, client Client) *ChainNode {
	return &ChainNode{address: address, store: store, client: client, onCommitCh: make(chan OnCommitMessage, msgChSize)}
}

func (c *ChainNode) WriteWithVersion(ctx context.Context, key string, value []byte, version uint64) error {
	// Ensure this node is a member of a chain.
	membership := c.chainConfig.Load()
	if membership == nil {
		return ErrNotMemberOfChain
	}

	succ, err := membership.Successor(c.address)
	if err != nil {
		return err
	}

	// If this node is the tail, the write can be committed immediately.
	if succ == nil {
		err := c.store.CommittedWrite(key, value, version)
		if err != nil {
			return err
		}
		c.onCommitCh <- OnCommitMessage{Key: key, Version: version}
		return nil
	}

	// Otherwise, the write needs to be forwarded to the next node in the chain.
	if err := c.store.UncommittedWrite(key, value, version); err != nil {
		return err
	}
	return c.client.Write(ctx, succ, key, value, version)
}

func (c *ChainNode) InitiateReplicatedWrite(ctx context.Context, key string, value []byte) error {
	membership := c.chainConfig.Load()
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
	return c.client.Write(ctx, succ, key, value, version)
}

func (c *ChainNode) Commit(ctx context.Context, key string, version uint64) error {
	membership := c.chainConfig.Load()
	if membership == nil {
		return ErrNotMemberOfChain
	}

	if err := c.store.CommitVersion(key, version); err != nil {
		return err
	}
	c.onCommitCh <- OnCommitMessage{Key: key, Version: version}

	return nil
}

func (c *ChainNode) Read(ctx context.Context, key string) ([]byte, error) {
	membership := c.chainConfig.Load()
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

func (c *ChainNode) OnCommitRoutine(ctx context.Context) {
	workerCh := make(chan OnCommitMessage)
	worker := func() {
		for msg := range workerCh {
			c.OnCommit(ctx, msg.Key, msg.Version)
		}
	}

	var wg sync.WaitGroup
	wg.Add(numOnCommitWorkers)
	for range numOnCommitWorkers {
		go worker()
	}
	defer func() {
		close(workerCh)
		wg.Wait()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-c.onCommitCh:
			workerCh <- msg
		}
	}
}

func (c *ChainNode) OnCommit(ctx context.Context, key string, version uint64) error {
	err := c.store.CommitVersion(key, version)
	if err != nil {
		return err
	}
	m := c.chainConfig.Load()
	if m == nil {
		return nil
	}
	pred, err := m.Predecessor(c.address)
	if err != nil {
		return err
	}
	if pred == nil {
		return nil
	}
	return c.client.Commit(ctx, pred, key, version)
}

func (c *ChainNode) BackfillAllKeyValuePairs(ctx context.Context, sendFunc func(ctx context.Context, kvPairs []storage.KeyValuePair) error) error {
	return c.store.SendKeyValuePairs(ctx, sendFunc, storage.AllKeys)
}

func (c *ChainNode) BackfillDirtyKeyValuePairs(ctx context.Context, sendFunc func(ctx context.Context, kvPairs []storage.KeyValuePair) error) error {
	return c.store.SendKeyValuePairs(ctx, sendFunc, storage.DirtyKeys)
}

func (c *ChainNode) BackfillCommittedKeyValuePairs(ctx context.Context, sendFunc func(ctx context.Context, kvPairs []storage.KeyValuePair) error) error {
	return c.store.SendKeyValuePairs(ctx, sendFunc, storage.CommittedKeys)
}
