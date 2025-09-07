package chain

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/jmsadair/keychain/chain/storage"
)

var (
	// Indicates that a node is not a head of a chain.
	ErrNotHead = errors.New("writes must be initiated from the head of the chain")
	// Indicates a node is in the syncing state.
	ErrSyncing = errors.New("syncing and cannot serve reads at this time")
	// Indicates that a node is not a member of any chain.
	ErrNotMemberOfChain = errors.New("not a member of the chain")
)

// KeyValueSendStream is a stream for sending key-value pairs.
type KeyValueSendStream interface {
	// Send sends the key-valur pair over the stream.
	Send(*storage.KeyValuePair) error
}

const (
	defaultBufferedChSize = 256
	numOnCommitWorkers    = 16
)

type onConfigChangeMessage struct {
	config *Configuration
	doneCh chan bool
}

type onCommitMessage struct {
	key     string
	version uint64
}

type cancellableTask struct {
	cancel func()
	wg     sync.WaitGroup
}

func (ct *cancellableTask) cancelAndWait() {
	if ct.cancel != nil {
		ct.cancel()
		ct.cancel = nil
		ct.wg.Wait()
	}
}

func (ct *cancellableTask) run(ctx context.Context, fn func(ctx context.Context)) {
	ct.cancelAndWait()
	var taskCtx context.Context
	taskCtx, ct.cancel = context.WithCancel(ctx)
	ct.wg.Add(1)
	go func() {
		defer ct.wg.Done()
		fn(taskCtx)
	}()
}

// ChainNode represents a node in a chain replication system.
type ChainNode struct {
	address          net.Addr
	store            Storage
	tn               Transport
	onCommitCh       chan onCommitMessage
	onConfigChangeCh chan onConfigChangeMessage
	syncCompleteCh   chan any
	state            atomic.Pointer[State]
}

// NewChainNode creates a new ChainNode instance with the given address, storage, and transport.
// The node starts with an inactive status and an empty chain configuration.
func NewChainNode(address net.Addr, store Storage, tn Transport) *ChainNode {
	state := &State{Config: EmptyChain, Status: Inactive}
	node := &ChainNode{
		address:          address,
		store:            store,
		tn:               tn,
		onCommitCh:       make(chan onCommitMessage, defaultBufferedChSize),
		onConfigChangeCh: make(chan onConfigChangeMessage),
		syncCompleteCh:   make(chan any),
	}
	node.state.Store(state)
	return node
}

// Run will start this node.
func (c *ChainNode) Run(ctx context.Context) {
	go c.onCommitRoutine(ctx)
	go c.onConfigChangeRoutine(ctx)
}

// WriteWithVersion performs a write operation with a specific version number.
// This is used for replicated writes that are part of the chain replication protocol.
func (c *ChainNode) WriteWithVersion(ctx context.Context, key string, value []byte, version uint64) error {
	state := c.state.Load()
	if !state.Config.IsMember(c.address) {
		return ErrNotMemberOfChain
	}

	succ := state.Config.Successor(c.address)
	if succ == nil {
		err := c.store.CommittedWrite(key, value, version)
		if err != nil {
			return err
		}
		c.onCommitCh <- onCommitMessage{key: key, version: version}
		return nil
	}
	if err := c.store.UncommittedWrite(key, value, version); err != nil {
		return err
	}

	return c.tn.Write(ctx, succ, key, value, version)
}

// InitiateReplicatedWrite starts a new replicated write operation from the head of the chain.
// This method can only be called on the head node and will generate a new version number.
func (c *ChainNode) InitiateReplicatedWrite(ctx context.Context, key string, value []byte) error {
	state := c.state.Load()
	if !state.Config.IsMember(c.address) {
		return ErrNotMemberOfChain
	}
	if !state.Config.IsHead(c.address) {
		return ErrNotHead
	}

	succ := state.Config.Successor(c.address)
	if succ == nil {
		_, err := c.store.CommittedWriteNewVersion(key, value)
		return err
	}
	version, err := c.store.UncommittedWriteNewVersion(key, value)
	if err != nil {
		return err
	}

	return c.tn.Write(ctx, succ, key, value, version)
}

// Commit commits a previously written version, making it visible for reads.
// This is part of the two-phase commit protocol used in chain replication.
func (c *ChainNode) Commit(ctx context.Context, key string, version uint64) error {
	state := c.state.Load()
	if !state.Config.IsMember(c.address) {
		return ErrNotMemberOfChain
	}

	if err := c.store.CommitVersion(key, version); err != nil {
		return err
	}
	c.onCommitCh <- onCommitMessage{key: key, version: version}

	return nil
}

// Read retrieves the committed value for the given key.
// If the local store has uncommitted data, it forwards the read to the tail node.
func (c *ChainNode) Read(ctx context.Context, key string) ([]byte, error) {
	state := c.state.Load()
	if !state.Config.IsMember(c.address) {
		return nil, ErrNotMemberOfChain
	}

	if state.Status == Syncing {
		return nil, ErrSyncing
	}

	value, err := c.store.CommittedRead(key)
	if err != nil && errors.Is(err, storage.ErrDirtyRead) {
		tail := state.Config.Tail()
		return c.tn.Read(ctx, tail, key)
	}

	return value, err
}

// Propagate sends key-value pairs to a requesting node through the provided stream.
func (c *ChainNode) Propagate(ctx context.Context, keyFilter storage.KeyFilter, stream KeyValueSendStream) error {
	state := c.state.Load()
	if !state.Config.IsMember(c.address) {
		return ErrNotMemberOfChain
	}
	if state.Status == Syncing {
		return ErrSyncing
	}

	sendFunc := func(ctx context.Context, kvPairs []storage.KeyValuePair) error {
		for _, kvPair := range kvPairs {
			if err := stream.Send(&kvPair); err != nil {
				return err
			}
		}
		return nil
	}

	return c.store.SendKeyValuePairs(ctx, sendFunc, keyFilter)
}

// UpdateConfiguration updates the chain configuration for this node.
func (c *ChainNode) UpdateConfiguration(ctx context.Context, config *Configuration) error {
	msg := onConfigChangeMessage{config: config, doneCh: make(chan bool)}

	select {
	case c.onConfigChangeCh <- msg:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-msg.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *ChainNode) requestPropagation(ctx context.Context, address net.Addr, keyFilter storage.KeyFilter, isTail bool) error {
	stream, err := c.tn.Propagate(ctx, address, keyFilter)
	if err != nil {
		return err
	}

	for {
		kvPair, err := stream.Receive()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		shouldCommit := kvPair.Committed || isTail
		if shouldCommit {
			err = c.store.CommittedWrite(kvPair.Key, kvPair.Value, kvPair.Version)
			if err == nil {
				select {
				case <-ctx.Done():
					return nil
				case c.onCommitCh <- onCommitMessage{key: kvPair.Key, version: kvPair.Version}:
				}
			}

		} else {
			err = c.store.UncommittedWrite(kvPair.Key, kvPair.Value, kvPair.Version)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *ChainNode) onCommit(ctx context.Context, key string, version uint64) error {
	state := c.state.Load()
	if !state.Config.IsMember(c.address) {
		return ErrNotMemberOfChain
	}

	err := c.store.CommitVersion(key, version)
	if err != nil {
		return err
	}
	pred := state.Config.Predecessor(c.address)
	if pred == nil {
		return nil
	}

	return c.tn.Commit(ctx, pred, key, version)
}

func (c *ChainNode) onCommitRoutine(ctx context.Context) {
	workerCh := make(chan onCommitMessage)
	worker := func() {
		for msg := range workerCh {
			c.onCommit(ctx, msg.key, msg.version)
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

func (c *ChainNode) onConfigChangeRoutine(ctx context.Context) {
	var onNewPredTask, onNewSuccTask cancellableTask

	runNewPredecessorTask := func(config *Configuration, isSyncing bool) {
		onNewPredTask.run(ctx, func(ctx context.Context) { c.onNewPredecessor(ctx, config, isSyncing) })
	}
	runNewSuccessorTask := func(config *Configuration, isSyncing bool) {
		onNewSuccTask.run(ctx, func(ctx context.Context) {
			c.onNewSuccessor(ctx, config, isSyncing)
		})
	}
	defer func() {
		onNewPredTask.cancelAndWait()
		onNewSuccTask.cancelAndWait()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.syncCompleteCh:
			state := c.state.Load()
			newState := &State{Config: state.Config, Status: Active}
			c.state.Store(newState)
		case msg, ok := <-c.onConfigChangeCh:
			if !ok {
				return
			}

			state := c.state.Load()
			newState := &State{Config: msg.config, Status: state.Status}
			lostMembership := state.Config.IsMember(c.address) && !msg.config.IsMember(c.address)
			isNewMember := !state.Config.IsMember(c.address) && msg.config.IsMember(c.address)
			hasNewPred := state.Config.Predecessor(c.address) != msg.config.Predecessor(c.address)
			hasNewSucc := state.Config.Successor(c.address) != msg.config.Successor(c.address)

			if lostMembership {
				onNewPredTask.cancelAndWait()
				onNewSuccTask.cancelAndWait()
				newState.Config = EmptyChain
				newState.Status = Inactive
				c.state.Store(newState)
				continue
			}
			if isNewMember {
				onNewPredTask.cancelAndWait()
				onNewSuccTask.cancelAndWait()
				newState.Status = Syncing
				if !hasNewPred && !hasNewSucc {
					newState.Status = Active
				}
			}
			if hasNewPred {
				onNewPredTask.cancelAndWait()
				runNewPredecessorTask(msg.config, newState.Status == Syncing)
			}
			if hasNewSucc {
				onNewSuccTask.cancelAndWait()
				runNewSuccessorTask(msg.config, newState.Status == Syncing)
			}

			c.state.Store(newState)
			close(msg.doneCh)
		}
	}
}

func (c *ChainNode) onNewSuccessor(ctx context.Context, config *Configuration, isSyncing bool) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			keyFilter := storage.CommittedKeys
			if isSyncing {
				keyFilter = storage.AllKeys
			}
			if succ := config.Successor(c.address); succ != nil {
				if err := c.requestPropagation(ctx, succ, keyFilter, config.IsTail(c.address)); err != nil {
					continue
				}
			} else {
				onCommit := func(ctx context.Context, key string, version uint64) error {
					select {
					case <-ctx.Done():
						return nil
					case c.onCommitCh <- onCommitMessage{key: key, version: version}:
					}
					return nil
				}
				if err := c.store.CommitAll(ctx, onCommit); err != nil {
					continue
				}
			}
			if isSyncing {
				select {
				case c.syncCompleteCh <- struct{}{}:
				case <-ctx.Done():
				}
			}
		}
		break
	}
}

func (c *ChainNode) onNewPredecessor(ctx context.Context, config *Configuration, isSyncing bool) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			keyFilter := storage.DirtyKeys
			if isSyncing {
				keyFilter = storage.AllKeys
			}
			if pred := config.Predecessor(c.address); pred != nil {
				err := c.requestPropagation(ctx, pred, keyFilter, config.IsTail(c.address))
				if err != nil {
					continue
				}
			}
			if isSyncing {
				select {
				case c.syncCompleteCh <- struct{}{}:
				case <-ctx.Done():
				}
			}
		}
		break
	}
}
