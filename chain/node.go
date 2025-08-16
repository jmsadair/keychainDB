package chain

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/jmsadair/zebraos/storage"
)

var (
	// Indicates that a node is not a head of a chain.
	ErrNotHead = errors.New("writes must be initiated from the head of the chain")
	// Indicates a node is in the syncing state.
	ErrSyncing = errors.New("syncing and cannot server reads as this time")
)

const (
	defaultBufferedChSize = 256
	numOnCommitWorkers    = 16
)

type persistantStrorage interface {
	UncommittedWrite(key string, value []byte, version uint64) error
	UncommittedWriteNewVersion(key string, value []byte) (uint64, error)
	CommittedWrite(key string, value []byte, version uint64) error
	CommittedWriteNewVersion(key string, value []byte) (uint64, error)
	CommittedRead(key string) ([]byte, error)
	CommitVersion(key string, version uint64) error
	SendKeyValuePairs(ctx context.Context, sendFunc func(ctx context.Context, kvPairs []storage.KeyValuePair) error, keyFilter storage.KeyFilter) error
	CommitAll(ctx context.Context, onCommit func(ctx context.Context, key string, version uint64) error) error
}

type chainClient interface {
	Write(ctx context.Context, address net.Addr, key string, value []byte, version uint64) error
	Read(ctx context.Context, address net.Addr, key string) ([]byte, error)
	Commit(ctx context.Context, address net.Addr, key string, version uint64) error
	Propagate(ctx context.Context, address net.Addr, keyFilter storage.KeyFilter) (KeyValueRecieveStream, error)
}

type onConfigChangeMessage struct {
	config *ChainConfiguration
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

type chainNode struct {
	address          net.Addr
	store            persistantStrorage
	client           chainClient
	onCommitCh       chan onCommitMessage
	onConfigChangeCh chan onConfigChangeMessage
	syncCompleteCh   chan any
	state            atomic.Pointer[State]
}

func newChainNode(address net.Addr, store persistantStrorage, client chainClient) *chainNode {
	state := &State{config: EmptyChain, status: Inactive}
	node := &chainNode{
		address:          address,
		store:            store,
		client:           client,
		onCommitCh:       make(chan onCommitMessage, defaultBufferedChSize),
		onConfigChangeCh: make(chan onConfigChangeMessage),
		syncCompleteCh:   make(chan any),
	}
	node.state.Store(state)
	return node
}

func (c *chainNode) writeWithVersion(ctx context.Context, key string, value []byte, version uint64) error {
	state := c.state.Load()
	if !state.config.IsMember(c.address) {
		return ErrNotMemberOfChain
	}

	succ := state.config.Successor(c.address)
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

	return c.client.Write(ctx, succ, key, value, version)
}

func (c *chainNode) initiateReplicatedWrite(ctx context.Context, key string, value []byte) error {
	state := c.state.Load()
	if !state.config.IsMember(c.address) {
		return ErrNotMemberOfChain
	}
	if !state.config.IsHead(c.address) {
		return ErrNotHead
	}

	succ := state.config.Successor(c.address)
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

func (c *chainNode) commit(ctx context.Context, key string, version uint64) error {
	state := c.state.Load()
	if !state.config.IsMember(c.address) {
		return ErrNotMemberOfChain
	}

	if err := c.store.CommitVersion(key, version); err != nil {
		return err
	}
	c.onCommitCh <- onCommitMessage{key: key, version: version}

	return nil
}

func (c *chainNode) read(ctx context.Context, key string) ([]byte, error) {
	state := c.state.Load()
	if !state.config.IsMember(c.address) {
		return nil, ErrNotMemberOfChain
	}

	if state.status == Syncing {
		return nil, ErrSyncing
	}

	value, err := c.store.CommittedRead(key)
	if err != nil && errors.Is(err, storage.ErrDirtyRead) {
		tail := state.config.Tail()
		return c.client.Read(ctx, tail, key)
	}

	return value, err
}

func (c *chainNode) onCommitRoutine(ctx context.Context) {
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

func (c *chainNode) onCommit(ctx context.Context, key string, version uint64) error {
	state := c.state.Load()
	if !state.config.IsMember(c.address) {
		return ErrNotMemberOfChain
	}

	err := c.store.CommitVersion(key, version)
	if err != nil {
		return err
	}
	pred := state.config.Predecessor(c.address)
	if pred == nil {
		return nil
	}

	return c.client.Commit(ctx, pred, key, version)
}

func (c *chainNode) propagate(ctx context.Context, keyFilter storage.KeyFilter, stream KeyValueSendStream) error {
	state := c.state.Load()
	if !state.config.IsMember(c.address) {
		return ErrNotMemberOfChain
	}
	if state.status == Syncing {
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

func (c *chainNode) requestPropagation(ctx context.Context, address net.Addr, keyFilter storage.KeyFilter, isTail bool) error {
	stream, err := c.client.Propagate(ctx, address, keyFilter)
	if err != nil {
		return err
	}

	for {
		kvPair, err := stream.Recieve()
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

func (c *chainNode) updateConfiguration(ctx context.Context, config *ChainConfiguration) error {
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

func (c *chainNode) onConfigChangeRoutine(ctx context.Context) {
	var onNewPredTask, onNewSuccTask cancellableTask

	runNewPredecessorTask := func(config *ChainConfiguration, isSyncing bool) {
		onNewPredTask.run(ctx, func(ctx context.Context) { c.onNewPredecessor(ctx, config, isSyncing) })
	}
	runNewSuccessorTask := func(config *ChainConfiguration, isSyncing bool) {
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
			newState := &State{config: state.config, status: Active}
			c.state.Store(newState)
		case msg, ok := <-c.onConfigChangeCh:
			if !ok {
				return
			}

			state := c.state.Load()
			newState := &State{config: msg.config, status: state.status}
			lostMembership := state.config.IsMember(c.address) && !msg.config.IsMember(c.address)
			isNewMember := !state.config.IsMember(c.address) && msg.config.IsMember(c.address)
			hasNewPred := state.config.Predecessor(c.address) != msg.config.Predecessor(c.address)
			hasNewSucc := state.config.Successor(c.address) != msg.config.Successor(c.address)

			if lostMembership {
				onNewPredTask.cancelAndWait()
				onNewSuccTask.cancelAndWait()
				newState.config = EmptyChain
				newState.status = Inactive
				c.state.Store(newState)
				continue
			}
			if isNewMember {
				onNewPredTask.cancelAndWait()
				onNewSuccTask.cancelAndWait()
				newState.status = Syncing
				if !hasNewPred && !hasNewSucc {
					newState.status = Active
				}
			}
			if hasNewPred {
				onNewPredTask.cancelAndWait()
				runNewPredecessorTask(msg.config, newState.status == Syncing)
			}
			if hasNewSucc {
				onNewSuccTask.cancelAndWait()
				runNewSuccessorTask(msg.config, newState.status == Syncing)
			}

			c.state.Store(newState)
			close(msg.doneCh)
		}
	}
}

func (c *chainNode) onNewSuccessor(ctx context.Context, config *ChainConfiguration, isSyncing bool) {
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

func (c *chainNode) onNewPredecessor(ctx context.Context, config *ChainConfiguration, isSyncing bool) {
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
