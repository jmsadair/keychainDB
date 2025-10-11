package node

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/jmsadair/keychain/chain/storage"
	pb "github.com/jmsadair/keychain/proto/chain"
	storagepb "github.com/jmsadair/keychain/proto/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var (
	ErrSyncing              = errors.New("chainserver: cannot serve reads while syncing")
	ErrNotMemberOfChain     = errors.New("chainserver: not member of chain")
	ErrInvalidConfigVersion = errors.New("chainserver: configuration version mismatch")
	ErrNotHead              = errors.New("chainserver: chain head must serve writes")
)

const (
	defaultBufferedChSize = 256
	numOnCommitWorkers    = 16
)

// Status represents the operational status of a chain node.
type Status int32

const (
	// Unknown indicates the status of the node is not known.
	Unknown Status = iota
	// Syncing indicates the node is synchronizing with the chain.
	Syncing
	// Active indicates the node is actively participating in the chain.
	Active
	// Inactive indicates the node is not participating in any chain.
	Inactive
)

// State contains the membership configuration and status of a chain node.
type State struct {
	// The membership configuration for a chain node.
	Config *Configuration
	// The operation status of a chain node.
	Status Status
}

// Storage defines the interface for persistent storage operations on a chain node.
type Storage interface {
	// UncommittedWrite writes a versioned key-value pair to storage without committing it.
	UncommittedWrite(key string, value []byte, version uint64) error
	// UncommittedWriteNewVersion generates a new version number and writes the
	// key-value pair to storage without committing it.
	UncommittedWriteNewVersion(key string, value []byte) (uint64, error)
	// CommittedWrite writes a versioned key-value pair to storage and immediately commits it.
	// If a later version has already been committed, this operation is a no-op.
	CommittedWrite(key string, value []byte, version uint64) error
	// CommittedWriteNewVersion generates a new version number, writes the
	// key-value pair to storage, and immediately commits it.
	CommittedWriteNewVersion(key string, value []byte) (uint64, error)
	// CommittedRead reads the committed version of a key-value pair.
	CommittedRead(key string) ([]byte, error)
	// CommitVersion commits the provided version of the key. If a later version already
	// exists, this operation is a no-op.
	CommitVersion(key string, version uint64) error
	// SendKeyValuePairs iterates over storage, filters key-value pairs according to the key filter,
	// and invokes the callback for each.
	SendKeyValuePairs(ctx context.Context, sendFunc func(ctx context.Context, kvPairs []storage.KeyValuePair) error, keyFilter storage.KeyFilter) error
	// CommitAll commits all dirty keys in storage and invokes the provided callback for each.
	CommitAll(ctx context.Context, onCommit func(ctx context.Context, key string, version uint64) error) error
}

// Transport defines the interface for chain node communication.
type Transport interface {
	// Write will write a versioned key-value pair.
	Write(ctx context.Context, address string, request *pb.WriteRequest) (*pb.WriteResponse, error)
	// Read will read the committed version of the key.
	Read(ctx context.Context, address string, request *pb.ReadRequest) (*pb.ReadResponse, error)
	// Commit will commit the provided version of the key.
	Commit(ctx context.Context, address string, request *pb.CommitRequest) (*pb.CommitResponse, error)
	// Propagate will initiate a stream of key-value pairs from another node.
	Propagate(ctx context.Context, address string, request *pb.PropagateRequest) (grpc.ServerStreamingClient[storagepb.KeyValuePair], error)
}

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
	// The ID for this node.
	ID string
	// The address for this node.
	Address string

	log              *slog.Logger
	store            Storage
	tn               Transport
	onCommitCh       chan onCommitMessage
	onConfigChangeCh chan onConfigChangeMessage
	syncCompleteCh   chan any
	state            atomic.Pointer[State]
}

// NewChainNode creates a new ChainNode.
func NewChainNode(id, address string, store Storage, tn Transport, log *slog.Logger) *ChainNode {
	state := &State{Config: EmptyChain, Status: Inactive}
	node := &ChainNode{
		ID:               id,
		Address:          address,
		log:              log.With("local-id", id),
		store:            store,
		tn:               tn,
		onCommitCh:       make(chan onCommitMessage, defaultBufferedChSize),
		onConfigChangeCh: make(chan onConfigChangeMessage),
		syncCompleteCh:   make(chan any),
	}
	node.state.Store(state)
	return node
}

// Run will run the node.
func (c *ChainNode) Run(ctx context.Context) {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		c.onCommitRoutine(ctx)
		return nil
	})
	g.Go(func() error {
		c.onConfigChangeRoutine(ctx)
		return nil
	})
	g.Wait()
}

// WriteWithVersion performs a write operation with a specific version number.
// This is used for replicated writes that are part of the chain replication protocol.
func (c *ChainNode) WriteWithVersion(ctx context.Context, request *pb.WriteRequest) (*pb.WriteResponse, error) {
	state := c.state.Load()
	if request.ConfigVersion != state.Config.Version {
		return nil, ErrInvalidConfigVersion
	}
	if !state.Config.IsMemberByID(c.ID) {
		return nil, ErrNotMemberOfChain
	}

	succ := state.Config.Successor(c.ID)
	if succ == nil {
		err := c.store.CommittedWrite(request.Key, request.Value, request.Version)
		if err != nil {
			return nil, err
		}
		c.onCommitCh <- onCommitMessage{key: request.Key, version: request.Version}
		return &pb.WriteResponse{}, nil
	}
	if err := c.store.UncommittedWrite(request.Key, request.Value, request.Version); err != nil {
		return nil, err
	}

	return c.tn.Write(ctx, succ.Address, request)
}

// Replicate starts a new replicated write operation from the head of the chain.
// This method can only be invoked from the head of the chain and will generate a new version number for the key-value pair.
func (c *ChainNode) Replicate(ctx context.Context, request *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	state := c.state.Load()
	if request.ConfigVersion != state.Config.Version {
		return nil, ErrInvalidConfigVersion
	}
	if !state.Config.IsMemberByID(c.ID) {
		return nil, ErrNotMemberOfChain
	}
	if !state.Config.IsHead(c.ID) {
		return nil, ErrNotHead
	}

	succ := state.Config.Successor(c.ID)
	if succ == nil {
		if _, err := c.store.CommittedWriteNewVersion(request.Key, request.Value); err != nil {
			return nil, err
		}
		return &pb.ReplicateResponse{}, nil
	}
	version, err := c.store.UncommittedWriteNewVersion(request.Key, request.Value)
	if err != nil {
		return nil, err
	}

	req := &pb.WriteRequest{Key: request.Key, Value: request.Value, Version: version, ConfigVersion: state.Config.Version}
	if _, err := c.tn.Write(ctx, succ.Address, req); err != nil {
		return nil, err
	}
	return &pb.ReplicateResponse{}, nil
}

// Commit commits a previously written version of a key-value making it visible for reads.
func (c *ChainNode) Commit(ctx context.Context, request *pb.CommitRequest) (*pb.CommitResponse, error) {
	state := c.state.Load()
	if request.ConfigVersion != state.Config.Version {
		return nil, ErrInvalidConfigVersion
	}
	if !state.Config.IsMemberByID(c.ID) {
		return nil, ErrNotMemberOfChain
	}

	if err := c.store.CommitVersion(request.Key, request.Version); err != nil {
		return nil, err
	}
	c.onCommitCh <- onCommitMessage{key: request.Key, version: request.Version}

	return &pb.CommitResponse{}, nil
}

// Read retrieves the committed value for the given key.
// If the local store has uncommitted data, it forwards the read to the tail node.
func (c *ChainNode) Read(ctx context.Context, request *pb.ReadRequest) (*pb.ReadResponse, error) {
	state := c.state.Load()
	if request.ConfigVersion != state.Config.Version {
		return nil, ErrInvalidConfigVersion
	}
	if !state.Config.IsMemberByID(c.ID) {
		return nil, ErrNotMemberOfChain
	}

	// If this node is syncing, try to forward to the predecessor.
	if state.Status == Syncing {
		pred := state.Config.Predecessor(c.ID)
		if pred == nil {
			return nil, ErrSyncing
		}
		req := &pb.ReadRequest{Key: request.Key, ConfigVersion: state.Config.Version, Forwarded: true}
		return c.tn.Read(ctx, pred.Address, req)
	}

	value, err := c.store.CommittedRead(request.Key)

	// If the key-value pair is dirty, forward the request to the tail.
	if err != nil && errors.Is(err, storage.ErrUncommittedRead) {
		// Do not forward to the tail if this request was already forwarded.
		// This is necessary to prevent a recursive RPC loop where the tail
		// is syncing and forwards the request to its predecessor, but the
		// predecessor has yet to commit the key-value pair so it forwards
		// the request to the tail.
		if state.Config.IsTail(c.ID) || request.Forwarded {
			return nil, err
		}
		tail := state.Config.Tail()
		req := &pb.ReadRequest{Key: request.Key, ConfigVersion: state.Config.Version, Forwarded: true}
		return c.tn.Read(ctx, tail.Address, req)
	}
	if err != nil {
		return nil, err
	}

	return &pb.ReadResponse{Value: value}, nil
}

// Propagate sends key-value pairs to a requesting node through the provided stream.
func (c *ChainNode) Propagate(ctx context.Context, request *pb.PropagateRequest, stream grpc.ServerStreamingServer[storagepb.KeyValuePair]) error {
	state := c.state.Load()
	if request.ConfigVersion != state.Config.Version {
		return ErrInvalidConfigVersion
	}
	if !state.Config.IsMemberByID(c.ID) {
		return ErrNotMemberOfChain
	}
	if state.Status == Syncing {
		return ErrSyncing
	}

	sendFunc := func(ctx context.Context, kvPairs []storage.KeyValuePair) error {
		for _, kvPair := range kvPairs {
			if err := stream.Send(kvPair.Proto()); err != nil {
				return err
			}
		}
		return nil
	}

	return c.store.SendKeyValuePairs(ctx, sendFunc, storage.KeyFilterFromProto(request.KeyType))
}

// UpdateConfiguration updates the chain configuration for this node.
func (c *ChainNode) UpdateConfiguration(ctx context.Context, request *pb.UpdateConfigurationRequest) (*pb.UpdateConfigurationResponse, error) {
	msg := onConfigChangeMessage{config: NewConfigurationFromProto(request.Configuration), doneCh: make(chan bool)}

	select {
	case c.onConfigChangeCh <- msg:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case <-msg.doneCh:
		return &pb.UpdateConfigurationResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *ChainNode) Ping(request *pb.PingRequest) *pb.PingResponse {
	state := c.state.Load()
	return &pb.PingResponse{Status: int32(state.Status), ConfigVersion: state.Config.Version}
}

func (c *ChainNode) Configuration() *Configuration {
	state := c.state.Load()
	return state.Config
}

func (c *ChainNode) Status() Status {
	state := c.state.Load()
	return state.Status
}

func (c *ChainNode) Store() Storage {
	return c.store
}

func (c *ChainNode) requestPropagation(ctx context.Context, member *ChainMember, keyFilter storage.KeyFilter, config *Configuration) error {
	req := &pb.PropagateRequest{KeyType: keyFilter.Proto(), ConfigVersion: config.Version}
	isTail := config.IsTail(c.ID)

	stream, err := c.tn.Propagate(ctx, member.Address, req)
	if err != nil {
		return err
	}

	c.log.InfoContext(
		ctx,
		"initiating propagation of keys",
		"key-filter",
		keyFilter.String(),
		"remote-id",
		member.ID,
		"remote-address",
		member.Address,
	)

	for {
		kvPair, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			c.log.ErrorContext(
				ctx,
				"key propagation failed",
				"error",
				err.Error(),
				"key-filter",
				keyFilter.String(),
				"remote-id",
				member.ID,
				"remote-address",
				member.Address,
			)
			return err
		}

		shouldCommit := kvPair.GetIsCommitted() || isTail
		if shouldCommit {
			err = c.store.CommittedWrite(kvPair.GetKey(), kvPair.GetValue(), kvPair.GetVersion())
			if err == nil {
				select {
				case <-ctx.Done():
					return nil
				case c.onCommitCh <- onCommitMessage{key: kvPair.GetKey(), version: kvPair.GetVersion()}:
				}
			}

		} else {
			err = c.store.UncommittedWrite(kvPair.GetKey(), kvPair.GetValue(), kvPair.GetVersion())
		}

		if err != nil {
			c.log.ErrorContext(
				ctx,
				"key propagation failed",
				"error",
				err.Error(),
				"key-filter",
				keyFilter.String(),
				"remote-id",
				member.ID,
				"remote-address",
				member.Address,
			)
			return err
		}
	}

	c.log.InfoContext(
		ctx,
		"key propagation succeeded",
		"key-filter",
		keyFilter.String(),
		"remote-id",
		member.ID,
		"remote-address",
		member.Address,
	)

	return nil
}

func (c *ChainNode) onCommit(ctx context.Context, key string, version uint64) error {
	state := c.state.Load()
	if !state.Config.IsMemberByID(c.ID) {
		return ErrNotMemberOfChain
	}

	err := c.store.CommitVersion(key, version)
	if err != nil {
		return err
	}
	pred := state.Config.Predecessor(c.ID)
	if pred == nil {
		return nil
	}

	req := &pb.CommitRequest{Key: key, Version: version}
	_, err = c.tn.Commit(ctx, pred.Address, req)
	return err
}

func (c *ChainNode) onCommitRoutine(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(numOnCommitWorkers)
	workerCh := make(chan onCommitMessage)
	worker := func() {
		defer wg.Done()
		for msg := range workerCh {
			c.onCommit(ctx, msg.key, msg.version)
		}
	}
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
			c.log.InfoContext(ctx, "done syncing, entering the active state")
		case msg, ok := <-c.onConfigChangeCh:
			if !ok {
				return
			}

			state := c.state.Load()
			if state.Config.Version >= msg.config.Version {
				close(msg.doneCh)
				continue
			}

			newState := &State{Config: msg.config, Status: state.Status}
			lostMembership := state.Config.IsMemberByID(c.ID) && !msg.config.IsMemberByID(c.ID)
			isNewMember := !state.Config.IsMemberByID(c.ID) && msg.config.IsMemberByID(c.ID)
			hasNewPred := !state.Config.Predecessor(c.ID).Equal(msg.config.Predecessor(c.ID))
			hasNewSucc := !state.Config.Successor(c.ID).Equal(msg.config.Successor(c.ID))

			// Node has lost chain membership. Cancel any ongoing syncing and become inactive.
			if lostMembership {
				c.log.InfoContext(ctx, "lost chain membership, entering the inactive state")
				onNewPredTask.cancelAndWait()
				onNewSuccTask.cancelAndWait()
				newState.Config = EmptyChain
				newState.Status = Inactive
				c.state.Store(newState)
				close(msg.doneCh)
				continue
			}
			// Node is a new member of a chain. Cancel any ongoing syncing. Enter the active state
			// if this node is the sole member of the chain.
			if isNewMember {
				c.log.InfoContext(ctx, "gained chain membership, entering the the syncing state")
				onNewPredTask.cancelAndWait()
				onNewSuccTask.cancelAndWait()
				newState.Status = Syncing
				if !hasNewPred && !hasNewSucc {
					newState.Status = Active
					c.log.InfoContext(ctx, "no predecessor or successor, entering the the active state")
				}
			}
			// Node has a new predecessor. Cancel any syncing that was taking place with the prior
			// predecessor and begin syncing with the new one.
			if hasNewPred {
				onNewPredTask.cancelAndWait()
				runNewPredecessorTask(msg.config, newState.Status == Syncing)
			}
			// Node has a new successor. Cancel any syncing that was taking place with the prior
			// successor and begin syncing with the new one.
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
			if succ := config.Successor(c.ID); succ != nil {
				if err := c.requestPropagation(ctx, succ, keyFilter, config); err != nil {
					continue
				}
			} else {
				c.log.InfoContext(ctx, "no successor found, committing all keys now")
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
			if pred := config.Predecessor(c.ID); pred != nil {
				err := c.requestPropagation(ctx, pred, keyFilter, config)
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
