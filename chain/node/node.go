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
)

var (
	// ErrSyncing occurs when a read operation is invoked on a chain node that has yet
	// to be caught up by its peers. This typically happens when a new chain node has been
	// added to a chain.
	ErrSyncing = errors.New("chainserver: cannot serve reads while syncing")
	// ErrNotMemberOfChain occurs when an operation is invoked on a chain node that is not
	// a member of a chain. This may occur if a chain node has been removed from a chain but
	// is still running.
	ErrNotMemberOfChain = errors.New("chainserver: not member of chain")
	// ErrInvalidConfigVersion occurs when an operation is invoked on a chain node and the
	// provided configuration version does not match the configuration version that the chain
	// node has.
	ErrInvalidConfigVersion = errors.New("chainserver: configuration version mismatch")
	// ErrNotHead occurs when a write operation is invoked on a chain node that is not
	// the head of the chain. All writes are required to go through the head.
	ErrNotHead = errors.New("chainserver: chain head must serve writes")
)

const (
	// Default size of buffered channels. This is chosen arbitrarily.
	defaultBufferedChSize = 256
	// Default number of background threads used to propagate commits. This is chosen arbitrarily.
	numOnCommitWorkers = 16
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
	// UncommittedWrite will transactionally write the provided version of key to storage.
	UncommittedWrite(key string, value []byte, version uint64) error
	// UncommittedWriteNewVersion will transactionally generate a new version number for the
	// key and write the key-value pair to storage. The key-value pair will not be committed until
	// CommitVersion is called for the version that was generated.
	UncommittedWriteNewVersion(key string, value []byte) (uint64, error)
	// CommittedWrite will transactionally write the key-value pair to storage and will immediately commit it.
	CommittedWrite(key string, value []byte, version uint64) error
	// CommittedWriteNewVersion will transactionally generate a new version number for the key, write
	// the key-value pair to storage, and immediately commit it.
	CommittedWriteNewVersion(key string, value []byte) (uint64, error)
	// CommittedRead will transactionally read the value associated with the provided key.
	// If there are one or more uncommitted versions, then an error will be returned.
	CommittedRead(key string) ([]byte, error)
	// CommitVersion will transactionally commit the provided version of the key-value pair.
	// All versions of the key earlier than the committed version will be deleted.
	// Commiting a version that is older than the currently committed version is a no-op.
	CommitVersion(key string, version uint64) error
	// SendKeyValuePairs will iterate over the key-value pairs in storage that satisfy the provided filter,
	// group them into batches, and call the provided function with the batch as the argument. If at any
	// point during the process an error occurs, the process will be terminated and the error will be returned.
	SendKeyValuePairs(ctx context.Context, sendFunc func(context.Context, []storage.KeyValuePair) error, keyFilter storage.KeyFilter) error
	// CommitAll will commit all uncommitted key-value pairs from the current snapshot of the storage immediately.
	// For each key that is committed, the provided callback will be invoked. If committing any of the key-value
	// pairs fails, the process will be terminated and the error will be returned.
	CommitAll(ctx context.Context, onCommit func(ctx context.Context, key string, version uint64) error) error
}

// ChainClient defines the interface for chain node communication.
type ChainClient interface {
	// Write sends the appropriate RPC to the target node.
	Write(ctx context.Context, address string, request *pb.WriteRequest) (*pb.WriteResponse, error)
	// Read sends the appropriate RPC to the target node.
	Read(ctx context.Context, address string, request *pb.ReadRequest) (*pb.ReadResponse, error)
	// Commit sends the appropriate RPC to the target node.
	Commit(ctx context.Context, address string, request *pb.CommitRequest) (*pb.CommitResponse, error)
	// Propagate initiates a stream of key-value pairs from the target node to this node.
	Propagate(ctx context.Context, address string, request *pb.PropagateRequest) (pb.ChainService_PropagateClient, error)
	// Close closes all connections managed by this client.
	Close() error
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
	pb.ChainServiceServer
	// ID that uniquely identifies this node.
	ID string
	// Address for this node.
	Address string
	// Persistent storage for this node.
	store Storage
	// Client used to communicate with other nodes in the chain.
	chainClient ChainClient
	// The membership configuration and status of this node.
	state atomic.Pointer[State]

	log *slog.Logger

	onCommitCh       chan onCommitMessage
	onConfigChangeCh chan onConfigChangeMessage
	syncCompleteCh   chan any
}

// NewChainNode creates a new node.
func NewChainNode(id, address string, store Storage, chainClient ChainClient, log *slog.Logger) *ChainNode {
	state := &State{Config: EmptyChain, Status: Inactive}
	node := &ChainNode{
		ID:               id,
		Address:          address,
		log:              log.With("local-id", id),
		store:            store,
		chainClient:      chainClient,
		onCommitCh:       make(chan onCommitMessage, defaultBufferedChSize),
		onConfigChangeCh: make(chan onConfigChangeMessage),
		syncCompleteCh:   make(chan any),
	}
	node.state.Store(state)
	return node
}

// Run will run the node.
func (c *ChainNode) Run(ctx context.Context) {
	defer c.chainClient.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		c.onCommitRoutine(ctx)
	}()
	go func() {
		defer wg.Done()
		c.onConfigChangeRoutine(ctx)
	}()

	<-ctx.Done()
	wg.Wait()
}

// Write writes a versioned key-value pair to storage. This node will forward the request to its
// successor if it has one. If this node is the tail of the chain, it will immediately commit the
// key-value pair. This should only be invoked by other chain nodes. Clients should call Replicate instead.
func (c *ChainNode) Write(ctx context.Context, request *pb.WriteRequest) (*pb.WriteResponse, error) {
	state := c.state.Load()
	if request.ConfigVersion != state.Config.Version {
		return nil, ErrInvalidConfigVersion
	}
	if !state.Config.IsMemberByID(c.ID) {
		return nil, ErrNotMemberOfChain
	}

	// If this node is the tail then it is safe to immediately commit the key-value pair.
	succ := state.Config.Successor(c.ID)
	if succ == nil {
		err := c.store.CommittedWrite(request.Key, request.Value, request.Version)
		if err != nil {
			return nil, err
		}
		c.onCommitCh <- onCommitMessage{key: request.Key, version: request.Version}
		return &pb.WriteResponse{}, nil
	}
	// Otherwise, the key-value pair should be written to storage but not committed.
	if err := c.store.UncommittedWrite(request.Key, request.Value, request.Version); err != nil {
		return nil, err
	}

	// Forward the request to the next node in the chain.
	return c.chainClient.Write(ctx, succ.Address, request)
}

// Replicate initiates a write operation that will be replicated across the chain.
// This must only be invoked on the head of a chain. The operation will be rejected if this node is not the head.
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

	// If this node is the tail then it is safe to immediately commit the key-value pair.
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
	if _, err := c.chainClient.Write(ctx, succ.Address, req); err != nil {
		return nil, err
	}
	return &pb.ReplicateResponse{}, nil
}

// Commit commits a previously written version of a key-value and makes it visible to client reads.
// This should only be invoked by other chain nodes.
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

// Read is used to read a key-value pair. The read will be performed locally if the key-value pair
// has been committed. If the key-value pair has not been committed, this node will attempt to forward
// the request to the tail of the chain.
func (c *ChainNode) Read(ctx context.Context, request *pb.ReadRequest) (*pb.ReadResponse, error) {
	state := c.state.Load()
	if request.ConfigVersion != state.Config.Version {
		return nil, ErrInvalidConfigVersion
	}
	if !state.Config.IsMemberByID(c.ID) {
		return nil, ErrNotMemberOfChain
	}

	// If this node is syncing, try to forward to the predecessor.
	// New nodes are always added to the tail of the chain so it is likely that the predecessor
	// of this node has committed the key-value pair.
	if state.Status == Syncing {
		pred := state.Config.Predecessor(c.ID)
		if pred == nil {
			return nil, ErrSyncing
		}
		req := &pb.ReadRequest{Key: request.Key, ConfigVersion: state.Config.Version, Forwarded: true}
		return c.chainClient.Read(ctx, pred.Address, req)
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
		return c.chainClient.Read(ctx, tail.Address, req)
	}
	if err != nil {
		return nil, err
	}

	return &pb.ReadResponse{Value: value}, nil
}

// Propagate is used to stream key-value pairs from this node to the requesting node. This is useful when
// a new node has been added to the chain and needs to be caught up. This should only be invoked by other
// chain nodes.
func (c *ChainNode) Propagate(request *pb.PropagateRequest, stream pb.ChainService_PropagateServer) error {
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

	return c.store.SendKeyValuePairs(stream.Context(), sendFunc, storage.KeyFilterFromProto(request.KeyType))
}

// UpdateConfiguration is used to update the membership configuration of this node. This should only be invoked by the coordinator.
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

// Ping is used to check if this node is alive.
func (c *ChainNode) Ping(ctx context.Context, request *pb.PingRequest) (*pb.PingResponse, error) {
	state := c.state.Load()
	return &pb.PingResponse{Status: int32(state.Status), ConfigVersion: state.Config.Version}, nil
}

// Configuration will return the local membership configuration for this node.
func (c *ChainNode) Configuration() *Configuration {
	state := c.state.Load()
	return state.Config
}

// Status returns the status of this node.
func (c *ChainNode) Status() Status {
	state := c.state.Load()
	return state.Status
}

func (c *ChainNode) requestPropagation(ctx context.Context, member *ChainMember, keyFilter storage.KeyFilter, config *Configuration) error {
	req := &pb.PropagateRequest{KeyType: keyFilter.Proto(), ConfigVersion: config.Version}
	isTail := config.IsTail(c.ID)

	stream, err := c.chainClient.Propagate(ctx, member.Address, req)
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

		// If the key-value pair has already been committed by another node or if this node is the tail
		// it is safe to immediately commit.
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

// onCommit is used to inform the predecessor of this node that a key-value pair has
// been committed.
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
	_, err = c.chainClient.Commit(ctx, pred.Address, req)
	return err
}

// onCommitRoutine listens for key-value pairs being committed and will inform the
// predecessor of this node of the commit.
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

// onConfigChangeRoutine is used to listen for membership configuration updates from the coordinator
// and to perform any necessary key-value pair propagation.
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

			// Skip processing any configurations that have an older version than the version this node has.
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

// onNewSuccessor is used to initiate propagation of key-value pairs from the successor of this node
// to this node. This function will continue to initiate propagation until all key-value pairs have
// been successfully propagated.
func (c *ChainNode) onNewSuccessor(ctx context.Context, config *Configuration, isSyncing bool) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// If this node is not syncing, then only the key-value pairs that are committed need to be
			// propagated. It is necessary to propagate the committed key-value pairs since it is possible
			// that one or more successors committed the key-value pair but the commit was never propagated
			// to this node. Without this, all reads to this node and its predecessors would fail until another
			// write resulted in the key being successfully committed.
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

// onNewPredecessor is used to initiate propagation of key-value pairs from the predecessor of this node
// to this node. This function will continue to initiate propagation until all key-value pairs have
// been successfully propagated.
func (c *ChainNode) onNewPredecessor(ctx context.Context, config *Configuration, isSyncing bool) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// If this node is not syncing, then only the key-value pairs that are dirty need to be
			// propagated. It is necessary to propagate the dirty key-value pairs since it is possible
			// that one or more predecessors performed an uncommitted write that was never committed
			// due to the write not being successfully propagated to the tail. Without this, all reads
			// to the predecessors would fail until another write resulted in the key being successfully
			// committed.
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
