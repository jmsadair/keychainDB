package chain

import (
	"context"
	"net"

	"github.com/jmsadair/keychain/chain/storage"
	"github.com/jmsadair/keychain/chain/transport"
	pb "github.com/jmsadair/keychain/proto/pbchain"
)

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

// KeyValueReceiveStream is a stream for receiving key-value pairs.
type KeyValueReceiveStream interface {
	// Recieve reads the next key-value pair in a stream of key-value pairs.
	Receive() (*storage.KeyValuePair, error)
}

// Transport defines the interface for chain node communication.
type Transport interface {
	// Write will write a versioned key-value pair.
	Write(ctx context.Context, address net.Addr, key string, value []byte, version uint64) error
	// Read will read the committed version of the key.
	Read(ctx context.Context, address net.Addr, key string) ([]byte, error)
	// Commit will commit the provided version of th key.
	Commit(ctx context.Context, address net.Addr, key string, version uint64) error
	// Propagate will initiate a stream of key-value pairs from another node.
	Propagate(ctx context.Context, address net.Addr, keyFilter storage.KeyFilter) (KeyValueReceiveStream, error)
}

// Server is a server implementation for a chain node.
type Server struct {
	pb.ChainServiceServer
	node *ChainNode
}

// NewServer creates a new Server instance.
func NewServer(address net.Addr, store Storage, tn Transport) *Server {
	node := NewChainNode(address, store, tn)
	return &Server{node: node}
}

// Run will start and run the server. Run should only be called once and is blocking.
func (s *Server) Run(ctx context.Context) {
	s.node.Run(ctx)
}

// Write handles incoming requests from other nodes in the chain to write a particular version of a key-value pair to storage.
func (s *Server) Write(ctx context.Context, request *pb.WriteRequest) (*pb.WriteResponse, error) {
	if err := s.node.WriteWithVersion(ctx, request.GetKey(), request.GetValue(), request.GetVersion()); err != nil {
		return nil, err
	}
	return &pb.WriteResponse{}, nil
}

// Read handles incoming requests from other nodes in the chain to read the committed version of a key-value pair.
func (s *Server) Read(ctx context.Context, request *pb.ReadRequest) (*pb.ReadResponse, error) {
	value, err := s.node.Read(ctx, request.GetKey())
	if err != nil {
		return nil, err
	}
	return &pb.ReadResponse{Value: value}, nil
}

// UpdateConfiguration handles requests from the coordinator to update the membership configuration.
func (s *Server) UpdateConfiguration(ctx context.Context, request *pb.UpdateConfigurationRequest) (*pb.UpdateConfigurationResponse, error) {
	pbConfig := request.GetConfiguration()
	config, err := NewConfigurationFromProto(pbConfig)
	if err != nil {
		return nil, err
	}
	if err := s.node.UpdateConfiguration(ctx, config); err != nil {
		return nil, err
	}
	return &pb.UpdateConfigurationResponse{}, nil
}

// Propagate handles requests from other nodes in the chain to initiate a server-side stream of key-value pairs.
func (s *Server) Propagate(request *pb.PropagateRequest, stream pb.ChainService_PropagateServer) error {
	var keyFilter storage.KeyFilter
	switch request.GetKeyType() {
	case pb.KeyType_KEYTYPE_ALL:
		keyFilter = storage.AllKeys
	case pb.KeyType_KEYTYPE_COMMITTED:
		keyFilter = storage.CommittedKeys
	case pb.KeyType_KEYTYPE_DIRTY:
		keyFilter = storage.DirtyKeys
	}

	return s.node.Propagate(stream.Context(), keyFilter, &transport.KeyValueSendStream{Stream: stream})
}
