package chain

import (
	"net"
	"sync"

	pb "github.com/jmsadair/zebraos/proto/pbchain"
	"github.com/jmsadair/zebraos/storage"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// KeyValueReceiveStream is a stream for receiving key-value pairs.
type KeyValueReceiveStream interface {
	// Receive will read the next key-value pair from the stream.
	// If is the end of the stream, an io.EOF error will be returned.
	Receive() (*storage.KeyValuePair, error)
}

// KeyValueSendStream is a stream for sending key-value pairs.
type KeyValueSendStream interface {
	// Send will send the key-value pair over the stream.
	Send(*storage.KeyValuePair) error
}

// chainClient is a client for nodes within a chain to communicate with one another.
type chainClient struct {
	mu       sync.Mutex
	clients  map[string]pb.ChainServiceClient
	dialOpts []grpc.DialOption
}

// newChainClient creates a new chainClient instance with the provided dial options.
func newChainClient(dialOpts ...grpc.DialOption) (*chainClient, error) {
	return &chainClient{dialOpts: dialOpts}, nil
}

// Ensure chainClient implements Client interface
var _ Client = (*chainClient)(nil)

// Write writes a versioned key-value to another node in the chain.
func (cc *chainClient) Write(ctx context.Context, address net.Addr, key string, value []byte, version uint64) error {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return err
	}
	_, err = client.Write(ctx, &pb.WriteRequest{Key: key, Value: value, Version: version})
	return err
}

// Read reads a key-value pair from a node in the chain.
func (cc *chainClient) Read(ctx context.Context, address net.Addr, key string) ([]byte, error) {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return nil, err
	}
	response, err := client.Read(context.Background(), &pb.ReadRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return response.GetValue(), nil
}

// Commit commits a particular version of a key to storage.
func (cc *chainClient) Commit(ctx context.Context, address net.Addr, key string, version uint64) error {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return err
	}
	_, err = client.Commit(ctx, &pb.CommitRequest{Key: key, Version: version})
	return err
}

// Propagate initiates a key-value stream where the keys will be filtered according to provided filter.
func (cc *chainClient) Propagate(ctx context.Context, address net.Addr, keyFilter storage.KeyFilter) (KeyValueReceiveStream, error) {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return nil, err
	}

	var keyType pb.KeyType
	switch keyFilter {
	case storage.AllKeys:
		keyType = pb.KeyType_KEYTYPE_ALL
	case storage.CommittedKeys:
		keyType = pb.KeyType_KEYTYPE_COMMITTED
	case storage.DirtyKeys:
		keyType = pb.KeyType_KEYTYPE_DIRTY
	}

	stream, err := client.Propagate(ctx, &pb.PropagateRequest{KeyType: keyType})
	if err != nil {
		return nil, err
	}
	return &keyValueReceiveStream{stream: stream}, nil
}

func (cc *chainClient) getOrCreateClient(address net.Addr) (pb.ChainServiceClient, error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	client, ok := cc.clients[address.String()]
	if !ok {
		conn, err := grpc.NewClient(address.String(), cc.dialOpts...)
		if err != nil {
			return nil, err
		}
		client = pb.NewChainServiceClient(conn)
		cc.clients[address.String()] = client
	}

	return client, nil
}

// Stream implementations

type keyValueReceiveStream struct {
	stream grpc.ClientStream
}

func (kvr *keyValueReceiveStream) Receive() (*storage.KeyValuePair, error) {
	var msg *pb.KeyValuePair
	if err := kvr.stream.RecvMsg(msg); err != nil {
		return nil, err
	}
	return &storage.KeyValuePair{Key: msg.GetKey(), Value: msg.GetValue(), Version: msg.GetVersion(), Committed: msg.GetIsCommitted()}, nil
}

type keyValueSendStream struct {
	stream grpc.ServerStream
}

func (kvs *keyValueSendStream) Send(kv *storage.KeyValuePair) error {
	msg := &pb.KeyValuePair{Key: kv.Key, Value: kv.Value, Version: kv.Version, IsCommitted: kv.Committed}
	return kvs.stream.SendMsg(msg)
}
