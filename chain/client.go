package chain

import (
	"net"
	"sync"

	pb "github.com/jmsadair/zebraos/proto/pbchain"
	"github.com/jmsadair/zebraos/storage"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// KeyValueRecieveStream is a stream for recieving key-value pairs.
type KeyValueRecieveStream interface {
	// Recieve will read the next key-value pair from the stream.
	// If is the end of the stream, an io.EOF error will be returned.
	Recieve() (*storage.KeyValuePair, error)
}

// KeyValueRecieveStream is a stream for sending key-value pairs.
type KeyValueSendStream interface {
	// Send will send the key-value pair over the stream.
	Send(*storage.KeyValuePair) error
}

// ChainClient is a client for nodes within a chain to communicate with one another.
type ChainClient struct {
	mu       sync.Mutex
	clients  map[string]pb.ChainServiceClient
	dialOpts []grpc.DialOption
}

// NewChainClient creates a new ChainClient instance with the provided dial options.
func NewChainClient(dialOpts ...grpc.DialOption) (*ChainClient, error) {
	return &ChainClient{dialOpts: dialOpts}, nil
}

// Write writes a versioned key-value to another node in the chain.
func (cc *ChainClient) Write(ctx context.Context, address net.Addr, key string, value []byte, version uint64) error {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return err
	}
	_, err = client.Write(ctx, &pb.WriteRequest{Key: key, Value: value, Version: version})
	return err
}

// Read reads a key-value pair from a node in the chain.
func (cc *ChainClient) Read(ctx context.Context, address net.Addr, key string) ([]byte, error) {
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
func (cc *ChainClient) Commit(ctx context.Context, address net.Addr, key string, version uint64) error {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return err
	}
	_, err = client.Commit(ctx, &pb.CommitRequest{Key: key, Version: version})
	return err
}

// Propagate initiates a key-value stream where the keys will be filtered according to provided filter.
func (cc *ChainClient) Propagate(ctx context.Context, address net.Addr, keyFilter storage.KeyFilter) (KeyValueRecieveStream, error) {
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
	return &keyValueRecieveStream{stream: stream}, nil
}

func (cc *ChainClient) getOrCreateClient(address net.Addr) (pb.ChainServiceClient, error) {
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
