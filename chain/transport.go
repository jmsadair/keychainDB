package chain

import (
	"context"
	"net"
	"sync"

	"github.com/jmsadair/keychain/chain/storage"
	pb "github.com/jmsadair/keychain/proto/pbchain"
	"google.golang.org/grpc"
)

// KeyValueReceiveStream is a stream for receiving key-value pairs.
type KeyValueReceiveStream interface {
	// Recieve reads the next key-value pair in a stream of key-value pairs.
	Receive() (*storage.KeyValuePair, error)
}

type wrappedReceiveStream struct {
	stream grpc.ClientStream
}

func (kvr *wrappedReceiveStream) Receive() (*storage.KeyValuePair, error) {
	var msg *pb.KeyValuePair
	if err := kvr.stream.RecvMsg(msg); err != nil {
		return nil, err
	}
	return &storage.KeyValuePair{Key: msg.GetKey(), Value: msg.GetValue(), Version: msg.GetVersion(), Committed: msg.GetIsCommitted()}, nil
}

type wrappedSendStream struct {
	stream grpc.ServerStream
}

func (kvs *wrappedSendStream) Send(kv *storage.KeyValuePair) error {
	msg := &pb.KeyValuePair{Key: kv.Key, Value: kv.Value, Version: kv.Version, IsCommitted: kv.Committed}
	return kvs.stream.SendMsg(msg)
}

type gRPCTransport struct {
	mu       sync.Mutex
	clients  map[string]pb.ChainServiceClient
	dialOpts []grpc.DialOption
}

// NewGRPCTransport creates a new gRPC-based chain client with the provided dial options.
func NewGRPCTransport(dialOpts ...grpc.DialOption) (Transport, error) {
	return &gRPCTransport{
		clients:  make(map[string]pb.ChainServiceClient),
		dialOpts: dialOpts,
	}, nil
}

func (cc *gRPCTransport) Write(ctx context.Context, address net.Addr, key string, value []byte, version uint64) error {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return err
	}
	_, err = client.Write(ctx, &pb.WriteRequest{Key: key, Value: value, Version: version})
	return err
}

func (cc *gRPCTransport) Read(ctx context.Context, address net.Addr, key string) ([]byte, error) {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return nil, err
	}
	response, err := client.Read(ctx, &pb.ReadRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return response.GetValue(), nil
}

func (cc *gRPCTransport) Commit(ctx context.Context, address net.Addr, key string, version uint64) error {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return err
	}
	_, err = client.Commit(ctx, &pb.CommitRequest{Key: key, Version: version})
	return err
}

func (cc *gRPCTransport) Propagate(ctx context.Context, address net.Addr, keyFilter storage.KeyFilter) (KeyValueReceiveStream, error) {
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
	return &wrappedReceiveStream{stream: stream}, nil
}

func (cc *gRPCTransport) getOrCreateClient(address net.Addr) (pb.ChainServiceClient, error) {
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
