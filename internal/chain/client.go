package chain

import (
	"net"
	"sync"

	pb "github.com/jmsadair/zebraos/proto/pbchain"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type ChainClient struct {
	mu       sync.Mutex
	clients  map[string]pb.ChainServiceClient
	dialOpts []grpc.DialOption
}

func NewChainClient(dialOpts ...grpc.DialOption) (*ChainClient, error) {
	return &ChainClient{dialOpts: dialOpts}, nil
}

func (cc *ChainClient) Write(ctx context.Context, address net.Addr, key string, value []byte, version uint64) error {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return err
	}
	_, err = client.Write(ctx, &pb.WriteRequest{Key: key, Value: value, Version: version})
	return err
}

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

func (cc *ChainClient) Commit(ctx context.Context, address net.Addr, key string, version uint64) error {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return err
	}
	_, err = client.Commit(ctx, &pb.CommitRequest{Key: key, Version: version})
	return err
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
