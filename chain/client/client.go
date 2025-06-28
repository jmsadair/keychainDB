package client

import (
	"net"
	"sync"

	pb "github.com/jmsadair/zebraos/proto/pbchain"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type ChainClient struct {
	mu       sync.Mutex
	address  net.Addr
	clients  map[string]pb.ChainServiceClient
	dialOpts []grpc.DialOption
}

func NewChainClient(dialOpts ...grpc.DialOption) (*ChainClient, error) {
	return &ChainClient{dialOpts: dialOpts}, nil
}

func (cc *ChainClient) Put(address net.Addr, key string, value []byte) error {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return err
	}
	_, err = client.Put(context.Background(), &pb.PutRequest{Key: key, Value: value})
	return err
}

func (cc *ChainClient) Delete(address net.Addr, key string) error {
	client, err := cc.getOrCreateClient(address)
	if err != nil {
		return err
	}
	_, err = client.Delete(context.Background(), &pb.DeleteRequest{Key: key})
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
