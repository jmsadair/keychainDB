package node

import (
	"context"
	"net"

	"github.com/jmsadair/zebraos/chain/chainclient"
	"github.com/jmsadair/zebraos/chain/chainnode"
	pb "github.com/jmsadair/zebraos/proto/pbchain"
	"google.golang.org/grpc"
)

type Server struct {
	pb.ChainServiceServer

	dialOpts  []grpc.DialOption
	chainNode *chainnode.ChainNode
}

func NewServer(address net.Addr, dialOpts ...grpc.DialOption) (*Server, error) {
	chainClient, err := chainclient.NewChainClient(dialOpts...)
	if err != nil {
		return nil, err
	}
	chainNode := chainnode.NewChainNode(address, nil, nil, nil, chainClient)
	return &Server{dialOpts: dialOpts, chainNode: chainNode}, nil
}

func (s *Server) Put(ctx context.Context, key string, value []byte) (*pb.PutResponse, error) {
	return &pb.PutResponse{}, s.chainNode.Put(key, value)
}

func (s *Server) Get(ctx context.Context, key string) (*pb.GetResponse, error) {
	value, err := s.chainNode.Get(key)
	if err != nil {
		return nil, err
	}
	return &pb.GetResponse{Value: value}, nil
}
