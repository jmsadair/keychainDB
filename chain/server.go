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
	chainNode := chainnode.NewChainNode(address, nil, chainClient)
	return &Server{dialOpts: dialOpts, chainNode: chainNode}, nil
}

func (s *Server) Write(ctx context.Context, request *pb.WriteRequest) (*pb.WriteResponse, error) {
	return nil, nil
}

func (s *Server) Read(ctx context.Context, request *pb.ReadRequest) (*pb.ReadResponse, error) {
	return nil, nil
}
