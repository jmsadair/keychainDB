package node

import (
	"context"
	"net"

	"github.com/jmsadair/zebraos/chain/chainclient"
	"github.com/jmsadair/zebraos/chain/chainnode"
	"github.com/jmsadair/zebraos/chain/storage"
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
	if err := s.chainNode.WriteWithVersion(ctx, request.GetKey(), request.GetValue(), request.GetVersion()); err != nil {
		return nil, err
	}
	return &pb.WriteResponse{}, nil
}

func (s *Server) Read(ctx context.Context, request *pb.ReadRequest) (*pb.ReadResponse, error) {
	value, err := s.chainNode.Read(ctx, request.GetKey())
	if err != nil {
		return nil, err
	}
	return &pb.ReadResponse{Value: value}, nil
}

func (s *Server) Backfill(request *pb.BackfillRequest, stream pb.ChainService_BackfillServer) error {
	sendFunc := func(ctx context.Context, kvPairs []storage.KeyValuePair) error {
		for _, kvPair := range kvPairs {
			if err := stream.Send(&pb.KeyValuePair{Key: kvPair.Key, Value: kvPair.Value, Version: kvPair.Version}); err != nil {
				return err
			}
		}
		return nil
	}

	switch request.GetKeyType() {
	case pb.KeyType_KEYTYPE_ALL:
		return s.chainNode.ListAllKeyValuePairs(stream.Context(), sendFunc)
	case pb.KeyType_KEYTYPE_COMMITTED:
		return s.chainNode.ListCommittedKeyValuePairs(stream.Context(), sendFunc)
	case pb.KeyType_KEYTYPE_DIRTY:
		return s.chainNode.ListDirtyKeyValuePairs(stream.Context(), sendFunc)
	}

	return nil
}
