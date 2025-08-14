package chain

import (
	"context"
	"net"

	"github.com/jmsadair/zebraos/internal/chain"
	"github.com/jmsadair/zebraos/internal/storage"
	pb "github.com/jmsadair/zebraos/proto/pbchain"
	"google.golang.org/grpc"
)

type Server struct {
	pb.ChainServiceServer

	dialOpts  []grpc.DialOption
	chainNode *chain.ChainNode
}

func NewServer(address net.Addr, dbPath string, dialOpts ...grpc.DialOption) (*Server, error) {
	chainClient, err := chain.NewChainClient(dialOpts...)
	if err != nil {
		return nil, err
	}
	store, err := storage.NewPersistantStorage(dbPath)
	if err != nil {
		return nil, err
	}
	chainNode := chain.NewChainNode(address, store, chainClient)
	server := &Server{dialOpts: dialOpts, chainNode: chainNode}
	return server, nil
}

func (s *Server) Run(ctx context.Context) {
	go s.chainNode.OnCommitRoutine(ctx)
	go s.chainNode.OnConfigChangeRoutine(ctx)
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

func (s *Server) UpdateConfiguration(ctx context.Context, request *pb.UpdateConfigurationRequest) (*pb.UpdateConfigurationResponse, error) {
	pbConfig := request.GetConfiguration()
	config, err := chain.NewChainConfigurationFromProto(pbConfig)
	if err != nil {
		return nil, err
	}
	if err := s.chainNode.UpdateConfiguration(ctx, config); err != nil {
		return nil, err
	}
	return &pb.UpdateConfigurationResponse{}, nil
}

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

	return s.chainNode.Propagate(stream.Context(), keyFilter, &chain.KeyValueSender{Stream: stream})
}
