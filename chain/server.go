package chain

import (
	"context"
	"net"

	pb "github.com/jmsadair/zebraos/proto/pbchain"
	"github.com/jmsadair/zebraos/storage"
	"google.golang.org/grpc"
)

// Server is a server implementation for a chain node.
type Server struct {
	pb.ChainServiceServer

	dialOpts []grpc.DialOption
	node     *ChainNode
}

// NewServer creates a new Server instance.
func NewServer(address net.Addr, dbPath string, dialOpts ...grpc.DialOption) (*Server, error) {
	client, err := newChainClient(dialOpts...)
	if err != nil {
		return nil, err
	}
	store, err := storage.NewPersistentStorage(dbPath)
	if err != nil {
		return nil, err
	}
	node := NewChainNode(address, store, client)
	server := &Server{dialOpts: dialOpts, node: node}
	return server, nil
}

// Run will start and run the server. Run should only be called once and is blocking.
func (s *Server) Run(ctx context.Context) {
	go s.node.onCommitRoutine(ctx)
	go s.node.onConfigChangeRoutine(ctx)
}

// Write handles incoming requests from other nodes in the chain to write a particular version of a key-value pair to storage.
func (s *Server) Write(ctx context.Context, request *pb.WriteRequest) (*pb.WriteResponse, error) {
	if err := s.node.writeWithVersion(ctx, request.GetKey(), request.GetValue(), request.GetVersion()); err != nil {
		return nil, err
	}
	return &pb.WriteResponse{}, nil
}

// Read handles incoming requests from other nodes in the chain read the committed version of a key-value pair.
func (s *Server) Read(ctx context.Context, request *pb.ReadRequest) (*pb.ReadResponse, error) {
	value, err := s.node.read(ctx, request.GetKey())
	if err != nil {
		return nil, err
	}
	return &pb.ReadResponse{Value: value}, nil
}

// UpdateConfiguration handles requests from the coordinator to update the membership configuration.
func (s *Server) UpdateConfiguration(ctx context.Context, request *pb.UpdateConfigurationRequest) (*pb.UpdateConfigurationResponse, error) {
	pbConfig := request.GetConfiguration()
	config, err := NewChainConfigurationFromProto(pbConfig)
	if err != nil {
		return nil, err
	}
	if err := s.node.updateConfiguration(ctx, config); err != nil {
		return nil, err
	}
	return &pb.UpdateConfigurationResponse{}, nil
}

// Propagate handles requests from other nodes in the chain to initate a server-side stream of key-value pairs.
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

	return s.node.propagate(stream.Context(), keyFilter, &keyValueSendStream{stream: stream})
}
