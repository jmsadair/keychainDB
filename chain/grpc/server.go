package grpc

import (
	"context"
	"net"
	"sync"

	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/chain/storage"
	pb "github.com/jmsadair/keychain/proto/pbchain"
	"google.golang.org/grpc"
)

type gRPCSendStream struct {
	stream grpc.ServerStream
}

func (g *gRPCSendStream) Send(kv *storage.KeyValuePair) error {
	msg := &pb.KeyValuePair{Key: kv.Key, Value: kv.Value, Version: kv.Version, IsCommitted: kv.Committed}
	return g.stream.SendMsg(msg)
}

// Server is a gRPC-based server implementation for chain nodes.
type Server struct {
	pb.ChainServiceServer
	address string
	node    *node.ChainNode
}

// NewServer creates a new Server instance.
func NewServer(address string, node *node.ChainNode) *Server {
	return &Server{address: address, node: node}
}

// Run will start and run the server. Run should only be called once and is blocking.
func (s *Server) Run(ctx context.Context) error {
	resolved, err := net.ResolveTCPAddr("tcp", s.address)
	if err != nil {
		return err
	}
	listener, err := net.Listen(resolved.Network(), resolved.String())
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	pb.RegisterChainServiceServer(grpcServer, s)

	var wg sync.WaitGroup
	go func() {
		wg.Done()
		grpcServer.Serve(listener)
	}()

	<-ctx.Done()
	grpcServer.GracefulStop()
	wg.Wait()

	return nil
}

// Write handles incoming requests from other nodes in the chain to write a particular version of a key-value pair to storage.
func (s *Server) Write(ctx context.Context, request *pb.WriteRequest) (*pb.WriteResponse, error) {
	if err := s.node.WriteWithVersion(ctx, request.GetKey(), request.GetValue(), request.GetVersion()); err != nil {
		return nil, err
	}
	return &pb.WriteResponse{}, nil
}

// Read handles incoming requests from other nodes in the chain to read the committed version of a key-value pair.
func (s *Server) Read(ctx context.Context, request *pb.ReadRequest) (*pb.ReadResponse, error) {
	value, err := s.node.Read(ctx, request.GetKey())
	if err != nil {
		return nil, err
	}
	return &pb.ReadResponse{Value: value}, nil
}

// UpdateConfiguration handles requests from the coordinator to update the membership configuration.
func (s *Server) UpdateConfiguration(ctx context.Context, request *pb.UpdateConfigurationRequest) (*pb.UpdateConfigurationResponse, error) {
	pbConfig := request.GetConfiguration()
	config, err := node.NewConfigurationFromProto(pbConfig)
	if err != nil {
		return nil, err
	}
	if err := s.node.UpdateConfiguration(ctx, config); err != nil {
		return nil, err
	}
	return &pb.UpdateConfigurationResponse{}, nil
}

// Propagate handles requests from other nodes in the chain to initiate a server-side stream of key-value pairs.
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

	return s.node.Propagate(stream.Context(), keyFilter, &gRPCSendStream{stream: stream})
}
