package network

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/jmsadair/keychain/proto/raft"
	"google.golang.org/grpc"
)

const (
	// Max size in bytes of snapshot chunks that are streamed.
	snapshotChunkSize = 1024 * 1024 // 1 MB
	// Default size for buffered channels. This value is chosen arbitrarily.
	bufferedChSize = 32
)

// Client is used to make RPCs to other nodes in the cluster.
type Client interface {
	// AppendEntries sends the appropriate RPC to the target node.
	AppendEntries(ctx context.Context, address string, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	// RequestVote sends the appropriate RPC to the target node.
	AppendEntriesPipeline(ctx context.Context, address string) (pb.RaftService_AppendEntriesPipelineClient, error)
	// RequestVote sends the appropriate RPC to the target node.
	RequestVote(ctx context.Context, address string, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	// RequestPreVote sends the appropriate RPC to the target node.
	RequestPreVote(ctx context.Context, address string, request *pb.RequestPreVoteRequest) (*pb.RequestPreVoteResponse, error)
	// TimeoutNow is used to start a leadership transfer to the target node.
	TimeoutNow(ctx context.Context, address string, request *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error)
	// InstallSnapshot is used to stream a snapshot down to a follower.
	InstallSnapshot(ctx context.Context, address string) (pb.RaftService_InstallSnapshotClient, error)
	// Close closes all connections managed by this client.
	Close() error
}

var _ raft.Transport = Transport{}
var _ raft.WithPreVote = Transport{}
var _ raft.WithClose = Transport{}

func isHeartbeat(cmd any) bool {
	request, ok := cmd.(*raft.AppendEntriesRequest)
	if !ok {
		return false
	}
	leaderAddr := request.Addr
	return request.Term != 0 && leaderAddr != nil && request.PrevLogEntry == 0 &&
		request.PrevLogTerm == 0 && len(request.Entries) == 0 && request.LeaderCommitIndex == 0
}

// Network manages the client and server that are used by raft.
type Network struct {
	rpcCh            chan raft.RPC
	client           Client
	address          string
	heartbeatHandler func(raft.RPC)
	mu               sync.RWMutex
}

// NewNetwork creates a new network.
func NewNetwork(address string, client Client) *Network {
	return &Network{address: address, client: client, rpcCh: make(chan raft.RPC)}
}

// Transport creates a new transport.
func (n *Network) Transport() *Transport {
	return &Transport{nw: n}
}

// Register registers the raft server implementation with gRPC.
func (n *Network) Register(s grpc.ServiceRegistrar) {
	pb.RegisterRaftServiceServer(s, server{nw: n})
}

type server struct {
	pb.UnimplementedRaftServiceServer
	nw *Network
}

func (srv server) SetHeartbeatHandler(fn func(rpc raft.RPC)) {}

func (srv server) heartbeatHandler() func(raft.RPC) {
	srv.nw.mu.RLock()
	defer srv.nw.mu.RUnlock()
	return srv.nw.heartbeatHandler
}

func (srv server) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	var command raft.AppendEntriesRequest
	var response pb.AppendEntriesResponse
	protoToAppendEntriesRequest(request, &command)
	result, err := executeRPC[*raft.AppendEntriesResponse](ctx, &command, nil, srv.nw.rpcCh, srv.heartbeatHandler())
	if err != nil {
		return nil, err
	}
	appendEntriesResponseToProto(result, &response)
	return &response, nil
}

func (srv server) AppendEntriesPipeline(s pb.RaftService_AppendEntriesPipelineServer) error {
	for {
		msg, err := s.Recv()
		if err != nil {
			return err
		}
		var command raft.AppendEntriesRequest
		var response pb.AppendEntriesResponse
		protoToAppendEntriesRequest(msg, &command)
		resp, err := executeRPC[*raft.AppendEntriesResponse](s.Context(), &command, nil, srv.nw.rpcCh, srv.heartbeatHandler())
		if err != nil {
			return err
		}
		appendEntriesResponseToProto(resp, &response)
		if err := s.Send(&response); err != nil {
			return err
		}
	}
}

func (srv server) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	var command raft.RequestVoteRequest
	var response pb.RequestVoteResponse
	protoToRequestVoteRequest(request, &command)
	result, err := executeRPC[*raft.RequestVoteResponse](ctx, &command, nil, srv.nw.rpcCh, srv.heartbeatHandler())
	if err != nil {
		return nil, err
	}
	requestVoteResponseToProto(result, &response)
	return &response, nil
}

func (srv server) RequestPreVote(ctx context.Context, request *pb.RequestPreVoteRequest) (*pb.RequestPreVoteResponse, error) {
	var command raft.RequestPreVoteRequest
	var response pb.RequestPreVoteResponse
	protoToRequestPreVoteRequest(request, &command)
	result, err := executeRPC[*raft.RequestPreVoteResponse](ctx, &command, nil, srv.nw.rpcCh, srv.heartbeatHandler())
	if err != nil {
		return nil, err
	}
	requestPreVoteResponseToProto(result, &response)
	return &response, nil
}

func (srv server) TimeoutNow(ctx context.Context, request *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	var command raft.TimeoutNowRequest
	var response pb.TimeoutNowResponse
	protoToTimeoutNowRequest(request, &command)
	result, err := executeRPC[*raft.TimeoutNowResponse](ctx, &command, nil, srv.nw.rpcCh, srv.heartbeatHandler())
	if err != nil {
		return nil, err
	}
	timeoutNowResponseToProto(result, &response)
	return &response, nil
}

func (srv server) InstallSnapshot(stream pb.RaftService_InstallSnapshotServer) error {
	ctx := stream.Context()
	request, err := stream.Recv()
	if err != nil {
		return err
	}

	var command raft.InstallSnapshotRequest
	protoToInstallSnapshotRequest(request, &command)
	streamWrapper := &snapshotStream{stream: stream, buf: request.GetData()}
	result, err := executeRPC[*raft.InstallSnapshotResponse](ctx, &command, streamWrapper, srv.nw.rpcCh, srv.heartbeatHandler())
	if err != nil {
		return err
	}

	var response pb.InstallSnapshotResponse
	installSnapshotResponseToProto(result, &response)
	return stream.SendAndClose(&response)
}

func executeRPC[T any](ctx context.Context, command any, reader io.Reader, rpcCh chan raft.RPC, heartbeatHandler func(raft.RPC)) (T, error) {
	respCh := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{Command: command, Reader: reader, RespChan: respCh}
	var zero T

	isHeartbeatCmd := isHeartbeat(command)
	if !isHeartbeatCmd || heartbeatHandler == nil {
		select {
		case rpcCh <- rpc:
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	} else {
		heartbeatHandler(rpc)
	}

	select {
	case resp := <-respCh:
		if resp.Error != nil {
			return zero, resp.Error
		}
		return resp.Response.(T), nil
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

type snapshotStream struct {
	buf    []byte
	eof    bool
	stream pb.RaftService_InstallSnapshotServer
}

func (s *snapshotStream) Read(p []byte) (int, error) {
	for len(s.buf) == 0 && !s.eof {
		resp, err := s.stream.Recv()
		if err != nil {
			if err == io.EOF {
				s.eof = true
			} else {
				return 0, err
			}
			break
		}
		s.buf = resp.GetData()
	}

	if len(s.buf) == 0 {
		return 0, io.EOF
	}

	n := copy(p, s.buf)
	s.buf = s.buf[n:]

	return n, nil
}

type Transport struct {
	nw *Network
}

func (t Transport) Consumer() <-chan raft.RPC {
	return t.nw.rpcCh
}

func (t Transport) LocalAddr() raft.ServerAddress {
	return raft.ServerAddress(t.nw.address)
}

func (t Transport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {}

func (t Transport) RequestVote(
	id raft.ServerID,
	target raft.ServerAddress,
	request *raft.RequestVoteRequest,
	response *raft.RequestVoteResponse,
) error {
	var pbRequest pb.RequestVoteRequest
	requestVoteRequestToProto(request, &pbRequest)
	pbResponse, err := t.nw.client.RequestVote(context.TODO(), string(target), &pbRequest)
	if err != nil {
		return err
	}
	protoToRequestVoteResponse(pbResponse, response)
	return nil
}

func (t Transport) RequestPreVote(
	id raft.ServerID,
	target raft.ServerAddress,
	request *raft.RequestPreVoteRequest,
	response *raft.RequestPreVoteResponse,
) error {
	var pbRequest pb.RequestPreVoteRequest
	requestPreVoteRequestToProto(request, &pbRequest)
	pbResponse, err := t.nw.client.RequestPreVote(context.TODO(), string(target), &pbRequest)
	if err != nil {
		return err
	}
	protoToRequestPreVoteResponse(pbResponse, response)
	return nil
}

func (t Transport) AppendEntries(
	id raft.ServerID,
	target raft.ServerAddress,
	request *raft.AppendEntriesRequest,
	response *raft.AppendEntriesResponse,
) error {
	var pbRequest pb.AppendEntriesRequest
	appendEntriesRequestToProto(request, &pbRequest)
	pbResponse, err := t.nw.client.AppendEntries(context.TODO(), string(target), &pbRequest)
	if err != nil {
		return err
	}
	protoToAppendEntriesResponse(pbResponse, response)
	return nil
}

func (t Transport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	ctx, cancel := context.WithCancel(context.TODO())
	stream, err := t.nw.client.AppendEntriesPipeline(ctx, string(target))
	if err != nil {
		cancel()
		return nil, err
	}
	return newAppendPipeline(cancel, stream), nil
}

func (t Transport) TimeoutNow(
	id raft.ServerID,
	target raft.ServerAddress,
	request *raft.TimeoutNowRequest,
	response *raft.TimeoutNowResponse,
) error {
	var pbRequest pb.TimeoutNowRequest
	timeoutNowRequestToProto(request, &pbRequest)
	pbResponse, err := t.nw.client.TimeoutNow(context.TODO(), string(target), &pbRequest)
	if err != nil {
		return err
	}
	protoToTimeoutNowResponse(pbResponse, response)
	return nil
}

func (t Transport) InstallSnapshot(
	id raft.ServerID,
	target raft.ServerAddress,
	request *raft.InstallSnapshotRequest,
	response *raft.InstallSnapshotResponse,
	data io.Reader,
) error {
	stream, err := t.nw.client.InstallSnapshot(context.TODO(), string(target))
	if err != nil {
		return err
	}

	var pbRequest pb.InstallSnapshotRequest
	installSnapshotRequestToProto(request, &pbRequest)
	buf := make([]byte, snapshotChunkSize)
	for {
		n, err := data.Read(buf)
		if (err == nil && n == 0) || (err != nil && errors.Is(err, io.EOF)) {
			break
		}
		if err != nil {
			return err
		}

		pbRequest.Data = buf[:n]
		if err := stream.SendMsg(&pbRequest); err != nil {
			return err
		}
	}

	pbResponse, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}

	protoToInstallSnapshotResponse(pbResponse, response)
	return nil
}

func (t Transport) Close() error {
	return t.nw.client.Close()
}

func (t Transport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(addr)
}

func (t Transport) DecodePeer(peer []byte) raft.ServerAddress {
	return raft.ServerAddress(peer)
}

type appendFuture struct {
	start    time.Time
	request  *raft.AppendEntriesRequest
	response *raft.AppendEntriesResponse
	doneCh   chan any
	err      error
}

func (a *appendFuture) Start() time.Time {
	return a.start
}

func (a *appendFuture) Request() *raft.AppendEntriesRequest {
	return a.request
}

func (a *appendFuture) Response() *raft.AppendEntriesResponse {
	return a.response
}

func (a *appendFuture) Error() error {
	<-a.doneCh
	return a.err
}

type appendPipeline struct {
	inProgressCh chan *appendFuture
	doneCh       chan raft.AppendFuture
	stream       pb.RaftService_AppendEntriesPipelineClient
	cancel       func()
	wg           sync.WaitGroup
}

func newAppendPipeline(cancel func(), stream pb.RaftService_AppendEntriesPipelineClient) *appendPipeline {
	pipeline := &appendPipeline{
		stream:       stream,
		doneCh:       make(chan raft.AppendFuture, bufferedChSize),
		inProgressCh: make(chan *appendFuture, bufferedChSize),
		cancel:       cancel,
	}

	pipeline.wg.Add(1)
	go pipeline.processResponses()
	return pipeline
}

func (a *appendPipeline) AppendEntries(request *raft.AppendEntriesRequest, response *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	future := &appendFuture{
		start:    time.Now(),
		request:  request,
		response: response,
		doneCh:   make(chan any),
	}
	var pbRequest pb.AppendEntriesRequest
	appendEntriesRequestToProto(request, &pbRequest)

	if err := a.stream.Send(&pbRequest); err != nil {
		return nil, err
	}

	select {
	case a.inProgressCh <- future:
	case <-a.stream.Context().Done():
		return nil, raft.ErrPipelineShutdown
	}

	return future, nil
}

func (a *appendPipeline) Consumer() <-chan raft.AppendFuture {
	return a.doneCh
}

func (a *appendPipeline) Close() error {
	a.cancel()
	a.wg.Wait()
	return nil
}

func (a *appendPipeline) processResponses() {
	defer a.wg.Done()
	for {
		select {
		case future := <-a.inProgressCh:
			msg, err := a.stream.Recv()
			if err == nil {
				protoToAppendEntriesResponse(msg, future.response)
			}
			future.err = err
			close(future.doneCh)
			a.doneCh <- future
		case <-a.stream.Context().Done():
			return
		}
	}
}
