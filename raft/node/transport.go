package node

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/jmsadair/keychain/proto/raft"
)

const (
	snapshotChunkSize = 1024 * 1024
	bufferedChSize    = 32
)

type RaftClient interface {
	AppendEntries(ctx context.Context, address string, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
	AppendEntriesPipeline(ctx context.Context, address string) (pb.RaftService_AppendEntriesPipelineClient, error)
	RequestVote(ctx context.Context, address string, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)
	TimeoutNow(ctx context.Context, address string, request *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error)
	InstallSnapshot(ctx context.Context, address string) (pb.RaftService_InstallSnapshotClient, error)
}

type Transport struct {
	client RaftClient
	rpcCh  chan raft.RPC
}

func NewTransport(client RaftClient) *Transport {
	return &Transport{client: client, rpcCh: make(chan raft.RPC, bufferedChSize)}
}

func (t *Transport) RequestVote(
	id raft.ServerID,
	target raft.ServerAddress,
	request *raft.RequestVoteRequest,
	response *raft.RequestVoteResponse,
) error {
	var pbRequest pb.RequestVoteRequest
	requestVoteRequestToProto(request, &pbRequest)
	pbResponse, err := t.client.RequestVote(context.TODO(), string(target), &pbRequest)
	if err != nil {
		return err
	}
	protoToRequestVoteResponse(pbResponse, response)
	return nil
}

func (t *Transport) AppendEntries(
	id raft.ServerID,
	target raft.ServerAddress,
	request *raft.AppendEntriesRequest,
	response *raft.AppendEntriesResponse,
) error {
	var pbRequest pb.AppendEntriesRequest
	appendEntriesRequestToProto(request, &pbRequest)
	pbResponse, err := t.client.AppendEntries(context.TODO(), string(target), &pbRequest)
	if err != nil {
		return err
	}
	protoToAppendEntriesResponse(pbResponse, response)
	return nil
}

func (t *Transport) AppendEntriesPipeline(id string, target string) (raft.AppendPipeline, error) {
	ctx, cancel := context.WithCancel(context.TODO())
	stream, err := t.client.AppendEntriesPipeline(ctx, target)
	if err != nil {
		cancel()
		return nil, err
	}
	return newAppendPipeline(cancel, stream), nil
}

func (t *Transport) TimeoutNow(
	id raft.ServerID,
	target raft.ServerAddress,
	request *raft.TimeoutNowRequest,
	response *raft.TimeoutNowResponse,
) error {
	var pbRequest pb.TimeoutNowRequest
	timeoutNowRequestToProto(request, &pbRequest)
	pbResponse, err := t.client.TimeoutNow(context.TODO(), string(target), &pbRequest)
	if err != nil {
		return err
	}
	protoToTimeoutNowResponse(pbResponse, response)
	return nil
}

func (t *Transport) InstallSnapshot(
	id raft.ServerID,
	target raft.ServerAddress,
	request *raft.InstallSnapshotRequest,
	response *raft.InstallSnapshotResponse,
	data io.ReadCloser,
) error {
	defer data.Close()
	stream, err := t.client.InstallSnapshot(context.TODO(), string(target))
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

func (t *Transport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return []byte(addr)
}

func (t *Transport) DecodePeer(peer []byte) raft.ServerAddress {
	return raft.ServerAddress(peer)
}

type appendFuture struct {
	start      time.Time
	request    *raft.AppendEntriesRequest
	response   *raft.AppendEntriesResponse
	errorCh    chan error
	shutdownCh <-chan struct{}
	error      error
	valid      bool
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
	if a.valid {
		return a.error
	}

	var err error
	select {
	case err = <-a.errorCh:
	case <-a.shutdownCh:
		err = raft.ErrPipelineShutdown
	}

	a.valid = true
	a.error = err
	return err
}

type appendPipeline struct {
	inProgressCh chan *appendFuture
	doneCh       <-chan raft.AppendFuture
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
		start:      time.Now(),
		request:    request,
		response:   response,
		errorCh:    make(chan error, 1),
		shutdownCh: a.stream.Context().Done(),
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
	for {
		select {
		case future := <-a.inProgressCh:
			msg, err := a.stream.Recv()
			if err == nil {
				protoToAppendEntriesResponse(msg, future.response)
			}
			future.errorCh <- err
		case <-a.stream.Context().Done():
			return
		}
	}
}
