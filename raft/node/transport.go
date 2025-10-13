package node

import (
	"context"
	"errors"
	"io"

	"github.com/hashicorp/raft"
	pb "github.com/jmsadair/keychain/proto/raft"
)

const (
	snapshotChunkSize = 1024 * 1024
	bufferedChSize    = 32
)

type RaftClient interface {
	AppendEntries(ctx context.Context, address string, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)
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
