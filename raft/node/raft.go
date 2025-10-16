package node

import (
	"context"
	"io"

	"github.com/hashicorp/raft"
	pb "github.com/jmsadair/keychain/proto/raft"
)

type Raft struct {
	rpcCh chan raft.RPC
}

func (r *Raft) AppendEntries(ctx context.Context, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	var command raft.AppendEntriesRequest
	var response pb.AppendEntriesResponse
	protoToAppendEntriesRequest(request, &command)
	result, err := executeRPC[*raft.AppendEntriesResponse](ctx, command, nil, r.rpcCh)
	if err != nil {
		return nil, err
	}
	appendEntriesResponseToProto(result, &response)
	return &response, nil
}

func (r *Raft) RequestVote(ctx context.Context, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	var command raft.RequestVoteRequest
	var response pb.RequestVoteResponse
	protoToRequestVoteRequest(request, &command)
	result, err := executeRPC[*raft.RequestVoteResponse](ctx, command, nil, r.rpcCh)
	if err != nil {
		return nil, err
	}
	requestVoteResponseToProto(result, &response)
	return &response, nil
}

func (r *Raft) TimeoutNow(ctx context.Context, request *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	var command raft.TimeoutNowRequest
	var response pb.TimeoutNowResponse
	protoToTimeoutNowRequest(request, &command)
	result, err := executeRPC[*raft.TimeoutNowResponse](ctx, command, nil, r.rpcCh)
	if err != nil {
		return nil, err
	}
	timeoutNowResponseToProto(result, &response)
	return &response, nil
}

func (r *Raft) InstallSnapshot(stream pb.RaftService_InstallSnapshotServer) error {
	ctx := stream.Context()
	request, err := stream.Recv()
	if err != nil {
		return err
	}

	var command raft.InstallSnapshotRequest
	protoToInstallSnapshotRequest(request, &command)
	streamWrapper := &snapshotStream{stream: stream, buf: request.GetData()}
	result, err := executeRPC[*raft.InstallSnapshotResponse](ctx, command, streamWrapper, r.rpcCh)
	if err != nil {
		return err
	}

	var response pb.InstallSnapshotResponse
	installSnapshotResponseToProto(result, &response)
	return stream.SendAndClose(&response)
}

func executeRPC[T any](ctx context.Context, command any, reader io.Reader, rpcCh chan raft.RPC) (T, error) {
	respCh := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{Command: command, Reader: reader, RespChan: respCh}
	var zero T

	select {
	case rpcCh <- rpc:
	case <-ctx.Done():
		return zero, raft.ErrRaftShutdown
	}
	select {
	case resp := <-respCh:
		if resp.Error != nil {
			return zero, raft.ErrRaftShutdown
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
