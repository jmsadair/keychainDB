package raftnet

import (
	"context"

	pb "github.com/jmsadair/keychainDB/proto/raft"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/metadata"
)

type mockAppendEntriesPipelineServer struct {
	mock.Mock
}

func (m *mockAppendEntriesPipelineServer) Send(resp *pb.AppendEntriesResponse) error {
	args := m.MethodCalled("Send", resp)
	return args.Error(0)
}

func (m *mockAppendEntriesPipelineServer) Recv() (*pb.AppendEntriesRequest, error) {
	args := m.MethodCalled("Recv")
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.AppendEntriesRequest), args.Error(1)
}

func (m *mockAppendEntriesPipelineServer) Context() context.Context {
	args := m.MethodCalled("Context")
	return args.Get(0).(context.Context)
}

func (m *mockAppendEntriesPipelineServer) SendMsg(msg any) error {
	args := m.MethodCalled("SendMsg", msg)
	return args.Error(0)
}

func (m *mockAppendEntriesPipelineServer) RecvMsg(msg any) error {
	args := m.MethodCalled("RecvMsg", msg)
	return args.Error(0)
}

func (m *mockAppendEntriesPipelineServer) SetHeader(md metadata.MD) error {
	args := m.MethodCalled("SetHeader", md)
	return args.Error(0)
}

func (m *mockAppendEntriesPipelineServer) SendHeader(md metadata.MD) error {
	args := m.MethodCalled("SendHeader", md)
	return args.Error(0)
}

func (m *mockAppendEntriesPipelineServer) SetTrailer(md metadata.MD) {
	m.MethodCalled("SetTrailer", md)
}

type mockInstallSnapshotServer struct {
	mock.Mock
}

func (m *mockInstallSnapshotServer) SendAndClose(resp *pb.InstallSnapshotResponse) error {
	args := m.MethodCalled("SendAndClose", resp)
	return args.Error(0)
}

func (m *mockInstallSnapshotServer) Recv() (*pb.InstallSnapshotRequest, error) {
	args := m.MethodCalled("Recv")
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.InstallSnapshotRequest), args.Error(1)
}

func (m *mockInstallSnapshotServer) Context() context.Context {
	args := m.MethodCalled("Context")
	return args.Get(0).(context.Context)
}

func (m *mockInstallSnapshotServer) SendMsg(msg any) error {
	args := m.MethodCalled("SendMsg", msg)
	return args.Error(0)
}

func (m *mockInstallSnapshotServer) RecvMsg(msg any) error {
	args := m.MethodCalled("RecvMsg", msg)
	return args.Error(0)
}

func (m *mockInstallSnapshotServer) SetHeader(md metadata.MD) error {
	args := m.MethodCalled("SetHeader", md)
	return args.Error(0)
}

func (m *mockInstallSnapshotServer) SendHeader(md metadata.MD) error {
	args := m.MethodCalled("SendHeader", md)
	return args.Error(0)
}

func (m *mockInstallSnapshotServer) SetTrailer(md metadata.MD) {
	m.MethodCalled("SetTrailer", md)
}

type mockClient struct {
	mock.Mock
}

func (m *mockClient) AppendEntries(ctx context.Context, address string, request *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	args := m.MethodCalled("AppendEntries", ctx, address, request)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.AppendEntriesResponse), args.Error(1)
}

func (m *mockClient) AppendEntriesPipeline(ctx context.Context, address string) (pb.RaftService_AppendEntriesPipelineClient, error) {
	args := m.MethodCalled("AppendEntriesPipeline", ctx, address)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(pb.RaftService_AppendEntriesPipelineClient), args.Error(1)
}

func (m *mockClient) RequestVote(ctx context.Context, address string, request *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	args := m.MethodCalled("RequestVote", ctx, address, request)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.RequestVoteResponse), args.Error(1)
}

func (m *mockClient) RequestPreVote(ctx context.Context, address string, request *pb.RequestPreVoteRequest) (*pb.RequestPreVoteResponse, error) {
	args := m.MethodCalled("RequestPreVote", ctx, address, request)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.RequestPreVoteResponse), args.Error(1)
}

func (m *mockClient) TimeoutNow(ctx context.Context, address string, request *pb.TimeoutNowRequest) (*pb.TimeoutNowResponse, error) {
	args := m.MethodCalled("TimeoutNow", ctx, address, request)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.TimeoutNowResponse), args.Error(1)
}

func (m *mockClient) InstallSnapshot(ctx context.Context, address string) (pb.RaftService_InstallSnapshotClient, error) {
	args := m.MethodCalled("InstallSnapshot", ctx, address)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(pb.RaftService_InstallSnapshotClient), args.Error(1)
}

func (m *mockClient) Close() error {
	args := m.MethodCalled("Close")
	return args.Error(0)
}

type mockAppendEntriesPipelineClient struct {
	mock.Mock
}

func (m *mockAppendEntriesPipelineClient) Send(req *pb.AppendEntriesRequest) error {
	args := m.MethodCalled("Send", req)
	return args.Error(0)
}

func (m *mockAppendEntriesPipelineClient) Recv() (*pb.AppendEntriesResponse, error) {
	args := m.MethodCalled("Recv")
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.AppendEntriesResponse), args.Error(1)
}

func (m *mockAppendEntriesPipelineClient) Context() context.Context {
	args := m.MethodCalled("Context")
	return args.Get(0).(context.Context)
}

func (m *mockAppendEntriesPipelineClient) CloseSend() error {
	args := m.MethodCalled("CloseSend")
	return args.Error(0)
}

func (m *mockAppendEntriesPipelineClient) SendMsg(msg any) error {
	args := m.MethodCalled("SendMsg", msg)
	return args.Error(0)
}

func (m *mockAppendEntriesPipelineClient) RecvMsg(msg any) error {
	args := m.MethodCalled("RecvMsg", msg)
	return args.Error(0)
}

func (m *mockAppendEntriesPipelineClient) Header() (metadata.MD, error) {
	args := m.MethodCalled("Header")
	return args.Get(0).(metadata.MD), args.Error(1)
}

func (m *mockAppendEntriesPipelineClient) Trailer() metadata.MD {
	args := m.MethodCalled("Trailer")
	return args.Get(0).(metadata.MD)
}

type mockInstallSnapshotClient struct {
	mock.Mock
}

func (m *mockInstallSnapshotClient) SendMsg(msg any) error {
	args := m.MethodCalled("SendMsg", msg)
	return args.Error(0)
}

func (m *mockInstallSnapshotClient) Send(msg *pb.InstallSnapshotRequest) error {
	args := m.MethodCalled("Send", msg)
	return args.Error(0)
}

func (m *mockInstallSnapshotClient) Receive() (*pb.InstallSnapshotResponse, error) {
	args := m.MethodCalled("Receieve")
	return args.Get(0).(*pb.InstallSnapshotResponse), args.Error(1)
}

func (m *mockInstallSnapshotClient) CloseAndRecv() (*pb.InstallSnapshotResponse, error) {
	args := m.MethodCalled("CloseAndRecv")
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pb.InstallSnapshotResponse), args.Error(1)
}

func (m *mockInstallSnapshotClient) Context() context.Context {
	args := m.MethodCalled("Context")
	return args.Get(0).(context.Context)
}

func (m *mockInstallSnapshotClient) RecvMsg(msg any) error {
	args := m.MethodCalled("RecvMsg", msg)
	return args.Error(0)
}

func (m *mockInstallSnapshotClient) CloseSend() error {
	args := m.MethodCalled("CloseSend")
	return args.Error(0)
}

func (m *mockInstallSnapshotClient) Header() (metadata.MD, error) {
	args := m.MethodCalled("Header")
	return args.Get(0).(metadata.MD), args.Error(1)
}

func (m *mockInstallSnapshotClient) Trailer() metadata.MD {
	args := m.MethodCalled("Trailer")
	return args.Get(0).(metadata.MD)
}
