package raftnet

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/jmsadair/keychain/proto/raft"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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

func TestServerAppendEntriesSuccess(t *testing.T) {
	nw := &Network{rpcCh: make(chan raft.RPC, 1), address: "127.0.0.1:8080"}
	srv := server{nw: nw}

	request := &pb.AppendEntriesRequest{
		RpcHeader: &pb.RpcHeader{
			ProtocolVersion: 3,
			Id:              []byte("node1"),
			Addr:            []byte("192.168.1.10:9000"),
		},
		Term:              5,
		PrevLogEntry:      10,
		PrevLogTerm:       4,
		LeaderCommitIndex: 10,
		Entries:           []*pb.Log{{Index: 11, Term: 5, Type: 0, Data: []byte("data")}},
	}

	go func() {
		rpc := <-nw.rpcCh
		require.NotNil(t, rpc.Command)
		require.IsType(t, &raft.AppendEntriesRequest{}, rpc.Command)

		cmd := rpc.Command.(*raft.AppendEntriesRequest)
		require.Equal(t, uint64(5), cmd.Term)
		require.Equal(t, uint64(10), cmd.PrevLogEntry)

		rpc.RespChan <- raft.RPCResponse{
			Response: &raft.AppendEntriesResponse{
				Term:    6,
				LastLog: 11,
				Success: true,
			},
		}
	}()

	resp, err := srv.AppendEntries(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(6), resp.Term)
	require.Equal(t, uint64(11), resp.LastLog)
	require.True(t, resp.Success)
}

func TestServerAppendEntriesContextCanceledBeforeSend(t *testing.T) {
	nw := &Network{rpcCh: make(chan raft.RPC), address: "127.0.0.1:8080"}
	srv := server{nw: nw}

	request := &pb.AppendEntriesRequest{
		RpcHeader: &pb.RpcHeader{ProtocolVersion: 3},
		Term:      5,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	resp, err := srv.AppendEntries(ctx, request)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, context.Canceled, err)
}

func TestServerAppendEntriesContextCanceledWaitingForResponse(t *testing.T) {
	nw := &Network{rpcCh: make(chan raft.RPC, 1), address: "127.0.0.1:8080"}
	srv := server{nw: nw}

	request := &pb.AppendEntriesRequest{
		RpcHeader: &pb.RpcHeader{ProtocolVersion: 3},
		Term:      5,
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-nw.rpcCh
		cancel()
	}()

	resp, err := srv.AppendEntries(ctx, request)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, context.Canceled, err)
}

func TestServerAppendEntriesRPCError(t *testing.T) {
	nw := &Network{rpcCh: make(chan raft.RPC, 1), address: "127.0.0.1:8080"}
	srv := server{nw: nw}

	request := &pb.AppendEntriesRequest{
		RpcHeader: &pb.RpcHeader{ProtocolVersion: 3},
		Term:      5,
	}

	expectedErr := errors.New("rpc failed")
	go func() {
		rpc := <-nw.rpcCh
		rpc.RespChan <- raft.RPCResponse{
			Error: expectedErr,
		}
	}()

	resp, err := srv.AppendEntries(context.Background(), request)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, expectedErr, err)
}

func TestServerAppendEntriesPipelineSuccess(t *testing.T) {
	nw := &Network{rpcCh: make(chan raft.RPC, 10), address: "127.0.0.1:8080"}
	srv := server{nw: nw}

	mockStream := new(mockAppendEntriesPipelineServer)
	ctx := context.Background()

	mockStream.On("Context").Return(ctx)

	requests := []*pb.AppendEntriesRequest{
		{RpcHeader: &pb.RpcHeader{ProtocolVersion: 3}, Term: 1},
		{RpcHeader: &pb.RpcHeader{ProtocolVersion: 3}, Term: 2},
		{RpcHeader: &pb.RpcHeader{ProtocolVersion: 3}, Term: 3},
	}

	for _, req := range requests {
		mockStream.On("Recv").Return(req, nil).Once()
	}
	mockStream.On("Recv").Return(nil, io.EOF).Once()

	mockStream.On("Send", mock.MatchedBy(func(resp *pb.AppendEntriesResponse) bool {
		return resp.Term == 1 && resp.Success
	})).Return(nil).Once()
	mockStream.On("Send", mock.MatchedBy(func(resp *pb.AppendEntriesResponse) bool {
		return resp.Term == 2 && resp.Success
	})).Return(nil).Once()
	mockStream.On("Send", mock.MatchedBy(func(resp *pb.AppendEntriesResponse) bool {
		return resp.Term == 3 && resp.Success
	})).Return(nil).Once()

	go func() {
		for range 3 {
			rpc := <-nw.rpcCh
			cmd := rpc.Command.(*raft.AppendEntriesRequest)
			rpc.RespChan <- raft.RPCResponse{
				Response: &raft.AppendEntriesResponse{
					Term:    cmd.Term,
					Success: true,
				},
			}
		}
	}()

	err := srv.AppendEntriesPipeline(mockStream)
	require.Equal(t, io.EOF, err)
	mockStream.AssertExpectations(t)
}

func TestServerAppendEntriesPipelineRecvError(t *testing.T) {
	nw := &Network{rpcCh: make(chan raft.RPC, 1), address: "127.0.0.1:8080"}
	srv := server{nw: nw}

	expectedErr := errors.New("recv failed")
	mockStream := new(mockAppendEntriesPipelineServer)
	mockStream.On("Recv").Return(nil, expectedErr)

	err := srv.AppendEntriesPipeline(mockStream)
	require.Equal(t, expectedErr, err)
	mockStream.AssertExpectations(t)
}

func TestServerAppendEntriesPipelineSendError(t *testing.T) {
	nw := &Network{rpcCh: make(chan raft.RPC, 1), address: "127.0.0.1:8080"}
	srv := server{nw: nw}

	sendErr := errors.New("send failed")
	mockStream := new(mockAppendEntriesPipelineServer)
	ctx := context.Background()

	mockStream.On("Context").Return(ctx)
	mockStream.On("Recv").Return(&pb.AppendEntriesRequest{
		RpcHeader: &pb.RpcHeader{ProtocolVersion: 3},
		Term:      1,
	}, nil).Once()
	mockStream.On("Send", mock.Anything).Return(sendErr)

	go func() {
		rpc := <-nw.rpcCh
		rpc.RespChan <- raft.RPCResponse{
			Response: &raft.AppendEntriesResponse{Term: 1, Success: true},
		}
	}()

	err := srv.AppendEntriesPipeline(mockStream)
	require.Equal(t, sendErr, err)
	mockStream.AssertExpectations(t)
}

func TestServerRequestVoteSuccess(t *testing.T) {
	nw := &Network{rpcCh: make(chan raft.RPC, 1), address: "127.0.0.1:8080"}
	srv := server{nw: nw}

	request := &pb.RequestVoteRequest{
		RpcHeader: &pb.RpcHeader{
			ProtocolVersion: 3,
			Id:              []byte("node2"),
			Addr:            []byte("192.168.1.11:9001"),
		},
		Term:         5,
		LastLogIndex: 10,
		LastLogTerm:  4,
	}

	go func() {
		rpc := <-nw.rpcCh
		require.IsType(t, &raft.RequestVoteRequest{}, rpc.Command)
		cmd := rpc.Command.(*raft.RequestVoteRequest)
		require.Equal(t, uint64(5), cmd.Term)

		rpc.RespChan <- raft.RPCResponse{
			Response: &raft.RequestVoteResponse{
				Term:    5,
				Granted: true,
			},
		}
	}()

	resp, err := srv.RequestVote(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(5), resp.Term)
	require.True(t, resp.Granted)
}

func TestServerRequestPreVoteSuccess(t *testing.T) {
	nw := &Network{rpcCh: make(chan raft.RPC, 1), address: "127.0.0.1:8080"}
	srv := server{nw: nw}

	request := &pb.RequestPreVoteRequest{
		RpcHeader: &pb.RpcHeader{
			ProtocolVersion: 3,
			Id:              []byte("node3"),
			Addr:            []byte("192.168.1.12:9002"),
		},
		Term:         5,
		LastLogIndex: 10,
		LastLogTerm:  4,
	}

	go func() {
		rpc := <-nw.rpcCh
		require.IsType(t, &raft.RequestPreVoteRequest{}, rpc.Command)
		cmd := rpc.Command.(*raft.RequestPreVoteRequest)
		require.Equal(t, uint64(5), cmd.Term)

		rpc.RespChan <- raft.RPCResponse{
			Response: &raft.RequestPreVoteResponse{
				Term:    5,
				Granted: true,
			},
		}
	}()

	resp, err := srv.RequestPreVote(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(5), resp.Term)
	require.True(t, resp.Granted)
}

func TestServerTimeoutNowSuccess(t *testing.T) {
	nw := &Network{rpcCh: make(chan raft.RPC, 1), address: "127.0.0.1:8080"}
	srv := server{nw: nw}

	request := &pb.TimeoutNowRequest{
		RpcHeader: &pb.RpcHeader{
			ProtocolVersion: 3,
			Id:              []byte("node4"),
			Addr:            []byte("192.168.1.13:9003"),
		},
	}

	go func() {
		rpc := <-nw.rpcCh
		require.IsType(t, &raft.TimeoutNowRequest{}, rpc.Command)

		rpc.RespChan <- raft.RPCResponse{
			Response: &raft.TimeoutNowResponse{
				RPCHeader: raft.RPCHeader{
					ProtocolVersion: 3,
					ID:              []byte("node4"),
					Addr:            []byte("192.168.1.13:9003"),
				},
			},
		}
	}()

	resp, err := srv.TimeoutNow(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, int32(3), resp.RpcHeader.ProtocolVersion)
}

func TestServerTimeoutNowContextCanceled(t *testing.T) {
	nw := &Network{rpcCh: make(chan raft.RPC), address: "127.0.0.1:8080"}
	srv := server{nw: nw}

	request := &pb.TimeoutNowRequest{
		RpcHeader: &pb.RpcHeader{ProtocolVersion: 3},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	resp, err := srv.TimeoutNow(ctx, request)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, context.Canceled, err)
}

func TestServerInstallSnapshotSuccess(t *testing.T) {
	nw := &Network{rpcCh: make(chan raft.RPC, 1), address: "127.0.0.1:8080"}
	srv := server{nw: nw}

	mockStream := new(mockInstallSnapshotServer)
	ctx := context.Background()
	mockStream.On("Context").Return(ctx)

	chunk1 := &pb.InstallSnapshotRequest{
		RpcHeader: &pb.RpcHeader{ProtocolVersion: 3},
		Term:      5,
		Leader:    []byte("192.168.1.10:9000"),
		Data:      []byte("abc"),
	}
	chunk2 := &pb.InstallSnapshotRequest{
		RpcHeader: &pb.RpcHeader{ProtocolVersion: 3},
		Data:      []byte("def"),
	}
	chunk3 := &pb.InstallSnapshotRequest{
		RpcHeader: &pb.RpcHeader{ProtocolVersion: 3},
		Data:      []byte("ghi"),
	}
	mockStream.On("Recv").Return(chunk1, nil).Once()
	mockStream.On("Recv").Return(chunk2, nil).Once()
	mockStream.On("Recv").Return(chunk3, nil).Once()
	mockStream.On("Recv").Return(nil, io.EOF).Once()
	mockStream.On("SendAndClose", mock.MatchedBy(func(resp *pb.InstallSnapshotResponse) bool {
		return resp.Term == 5 && resp.Success
	})).Return(nil)

	expectedData := []byte("abcdefghi")

	go func() {
		rpc := <-nw.rpcCh
		require.IsType(t, &raft.InstallSnapshotRequest{}, rpc.Command)
		require.NotNil(t, rpc.Reader)

		data, err := io.ReadAll(rpc.Reader)
		require.NoError(t, err)
		require.Equal(t, expectedData, data)

		rpc.RespChan <- raft.RPCResponse{
			Response: &raft.InstallSnapshotResponse{
				Term:    5,
				Success: true,
			},
		}
	}()

	err := srv.InstallSnapshot(mockStream)
	require.NoError(t, err)
	mockStream.AssertExpectations(t)
}

func TestServerInstallSnapshotRecvError(t *testing.T) {
	nw := &Network{rpcCh: make(chan raft.RPC, 1), address: "127.0.0.1:8080"}
	srv := server{nw: nw}

	mockStream := new(mockInstallSnapshotServer)
	ctx := context.Background()
	mockStream.On("Context").Return(ctx)
	expectedErr := errors.New("stream failed")
	mockStream.On("Recv").Return(nil, expectedErr)

	err := srv.InstallSnapshot(mockStream)
	require.Equal(t, expectedErr, err)
	mockStream.AssertExpectations(t)
}

func TestServerInstallSnapshotRPCError(t *testing.T) {
	nw := &Network{rpcCh: make(chan raft.RPC, 1), address: "127.0.0.1:8080"}
	srv := server{nw: nw}

	mockStream := new(mockInstallSnapshotServer)
	ctx := context.Background()
	mockStream.On("Context").Return(ctx)
	mockStream.On("Recv").Return(&pb.InstallSnapshotRequest{
		RpcHeader: &pb.RpcHeader{ProtocolVersion: 3},
		Term:      5,
		Data:      []byte("data"),
	}, nil).Once()

	expectedErr := errors.New("rpc failed")
	go func() {
		rpc := <-nw.rpcCh
		rpc.RespChan <- raft.RPCResponse{
			Error: expectedErr,
		}
	}()

	err := srv.InstallSnapshot(mockStream)
	require.Equal(t, expectedErr, err)
	mockStream.AssertExpectations(t)
}

func TestSnapshotStreamReadMultipleChunks(t *testing.T) {
	mockStream := new(mockInstallSnapshotServer)
	mockStream.On("Recv").Return(&pb.InstallSnapshotRequest{Data: []byte("123")}, nil).Once()
	mockStream.On("Recv").Return(&pb.InstallSnapshotRequest{Data: []byte("456")}, nil).Once()
	mockStream.On("Recv").Return(nil, io.EOF).Once()

	stream := &snapshotStream{stream: mockStream, buf: []byte("initial")}

	data, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, []byte("initial123456"), data)
	mockStream.AssertExpectations(t)
}

func TestSnapshotStreamReadWithSmallBuffer(t *testing.T) {
	mockStream := new(mockInstallSnapshotServer)
	mockStream.On("Recv").Return(&pb.InstallSnapshotRequest{Data: []byte("123abc456def")}, nil).Once()
	mockStream.On("Recv").Return(nil, io.EOF).Once()

	stream := &snapshotStream{stream: mockStream, buf: []byte{}}
	buf := make([]byte, 3)
	var result []byte
	for {
		n, err := stream.Read(buf)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		result = append(result, buf[:n]...)
	}
	require.Equal(t, []byte("123abc456def"), result)
	mockStream.AssertExpectations(t)
}

func TestSnapshotStreamRecvError(t *testing.T) {
	expectedErr := errors.New("stream failed")
	mockStream := new(mockInstallSnapshotServer)
	mockStream.On("Recv").Return(nil, expectedErr)

	stream := &snapshotStream{stream: mockStream, buf: []byte{}}

	buf := make([]byte, 10)
	n, err := stream.Read(buf)
	require.Equal(t, 0, n)
	require.Equal(t, expectedErr, err)
	mockStream.AssertExpectations(t)
}

func TestSnapshotStreamEmptyChunks(t *testing.T) {
	mockStream := new(mockInstallSnapshotServer)
	mockStream.On("Recv").Return(&pb.InstallSnapshotRequest{Data: []byte{}}, nil).Once()
	mockStream.On("Recv").Return(&pb.InstallSnapshotRequest{Data: []byte("data")}, nil).Once()
	mockStream.On("Recv").Return(nil, io.EOF).Once()

	stream := &snapshotStream{stream: mockStream, buf: []byte{}}
	data, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, []byte("data"), data)
	mockStream.AssertExpectations(t)
}

func TestExecuteRPCSuccess(t *testing.T) {
	rpcCh := make(chan raft.RPC, 1)
	ctx := context.Background()

	go func() {
		rpc := <-rpcCh
		rpc.RespChan <- raft.RPCResponse{
			Response: &raft.AppendEntriesResponse{Term: 5, Success: true},
		}
	}()

	result, err := executeRPC[*raft.AppendEntriesResponse](ctx, &raft.AppendEntriesRequest{Term: 5}, nil, rpcCh, func(raft.RPC) {})
	require.NoError(t, err)
	require.Equal(t, uint64(5), result.Term)
	require.True(t, result.Success)
}

func TestExecuteRPCContextCanceledBeforeSend(t *testing.T) {
	rpcCh := make(chan raft.RPC)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := executeRPC[*raft.AppendEntriesResponse](ctx, &raft.AppendEntriesRequest{}, nil, rpcCh, func(raft.RPC) {})
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, context.Canceled, err)
}

func TestExecuteRPCContextCanceledWaitingForResponse(t *testing.T) {
	rpcCh := make(chan raft.RPC, 1)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-rpcCh
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	result, err := executeRPC[*raft.AppendEntriesResponse](ctx, &raft.AppendEntriesRequest{}, nil, rpcCh, func(raft.RPC) {})
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, context.Canceled, err)
}

func TestExecuteRPCError(t *testing.T) {
	rpcCh := make(chan raft.RPC, 1)
	ctx := context.Background()

	expectedErr := errors.New("rpc failed")
	go func() {
		rpc := <-rpcCh
		rpc.RespChan <- raft.RPCResponse{
			Error: expectedErr,
		}
	}()

	result, err := executeRPC[*raft.AppendEntriesResponse](ctx, &raft.AppendEntriesRequest{}, nil, rpcCh, func(raft.RPC) {})
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, expectedErr, err)
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

func TestTransportLocalAddr(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()
	require.Equal(t, raft.ServerAddress("127.0.0.1:8080"), transport.LocalAddr())
}

func TestTransportRequestVoteSuccess(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()

	pbResp := &pb.RequestVoteResponse{
		RpcHeader: &pb.RpcHeader{ProtocolVersion: 3},
		Term:      5,
		Granted:   true,
	}
	client.On("RequestVote", mock.Anything, "192.168.1.10:9000", mock.Anything).Return(pbResp, nil)

	request := &raft.RequestVoteRequest{Term: 5, LastLogIndex: 10, LastLogTerm: 4}
	response := &raft.RequestVoteResponse{}
	err := transport.RequestVote("node1", "192.168.1.10:9000", request, response)
	require.NoError(t, err)
	require.Equal(t, uint64(5), response.Term)
	require.True(t, response.Granted)
	client.AssertExpectations(t)
}

func TestTransportRequestVoteError(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()

	expectedErr := errors.New("vote failed")
	client.On("RequestVote", mock.Anything, "192.168.1.10:9000", mock.Anything).Return(nil, expectedErr)

	request := &raft.RequestVoteRequest{Term: 5}
	response := &raft.RequestVoteResponse{}
	err := transport.RequestVote("node1", "192.168.1.10:9000", request, response)
	require.Error(t, err)
	require.Equal(t, expectedErr, err)
	client.AssertExpectations(t)
}

func TestTransportRequestPreVoteSuccess(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()

	pbResp := &pb.RequestPreVoteResponse{RpcHeader: &pb.RpcHeader{ProtocolVersion: 3}, Term: 5, Granted: true}
	client.On("RequestPreVote", mock.Anything, "192.168.1.10:9000", mock.Anything).Return(pbResp, nil)

	request := &raft.RequestPreVoteRequest{Term: 5, LastLogIndex: 10, LastLogTerm: 4}
	response := &raft.RequestPreVoteResponse{}
	err := transport.RequestPreVote("node1", "192.168.1.10:9000", request, response)
	require.NoError(t, err)
	require.Equal(t, uint64(5), response.Term)
	require.True(t, response.Granted)
	client.AssertExpectations(t)
}

func TestTransportRequestPreVoteError(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()

	expectedErr := errors.New("prevote failed")
	client.On("RequestPreVote", mock.Anything, "192.168.1.10:9000", mock.Anything).Return(nil, expectedErr)

	request := &raft.RequestPreVoteRequest{Term: 5}
	response := &raft.RequestPreVoteResponse{}
	err := transport.RequestPreVote("node1", "192.168.1.10:9000", request, response)
	require.Error(t, err)
	require.Equal(t, expectedErr, err)
	client.AssertExpectations(t)
}

func TestTransportAppendEntriesSuccess(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()

	pbResp := &pb.AppendEntriesResponse{RpcHeader: &pb.RpcHeader{ProtocolVersion: 3}, Term: 6, LastLog: 11, Success: true}
	client.On("AppendEntries", mock.Anything, "192.168.1.10:9000", mock.Anything).Return(pbResp, nil)

	request := &raft.AppendEntriesRequest{Term: 5, PrevLogEntry: 10, PrevLogTerm: 4}
	response := &raft.AppendEntriesResponse{}
	err := transport.AppendEntries("node1", "192.168.1.10:9000", request, response)
	require.NoError(t, err)
	require.Equal(t, uint64(6), response.Term)
	require.Equal(t, uint64(11), response.LastLog)
	require.True(t, response.Success)
	client.AssertExpectations(t)
}

func TestTransportAppendEntriesError(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()

	expectedErr := errors.New("append failed")
	client.On("AppendEntries", mock.Anything, "192.168.1.10:9000", mock.Anything).Return(nil, expectedErr)

	request := &raft.AppendEntriesRequest{Term: 5}
	response := &raft.AppendEntriesResponse{}

	err := transport.AppendEntries("node1", "192.168.1.10:9000", request, response)
	require.Error(t, err)
	require.Equal(t, expectedErr, err)
	client.AssertExpectations(t)
}

func TestTransportAppendEntriesPipelineSuccess(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()

	mockPipelineClient := new(mockAppendEntriesPipelineClient)
	mockPipelineClient.On("Context").Return(t.Context())
	client.On("AppendEntriesPipeline", mock.Anything, "192.168.1.10:9000").Return(mockPipelineClient, nil)

	pipeline, err := transport.AppendEntriesPipeline("node1", "192.168.1.10:9000")
	require.NoError(t, err)
	require.NotNil(t, pipeline)
	client.AssertExpectations(t)
}

func TestTransportAppendEntriesPipelineError(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()

	expectedErr := errors.New("pipeline failed")
	client.On("AppendEntriesPipeline", mock.Anything, "192.168.1.10:9000").Return(nil, expectedErr)

	pipeline, err := transport.AppendEntriesPipeline("node1", "192.168.1.10:9000")
	require.Error(t, err)
	require.Nil(t, pipeline)
	require.Equal(t, expectedErr, err)
	client.AssertExpectations(t)
}

func TestTransportTimeoutNowSuccess(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()

	pbResp := &pb.TimeoutNowResponse{RpcHeader: &pb.RpcHeader{ProtocolVersion: 3}}
	client.On("TimeoutNow", mock.Anything, "192.168.1.10:9000", mock.Anything).Return(pbResp, nil)

	request := &raft.TimeoutNowRequest{}
	response := &raft.TimeoutNowResponse{}
	err := transport.TimeoutNow("node1", "192.168.1.10:9000", request, response)
	require.NoError(t, err)
	require.Equal(t, raft.ProtocolVersion(3), response.ProtocolVersion)
	client.AssertExpectations(t)
}

func TestTransportTimeoutNowError(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()

	expectedErr := errors.New("timeout failed")
	client.On("TimeoutNow", mock.Anything, "192.168.1.10:9000", mock.Anything).Return(nil, expectedErr)

	request := &raft.TimeoutNowRequest{}
	response := &raft.TimeoutNowResponse{}

	err := transport.TimeoutNow("node1", "192.168.1.10:9000", request, response)
	require.Error(t, err)
	require.Equal(t, expectedErr, err)
	client.AssertExpectations(t)
}

func TestTransportInstallSnapshotSuccess(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()

	mockSnapshotClient := new(mockInstallSnapshotClient)
	client.On("InstallSnapshot", mock.Anything, "192.168.1.10:9000").Return(mockSnapshotClient, nil)

	mockSnapshotClient.On("SendMsg", mock.Anything).Return(nil).Once()
	mockSnapshotClient.On("CloseAndRecv").Return(&pb.InstallSnapshotResponse{
		RpcHeader: &pb.RpcHeader{ProtocolVersion: 3},
		Term:      5,
		Success:   true,
	}, nil)

	request := &raft.InstallSnapshotRequest{Term: 5, Leader: []byte("192.168.1.10:9000")}
	response := &raft.InstallSnapshotResponse{}
	data := bytes.NewReader([]byte("abcdefghi"))
	err := transport.InstallSnapshot("node1", "192.168.1.10:9000", request, response, data)
	require.NoError(t, err)
	require.Equal(t, uint64(5), response.Term)
	require.True(t, response.Success)
	client.AssertExpectations(t)
	mockSnapshotClient.AssertExpectations(t)
}

func TestTransportInstallSnapshotClientError(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()

	expectedErr := errors.New("snapshot client failed")
	client.On("InstallSnapshot", mock.Anything, "192.168.1.10:9000").Return(nil, expectedErr)

	request := &raft.InstallSnapshotRequest{Term: 5}
	response := &raft.InstallSnapshotResponse{}
	data := bytes.NewReader([]byte("data"))
	err := transport.InstallSnapshot("node1", "192.168.1.10:9000", request, response, data)
	require.Error(t, err)
	require.Equal(t, expectedErr, err)
	client.AssertExpectations(t)
}

func TestTransportInstallSnapshotSendError(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()

	mockSnapshotClient := new(mockInstallSnapshotClient)
	client.On("InstallSnapshot", mock.Anything, "192.168.1.10:9000").Return(mockSnapshotClient, nil)

	expectedErr := errors.New("send failed")
	mockSnapshotClient.On("SendMsg", mock.Anything).Return(expectedErr)

	request := &raft.InstallSnapshotRequest{Term: 5}
	response := &raft.InstallSnapshotResponse{}
	data := bytes.NewReader([]byte("data"))
	err := transport.InstallSnapshot("node1", "192.168.1.10:9000", request, response, data)
	require.Error(t, err)
	require.Equal(t, expectedErr, err)
	client.AssertExpectations(t)
	mockSnapshotClient.AssertExpectations(t)
}

func TestTransportInstallSnapshotCloseError(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()

	mockSnapshotClient := new(mockInstallSnapshotClient)
	client.On("InstallSnapshot", mock.Anything, "192.168.1.10:9000").Return(mockSnapshotClient, nil)

	mockSnapshotClient.On("SendMsg", mock.Anything).Return(nil).Once()
	expectedErr := errors.New("close failed")
	mockSnapshotClient.On("CloseAndRecv").Return(nil, expectedErr)

	request := &raft.InstallSnapshotRequest{Term: 5}
	response := &raft.InstallSnapshotResponse{}
	data := bytes.NewReader([]byte("data"))
	err := transport.InstallSnapshot("node1", "192.168.1.10:9000", request, response, data)
	require.Error(t, err)
	require.Equal(t, expectedErr, err)
	client.AssertExpectations(t)
	mockSnapshotClient.AssertExpectations(t)
}

func TestTransportEncodePeer(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()
	encoded := transport.EncodePeer("node1", "192.168.1.10:9000")
	require.Equal(t, []byte("192.168.1.10:9000"), encoded)
}

func TestTransportDecodePeer(t *testing.T) {
	client := new(mockClient)
	nw := newNetworkWithClient("127.0.0.1:8080", client)
	transport := nw.Transport()
	decoded := transport.DecodePeer([]byte("192.168.1.10:9000"))
	require.Equal(t, raft.ServerAddress("192.168.1.10:9000"), decoded)
}

func TestAppendFutureStart(t *testing.T) {
	start := time.Now()
	future := &appendFuture{start: start}
	require.Equal(t, start, future.Start())
}

func TestAppendFutureRequest(t *testing.T) {
	request := &raft.AppendEntriesRequest{Term: 5}
	future := &appendFuture{request: request}
	require.Equal(t, request, future.Request())
}

func TestAppendFutureResponse(t *testing.T) {
	response := &raft.AppendEntriesResponse{Term: 5, Success: true}
	future := &appendFuture{response: response}
	require.Equal(t, response, future.Response())
}

func TestAppendFutureError(t *testing.T) {
	doneCh := make(chan any)
	expectedErr := errors.New("future error")
	future := &appendFuture{doneCh: doneCh, err: expectedErr}

	// Sleep to simulate waiting for response.
	go func() {
		time.Sleep(200 * time.Millisecond)
		close(doneCh)
	}()

	err := future.Error()
	require.Equal(t, expectedErr, err)
}

func TestAppendFutureErrorNil(t *testing.T) {
	doneCh := make(chan any)
	future := &appendFuture{doneCh: doneCh, err: nil}

	// Sleep to simulate waiting for response.
	go func() {
		time.Sleep(200 * time.Millisecond)
		close(doneCh)
	}()

	err := future.Error()
	require.NoError(t, err)
}

func TestAppendPipelineAppendEntriesSuccess(t *testing.T) {
	mockStream := new(mockAppendEntriesPipelineClient)
	ctx, cancel := context.WithCancel(t.Context())
	mockStream.On("Context").Return(ctx)
	mockStream.On("Send", mock.Anything).Return(nil)

	pipeline := newAppendPipeline(cancel, mockStream)
	defer pipeline.Close()

	request := &raft.AppendEntriesRequest{Term: 5}
	response := &raft.AppendEntriesResponse{Success: true, Term: 5, LastLog: 10}
	mockStream.On("Recv").Return(&pb.AppendEntriesResponse{Success: true, Term: 5, LastLog: 10}, nil)

	future, err := pipeline.AppendEntries(request, response)
	require.NoError(t, err)
	require.NotNil(t, future)
	require.NoError(t, future.Error())
	require.Equal(t, request, future.Request())
	require.Equal(t, response, future.Response())
	mockStream.AssertExpectations(t)
}

func TestAppendPipelineAppendEntriesSendError(t *testing.T) {
	mockStream := new(mockAppendEntriesPipelineClient)
	ctx, cancel := context.WithCancel(t.Context())

	expectedErr := errors.New("stream failed")
	mockStream.On("Send", mock.Anything).Return(expectedErr)

	pipeline := newAppendPipeline(cancel, mockStream)
	defer func() {
		mockStream.On("Context").Return(ctx)
		pipeline.Close()
		mockStream.AssertExpectations(t)
	}()

	request := &raft.AppendEntriesRequest{Term: 5}
	response := &raft.AppendEntriesResponse{}
	future, err := pipeline.AppendEntries(request, response)
	require.Error(t, err)
	require.Nil(t, future)
	mockStream.AssertExpectations(t)

}

func TestAppendPipelineConsumer(t *testing.T) {
	mockStream := new(mockAppendEntriesPipelineClient)
	ctx, cancel := context.WithCancel(t.Context())
	mockStream.On("Context").Return(ctx)

	pipeline := newAppendPipeline(cancel, mockStream)
	defer pipeline.Close()

	consumer := pipeline.Consumer()
	require.NotNil(t, consumer)
}

func TestAppendPipelineClose(t *testing.T) {
	mockStream := new(mockAppendEntriesPipelineClient)
	ctx, cancel := context.WithCancel(t.Context())
	mockStream.On("Context").Return(ctx)

	pipeline := newAppendPipeline(cancel, mockStream)
	err := pipeline.Close()
	require.NoError(t, err)
}

func TestAppendPipelineProcessResponses(t *testing.T) {
	mockStream := new(mockAppendEntriesPipelineClient)
	ctx, cancel := context.WithCancel(t.Context())
	mockStream.On("Context").Return(ctx)
	mockStream.On("Send", mock.Anything).Return(nil)
	mockStream.On("Recv").Return(&pb.AppendEntriesResponse{Term: 5, Success: true}, nil)

	pipeline := newAppendPipeline(cancel, mockStream)
	defer pipeline.Close()

	request := &raft.AppendEntriesRequest{Term: 5}
	response := &raft.AppendEntriesResponse{}
	future, err := pipeline.AppendEntries(request, response)
	require.NoError(t, err)
	require.NotNil(t, future)

	doneFuture := <-pipeline.Consumer()
	require.NotNil(t, doneFuture)
	require.NoError(t, doneFuture.Error())
	require.Equal(t, uint64(5), doneFuture.Response().Term)
	require.True(t, doneFuture.Response().Success)
	mockStream.AssertExpectations(t)
}
