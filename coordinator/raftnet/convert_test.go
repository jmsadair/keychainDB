package raftnet

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/jmsadair/keychain/proto/raft"
	"github.com/stretchr/testify/require"
)

func TestAppendEntriesRequestConversion(t *testing.T) {
	req := &raft.AppendEntriesRequest{
		RPCHeader: raft.RPCHeader{
			ProtocolVersion: 3,
			ID:              []byte("node-1"),
			Addr:            []byte("127.0.0.1:8080"),
		},
		Term:              5,
		PrevLogEntry:      10,
		PrevLogTerm:       4,
		LeaderCommitIndex: 9,
		Entries: []*raft.Log{
			{
				Index:      11,
				Term:       5,
				Type:       raft.LogCommand,
				Data:       []byte("data"),
				Extensions: []byte("ext"),
				AppendedAt: time.Now(),
			},
		},
	}

	reqPb := &pb.AppendEntriesRequest{}
	appendEntriesRequestToProto(req, reqPb)

	require.NotNil(t, reqPb.GetRpcHeader())
	require.Equal(t, int32(req.ProtocolVersion), reqPb.GetRpcHeader().GetProtocolVersion())
	require.Equal(t, req.ID, reqPb.GetRpcHeader().GetId())
	require.Equal(t, req.Addr, reqPb.GetRpcHeader().GetAddr())

	result := &raft.AppendEntriesRequest{}
	protoToAppendEntriesRequest(reqPb, result)

	require.Equal(t, req.ProtocolVersion, result.ProtocolVersion)
	require.Equal(t, req.ID, result.ID)
	require.Equal(t, req.Addr, result.Addr)
	require.Equal(t, req.Term, result.Term)
	require.Equal(t, req.PrevLogEntry, result.PrevLogEntry)
	require.Equal(t, req.PrevLogTerm, result.PrevLogTerm)
	require.Equal(t, req.LeaderCommitIndex, result.LeaderCommitIndex)
	require.Len(t, result.Entries, len(req.Entries))
}

func TestAppendEntriesResponseConversion(t *testing.T) {
	resp := &raft.AppendEntriesResponse{
		RPCHeader: raft.RPCHeader{
			ProtocolVersion: 3,
			ID:              []byte("node-1"),
			Addr:            []byte("127.0.0.1:8080"),
		},
		Term:           5,
		LastLog:        15,
		Success:        true,
		NoRetryBackoff: false,
	}

	respPb := &pb.AppendEntriesResponse{}
	appendEntriesResponseToProto(resp, respPb)

	require.NotNil(t, respPb.GetRpcHeader())
	require.Equal(t, int32(resp.ProtocolVersion), respPb.GetRpcHeader().GetProtocolVersion())
	require.Equal(t, resp.ID, respPb.GetRpcHeader().GetId())
	require.Equal(t, resp.Addr, respPb.GetRpcHeader().GetAddr())

	result := &raft.AppendEntriesResponse{}
	protoToAppendEntriesResponse(respPb, result)

	require.Equal(t, resp.ProtocolVersion, result.ProtocolVersion)
	require.Equal(t, resp.ID, result.ID)
	require.Equal(t, resp.Addr, result.Addr)
	require.Equal(t, resp.Term, result.Term)
	require.Equal(t, resp.LastLog, result.LastLog)
	require.Equal(t, resp.Success, result.Success)
	require.Equal(t, resp.NoRetryBackoff, result.NoRetryBackoff)
}

func TestRequestVoteRequestConversion(t *testing.T) {
	req := &raft.RequestVoteRequest{
		RPCHeader: raft.RPCHeader{
			ProtocolVersion: 3,
			ID:              []byte("node-1"),
			Addr:            []byte("127.0.0.1:8080"),
		},
		Term:               5,
		LastLogIndex:       10,
		LastLogTerm:        4,
		LeadershipTransfer: true,
	}

	reqPb := &pb.RequestVoteRequest{}
	requestVoteRequestToProto(req, reqPb)

	require.NotNil(t, reqPb.GetRpcHeader())
	require.Equal(t, int32(req.ProtocolVersion), reqPb.GetRpcHeader().GetProtocolVersion())
	require.Equal(t, req.ID, reqPb.GetRpcHeader().GetId())
	require.Equal(t, req.Addr, reqPb.GetRpcHeader().GetAddr())

	result := &raft.RequestVoteRequest{}
	protoToRequestVoteRequest(reqPb, result)

	require.Equal(t, req.ProtocolVersion, result.ProtocolVersion)
	require.Equal(t, req.ID, result.ID)
	require.Equal(t, req.Addr, result.Addr)
	require.Equal(t, req.Term, result.Term)
	require.Equal(t, req.LastLogIndex, result.LastLogIndex)
	require.Equal(t, req.LastLogTerm, result.LastLogTerm)
	require.Equal(t, req.LeadershipTransfer, result.LeadershipTransfer)
}

func TestRequestVoteResponseConversion(t *testing.T) {
	resp := &raft.RequestVoteResponse{
		RPCHeader: raft.RPCHeader{
			ProtocolVersion: 3,
			ID:              []byte("node-1"),
			Addr:            []byte("127.0.0.1:8080"),
		},
		Term:    5,
		Peers:   []byte("peers"),
		Granted: true,
	}

	respPb := &pb.RequestVoteResponse{}
	requestVoteResponseToProto(resp, respPb)

	require.NotNil(t, respPb.GetRpcHeader())
	require.Equal(t, int32(resp.ProtocolVersion), respPb.GetRpcHeader().GetProtocolVersion())
	require.Equal(t, resp.ID, respPb.GetRpcHeader().GetId())
	require.Equal(t, resp.Addr, respPb.GetRpcHeader().GetAddr())

	result := &raft.RequestVoteResponse{}
	protoToRequestVoteResponse(respPb, result)

	require.Equal(t, resp.ProtocolVersion, result.ProtocolVersion)
	require.Equal(t, resp.ID, result.ID)
	require.Equal(t, resp.Addr, result.Addr)
	require.Equal(t, resp.Term, result.Term)
	require.Equal(t, resp.Peers, result.Peers)
	require.Equal(t, resp.Granted, result.Granted)
}

func TestRequestPreVoteRequestConversion(t *testing.T) {
	req := &raft.RequestPreVoteRequest{
		RPCHeader: raft.RPCHeader{
			ProtocolVersion: 3,
			ID:              []byte("node-1"),
			Addr:            []byte("127.0.0.1:8080"),
		},
		Term:         5,
		LastLogIndex: 10,
		LastLogTerm:  4,
	}

	reqPb := &pb.RequestPreVoteRequest{}
	requestPreVoteRequestToProto(req, reqPb)

	require.NotNil(t, reqPb.GetRpcHeader())
	require.Equal(t, int32(req.ProtocolVersion), reqPb.GetRpcHeader().GetProtocolVersion())
	require.Equal(t, req.ID, reqPb.GetRpcHeader().GetId())
	require.Equal(t, req.Addr, reqPb.GetRpcHeader().GetAddr())

	result := &raft.RequestPreVoteRequest{}
	protoToRequestPreVoteRequest(reqPb, result)

	require.Equal(t, req.ProtocolVersion, result.ProtocolVersion)
	require.Equal(t, req.ID, result.ID)
	require.Equal(t, req.Addr, result.Addr)
	require.Equal(t, req.Term, result.Term)
	require.Equal(t, req.LastLogIndex, result.LastLogIndex)
	require.Equal(t, req.LastLogTerm, result.LastLogTerm)
}

func TestRequestPreVoteResponseConversion(t *testing.T) {
	resp := &raft.RequestPreVoteResponse{
		RPCHeader: raft.RPCHeader{
			ProtocolVersion: 3,
			ID:              []byte("node-1"),
			Addr:            []byte("127.0.0.1:8080"),
		},
		Term:    5,
		Granted: true,
	}

	respPb := &pb.RequestPreVoteResponse{}
	requestPreVoteResponseToProto(resp, respPb)

	require.NotNil(t, respPb.GetRpcHeader())
	require.Equal(t, int32(resp.ProtocolVersion), respPb.GetRpcHeader().GetProtocolVersion())
	require.Equal(t, resp.ID, respPb.GetRpcHeader().GetId())
	require.Equal(t, resp.Addr, respPb.GetRpcHeader().GetAddr())

	result := &raft.RequestPreVoteResponse{}
	protoToRequestPreVoteResponse(respPb, result)

	require.Equal(t, resp.ProtocolVersion, result.ProtocolVersion)
	require.Equal(t, resp.ID, result.ID)
	require.Equal(t, resp.Addr, result.Addr)
	require.Equal(t, resp.Term, result.Term)
	require.Equal(t, resp.Granted, result.Granted)
}

func TestInstallSnapshotRequestConversion(t *testing.T) {
	req := &raft.InstallSnapshotRequest{
		RPCHeader: raft.RPCHeader{
			ProtocolVersion: 3,
			ID:              []byte("node-1"),
			Addr:            []byte("127.0.0.1:8080"),
		},
		SnapshotVersion:    1,
		Term:               5,
		Leader:             []byte("leader"),
		LastLogIndex:       100,
		LastLogTerm:        4,
		Peers:              []byte("peers"),
		Configuration:      []byte("config"),
		ConfigurationIndex: 95,
		Size:               1024,
	}

	reqPb := &pb.InstallSnapshotRequest{}
	installSnapshotRequestToProto(req, reqPb)

	require.NotNil(t, reqPb.GetRpcHeader())
	require.Equal(t, int32(req.ProtocolVersion), reqPb.GetRpcHeader().GetProtocolVersion())
	require.Equal(t, req.ID, reqPb.GetRpcHeader().GetId())
	require.Equal(t, req.Addr, reqPb.GetRpcHeader().GetAddr())

	result := &raft.InstallSnapshotRequest{}
	protoToInstallSnapshotRequest(reqPb, result)

	require.Equal(t, req.ProtocolVersion, result.ProtocolVersion)
	require.Equal(t, req.ID, result.ID)
	require.Equal(t, req.Addr, result.Addr)
	require.Equal(t, req.SnapshotVersion, result.SnapshotVersion)
	require.Equal(t, req.Term, result.Term)
	require.Equal(t, req.Leader, result.Leader)
	require.Equal(t, req.LastLogIndex, result.LastLogIndex)
	require.Equal(t, req.LastLogTerm, result.LastLogTerm)
	require.Equal(t, req.Peers, result.Peers)
	require.Equal(t, req.Configuration, result.Configuration)
	require.Equal(t, req.ConfigurationIndex, result.ConfigurationIndex)
	require.Equal(t, req.Size, result.Size)
}

func TestInstallSnapshotResponseConversion(t *testing.T) {
	resp := &raft.InstallSnapshotResponse{
		RPCHeader: raft.RPCHeader{
			ProtocolVersion: 3,
			ID:              []byte("node-1"),
			Addr:            []byte("127.0.0.1:8080"),
		},
		Term:    5,
		Success: true,
	}

	respPb := &pb.InstallSnapshotResponse{}
	installSnapshotResponseToProto(resp, respPb)

	require.NotNil(t, respPb.GetRpcHeader())
	require.Equal(t, int32(resp.ProtocolVersion), respPb.GetRpcHeader().GetProtocolVersion())
	require.Equal(t, resp.ID, respPb.GetRpcHeader().GetId())
	require.Equal(t, resp.Addr, respPb.GetRpcHeader().GetAddr())

	result := &raft.InstallSnapshotResponse{}
	protoToInstallSnapshotResponse(respPb, result)

	require.Equal(t, resp.ProtocolVersion, result.ProtocolVersion)
	require.Equal(t, resp.ID, result.ID)
	require.Equal(t, resp.Addr, result.Addr)
	require.Equal(t, resp.Term, result.Term)
	require.Equal(t, resp.Success, result.Success)
}

func TestTimeoutNowRequestConversion(t *testing.T) {
	req := &raft.TimeoutNowRequest{
		RPCHeader: raft.RPCHeader{
			ProtocolVersion: 3,
			ID:              []byte("node-1"),
			Addr:            []byte("127.0.0.1:8080"),
		},
	}

	reqPb := &pb.TimeoutNowRequest{}
	timeoutNowRequestToProto(req, reqPb)

	require.NotNil(t, reqPb.GetRpcHeader())
	require.Equal(t, int32(req.ProtocolVersion), reqPb.GetRpcHeader().GetProtocolVersion())
	require.Equal(t, req.ID, reqPb.GetRpcHeader().GetId())
	require.Equal(t, req.Addr, reqPb.GetRpcHeader().GetAddr())

	result := &raft.TimeoutNowRequest{}
	protoToTimeoutNowRequest(reqPb, result)

	require.Equal(t, req.ProtocolVersion, result.ProtocolVersion)
	require.Equal(t, req.ID, result.ID)
	require.Equal(t, req.Addr, result.Addr)
}

func TestTimeoutNowResponseConversion(t *testing.T) {
	resp := &raft.TimeoutNowResponse{
		RPCHeader: raft.RPCHeader{
			ProtocolVersion: 3,
			ID:              []byte("node-1"),
			Addr:            []byte("127.0.0.1:8080"),
		},
	}

	respPb := &pb.TimeoutNowResponse{}
	timeoutNowResponseToProto(resp, respPb)

	require.NotNil(t, respPb.GetRpcHeader())
	require.Equal(t, int32(resp.ProtocolVersion), respPb.GetRpcHeader().GetProtocolVersion())
	require.Equal(t, resp.ID, respPb.GetRpcHeader().GetId())
	require.Equal(t, resp.Addr, respPb.GetRpcHeader().GetAddr())

	result := &raft.TimeoutNowResponse{}
	protoToTimeoutNowResponse(respPb, result)

	require.Equal(t, resp.ProtocolVersion, result.ProtocolVersion)
	require.Equal(t, resp.ID, result.ID)
	require.Equal(t, resp.Addr, result.Addr)
}

func TestLogConversion(t *testing.T) {
	log := &raft.Log{
		Index:      42,
		Term:       5,
		Type:       raft.LogCommand,
		Data:       []byte("data"),
		Extensions: []byte("extensions"),
		AppendedAt: time.Now(),
	}

	logPb := &pb.Log{}
	logToProto(log, logPb)

	result := &raft.Log{}
	protoToLog(logPb, result)

	require.Equal(t, log.Index, result.Index)
	require.Equal(t, log.Term, result.Term)
	require.Equal(t, log.Type, result.Type)
	require.Equal(t, log.Data, result.Data)
	require.Equal(t, log.Extensions, result.Extensions)
	require.WithinDuration(t, log.AppendedAt, result.AppendedAt, time.Millisecond)
}
