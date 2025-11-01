package raftnet

import (
	"github.com/hashicorp/raft"
	pb "github.com/jmsadair/keychain/proto/raft"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func appendEntriesRequestToProto(req *raft.AppendEntriesRequest, reqPb *pb.AppendEntriesRequest) {
	entries := make([]*pb.Log, len(req.Entries))
	for i, entry := range req.Entries {
		var logPb pb.Log
		logToProto(entry, &logPb)
		entries[i] = &logPb
	}

	reqPb.RpcHeader = &pb.RpcHeader{ProtocolVersion: int32(req.ProtocolVersion), Id: req.ID, Addr: req.Addr}
	reqPb.Term = req.Term
	reqPb.PrevLogEntry = req.PrevLogEntry
	reqPb.PrevLogTerm = req.PrevLogTerm
	reqPb.LeaderCommitIndex = req.LeaderCommitIndex
	reqPb.Entries = entries
}

func appendEntriesResponseToProto(resp *raft.AppendEntriesResponse, respPb *pb.AppendEntriesResponse) {
	respPb.RpcHeader = &pb.RpcHeader{ProtocolVersion: int32(resp.ProtocolVersion), Id: resp.ID, Addr: resp.Addr}
	respPb.Term = resp.Term
	respPb.LastLog = resp.LastLog
	respPb.Success = resp.Success
	respPb.NoRetryBackoff = resp.NoRetryBackoff
}

func requestVoteRequestToProto(req *raft.RequestVoteRequest, reqPb *pb.RequestVoteRequest) {
	reqPb.RpcHeader = &pb.RpcHeader{ProtocolVersion: int32(req.ProtocolVersion), Id: req.ID, Addr: req.Addr}
	reqPb.Term = req.Term
	reqPb.LastLogIndex = req.LastLogIndex
	reqPb.LastLogTerm = req.LastLogTerm
	reqPb.LeadershipTransfer = req.LeadershipTransfer
}

func requestVoteResponseToProto(resp *raft.RequestVoteResponse, respPb *pb.RequestVoteResponse) {
	respPb.RpcHeader = &pb.RpcHeader{ProtocolVersion: int32(resp.ProtocolVersion), Id: resp.ID, Addr: resp.Addr}
	respPb.Term = resp.Term
	respPb.Peers = resp.Peers
	respPb.Granted = resp.Granted
}

func requestPreVoteRequestToProto(req *raft.RequestPreVoteRequest, reqPb *pb.RequestPreVoteRequest) {
	reqPb.RpcHeader = &pb.RpcHeader{ProtocolVersion: int32(req.ProtocolVersion), Id: req.ID, Addr: req.Addr}
	reqPb.Term = req.Term
	reqPb.LastLogIndex = req.LastLogIndex
	reqPb.LastLogTerm = req.LastLogTerm
}

func requestPreVoteResponseToProto(resp *raft.RequestPreVoteResponse, respPb *pb.RequestPreVoteResponse) {
	respPb.RpcHeader = &pb.RpcHeader{ProtocolVersion: int32(resp.ProtocolVersion), Id: resp.ID, Addr: resp.Addr}
	respPb.Term = resp.Term
	respPb.Granted = resp.Granted
}

func installSnapshotRequestToProto(req *raft.InstallSnapshotRequest, reqPb *pb.InstallSnapshotRequest) {
	reqPb.RpcHeader = &pb.RpcHeader{ProtocolVersion: int32(req.ProtocolVersion), Id: req.ID, Addr: req.Addr}
	reqPb.SnapshotVersion = int32(req.SnapshotVersion)
	reqPb.Term = req.Term
	reqPb.Leader = req.Leader
	reqPb.LastLogIndex = req.LastLogIndex
	reqPb.LastLogTerm = req.LastLogTerm
	reqPb.Peers = req.Peers
	reqPb.Configuration = req.Configuration
	reqPb.ConfigurationIndex = req.ConfigurationIndex
	reqPb.Size = req.Size
}

func installSnapshotResponseToProto(resp *raft.InstallSnapshotResponse, respPb *pb.InstallSnapshotResponse) {
	respPb.RpcHeader = &pb.RpcHeader{ProtocolVersion: int32(resp.ProtocolVersion), Id: resp.ID, Addr: resp.Addr}
	respPb.Term = resp.Term
	respPb.Success = resp.Success
}

func timeoutNowRequestToProto(req *raft.TimeoutNowRequest, reqPb *pb.TimeoutNowRequest) {
	reqPb.RpcHeader = &pb.RpcHeader{ProtocolVersion: int32(req.ProtocolVersion), Id: req.ID, Addr: req.Addr}
}

func timeoutNowResponseToProto(resp *raft.TimeoutNowResponse, respPb *pb.TimeoutNowResponse) {
	respPb.RpcHeader = &pb.RpcHeader{ProtocolVersion: int32(resp.ProtocolVersion), Id: resp.ID, Addr: resp.Addr}
}

func logToProto(log *raft.Log, logPb *pb.Log) {
	var appendedAt *timestamppb.Timestamp
	if !log.AppendedAt.IsZero() {
		appendedAt = timestamppb.New(log.AppendedAt)
	}

	logPb.Index = log.Index
	logPb.Term = log.Term
	logPb.Type = uint32(log.Type)
	logPb.Data = log.Data
	logPb.Extensions = log.Extensions
	logPb.AppendedAt = appendedAt
}

func protoToAppendEntriesRequest(reqPb *pb.AppendEntriesRequest, req *raft.AppendEntriesRequest) {
	entries := make([]*raft.Log, len(reqPb.GetEntries()))
	for i, pbLog := range reqPb.GetEntries() {
		var log raft.Log
		protoToLog(pbLog, &log)
		entries[i] = &log
	}

	req.ProtocolVersion = raft.ProtocolVersion(reqPb.GetRpcHeader().GetProtocolVersion())
	req.ID = reqPb.GetRpcHeader().GetId()
	req.Addr = reqPb.GetRpcHeader().GetAddr()
	req.Term = reqPb.GetTerm()
	req.PrevLogEntry = reqPb.GetPrevLogEntry()
	req.PrevLogTerm = reqPb.GetPrevLogTerm()
	req.LeaderCommitIndex = reqPb.GetLeaderCommitIndex()
	req.Entries = entries
}

func protoToAppendEntriesResponse(respPb *pb.AppendEntriesResponse, resp *raft.AppendEntriesResponse) {
	resp.ProtocolVersion = raft.ProtocolVersion(respPb.GetRpcHeader().GetProtocolVersion())
	resp.ID = respPb.GetRpcHeader().GetId()
	resp.Addr = respPb.GetRpcHeader().GetAddr()
	resp.Term = respPb.GetTerm()
	resp.LastLog = respPb.GetLastLog()
	resp.Success = respPb.GetSuccess()
	resp.NoRetryBackoff = respPb.GetNoRetryBackoff()
}

func protoToRequestVoteRequest(reqPb *pb.RequestVoteRequest, req *raft.RequestVoteRequest) {
	req.ProtocolVersion = raft.ProtocolVersion(reqPb.GetRpcHeader().GetProtocolVersion())
	req.ID = reqPb.GetRpcHeader().GetId()
	req.Addr = reqPb.GetRpcHeader().GetAddr()
	req.Term = reqPb.GetTerm()
	req.LastLogIndex = reqPb.GetLastLogIndex()
	req.LastLogTerm = reqPb.GetLastLogTerm()
	req.LeadershipTransfer = reqPb.GetLeadershipTransfer()
}

func protoToRequestVoteResponse(respPb *pb.RequestVoteResponse, resp *raft.RequestVoteResponse) {
	resp.ProtocolVersion = raft.ProtocolVersion(respPb.GetRpcHeader().GetProtocolVersion())
	resp.ID = respPb.GetRpcHeader().GetId()
	resp.Addr = respPb.GetRpcHeader().GetAddr()
	resp.Term = respPb.GetTerm()
	resp.Peers = respPb.GetPeers()
	resp.Granted = respPb.GetGranted()
}

func protoToRequestPreVoteRequest(reqPb *pb.RequestPreVoteRequest, req *raft.RequestPreVoteRequest) {
	req.ProtocolVersion = raft.ProtocolVersion(reqPb.GetRpcHeader().GetProtocolVersion())
	req.ID = reqPb.GetRpcHeader().GetId()
	req.Addr = reqPb.GetRpcHeader().GetAddr()
	req.Term = reqPb.GetTerm()
	req.LastLogIndex = reqPb.GetLastLogIndex()
	req.LastLogTerm = reqPb.GetLastLogTerm()
}

func protoToRequestPreVoteResponse(respPb *pb.RequestPreVoteResponse, resp *raft.RequestPreVoteResponse) {
	resp.ProtocolVersion = raft.ProtocolVersion(respPb.GetRpcHeader().GetProtocolVersion())
	resp.ID = respPb.GetRpcHeader().GetId()
	resp.Addr = respPb.GetRpcHeader().GetAddr()
	resp.Term = respPb.GetTerm()
	resp.Granted = respPb.GetGranted()
}

func protoToInstallSnapshotRequest(reqPb *pb.InstallSnapshotRequest, req *raft.InstallSnapshotRequest) {
	req.ProtocolVersion = raft.ProtocolVersion(reqPb.GetRpcHeader().GetProtocolVersion())
	req.ID = reqPb.GetRpcHeader().GetId()
	req.Addr = reqPb.GetRpcHeader().GetAddr()
	req.SnapshotVersion = raft.SnapshotVersion(reqPb.GetSnapshotVersion())
	req.Term = reqPb.GetTerm()
	req.Leader = reqPb.GetLeader()
	req.LastLogIndex = reqPb.GetLastLogIndex()
	req.LastLogTerm = reqPb.GetLastLogTerm()
	req.Peers = reqPb.GetPeers()
	req.Configuration = reqPb.GetConfiguration()
	req.ConfigurationIndex = reqPb.GetConfigurationIndex()
	req.Size = reqPb.GetSize()
}

func protoToInstallSnapshotResponse(respPb *pb.InstallSnapshotResponse, resp *raft.InstallSnapshotResponse) {
	resp.ProtocolVersion = raft.ProtocolVersion(respPb.GetRpcHeader().GetProtocolVersion())
	resp.ID = respPb.GetRpcHeader().GetId()
	resp.Addr = respPb.GetRpcHeader().GetAddr()
	resp.Term = respPb.GetTerm()
	resp.Success = respPb.GetSuccess()
}

func protoToTimeoutNowRequest(reqPb *pb.TimeoutNowRequest, req *raft.TimeoutNowRequest) {
	req.ProtocolVersion = raft.ProtocolVersion(reqPb.GetRpcHeader().GetProtocolVersion())
	req.ID = reqPb.GetRpcHeader().GetId()
	req.Addr = reqPb.GetRpcHeader().GetAddr()
}

func protoToTimeoutNowResponse(respPb *pb.TimeoutNowResponse, resp *raft.TimeoutNowResponse) {
	resp.ProtocolVersion = raft.ProtocolVersion(respPb.GetRpcHeader().GetProtocolVersion())
	resp.ID = respPb.GetRpcHeader().GetId()
	resp.Addr = respPb.GetRpcHeader().GetAddr()
}

func protoToLog(pbLog *pb.Log, log *raft.Log) {
	log.Index = pbLog.GetIndex()
	log.Term = pbLog.GetTerm()
	log.Type = raft.LogType(pbLog.GetType())
	log.Data = pbLog.GetData()
	log.Extensions = pbLog.GetExtensions()
	log.AppendedAt = pbLog.AppendedAt.AsTime()
}
