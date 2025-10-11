package types

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrGRPCSyncing                   = status.Error(codes.Unavailable, "chainserver: cannot serve reads while syncing")
	ErrGRPCNotMemberOfChain          = status.Error(codes.Unavailable, "chainserver: not a member of a chain")
	ErrGRPCInvalidConfigVersion      = status.Error(codes.Aborted, "chainserver: configuration version mismatch")
	ErrGRPCNotHead                   = status.Error(codes.Unavailable, "chainserver: cannot serve writes if not head")
	ErrGRPCUncommittedRead           = status.Error(codes.Aborted, "storage: latest version is uncommitted")
	ErrGRPCKeyNotFound               = status.Error(codes.NotFound, "storage: key does not exist")
	ErrGRPCEmptyKey                  = status.Error(codes.InvalidArgument, "storage: key is empty")
	ErrGRPCConflict                  = status.Error(codes.Aborted, "storage: conflicting transactions")
	ErrGRPCNoMembers                 = status.Error(codes.FailedPrecondition, "proxyserver: chain has no members")
	ErrGRPCCoordinatorUnavailable    = status.Error(codes.Unavailable, "proxyserver: failed to contact coordinator")
	ErrGRPCNodeExists                = status.Error(codes.FailedPrecondition, "raft: node already exists with same ID or address")
	ErrGRPCLeadershipLost            = status.Error(codes.Unavailable, "raft: node lost leadership")
	ErrGRPCNotLeader                 = status.Error(codes.Unavailable, "raft: node is not the leader")
	ErrGRPCLeader                    = status.Error(codes.FailedPrecondition, "raft: node is the leader")
	ErrGRPCEnqueueTimeout            = status.Error(codes.Unavailable, "raft: enqueue timed out")
	ErrGRPCConfigurationUpdateFailed = status.Error(codes.Unavailable, "coordinator: failed to update configuration for chain member")
	ErrGRPCNoLeader                  = status.Error(codes.Unavailable, "coordinator: no leader elected")
)
