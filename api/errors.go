package api

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrGRPCSyncing                = status.Error(codes.Unavailable, "chainserver: cannot serve reads while syncing")
	ErrGRPCNotMemberOfChain       = status.Error(codes.Unavailable, "chainserver: not a member of a chain")
	ErrGRPCInvalidConfigVersion   = status.Error(codes.Aborted, "chainserver: configuration version mismatch")
	ErrGRPCNotHead                = status.Error(codes.Unavailable, "chainserver: cannot serve writes if not head")
	ErrGRPCUncommittedRead        = status.Error(codes.Aborted, "storage: latest version is uncommitted")
	ErrGRPCKeyNotFound            = status.Error(codes.NotFound, "storage: key does not exist")
	ErrGRPCEmptyKey               = status.Error(codes.InvalidArgument, "storage: key is empty")
	ErrGRPCConflict               = status.Error(codes.Aborted, "storage: conflicting transactions")
	ErrGRPCNoMembers              = status.Error(codes.FailedPrecondition, "proxyserver: chain has no members")
	ErrGRPCCoordinatorUnavailable = status.Error(codes.Unavailable, "proxyserver: failed to contact coordinator")
)
