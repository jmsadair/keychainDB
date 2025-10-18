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

	ErrSyncing                   = Error(ErrGRPCSyncing)
	ErrNotMemberOfChain          = Error(ErrGRPCNotMemberOfChain)
	ErrInvalidConfigVersion      = Error(ErrGRPCInvalidConfigVersion)
	ErrNotHead                   = Error(ErrGRPCNotHead)
	ErrUncommittedRead           = Error(ErrGRPCUncommittedRead)
	ErrKeyNotFound               = Error(ErrGRPCKeyNotFound)
	ErrEmptyKey                  = Error(ErrGRPCEmptyKey)
	ErrConflict                  = Error(ErrGRPCConflict)
	ErrNoMembers                 = Error(ErrGRPCNoMembers)
	ErrCoordinatorUnavailable    = Error(ErrGRPCCoordinatorUnavailable)
	ErrNodeExists                = Error(ErrGRPCNodeExists)
	ErrLeadershipLost            = Error(ErrGRPCLeadershipLost)
	ErrNotLeader                 = Error(ErrGRPCNotLeader)
	ErrLeader                    = Error(ErrGRPCLeader)
	ErrEnqueueTimeout            = Error(ErrGRPCEnqueueTimeout)
	ErrConfigurationUpdateFailed = Error(ErrGRPCConfigurationUpdateFailed)
	ErrNoLeader                  = Error(ErrGRPCNoLeader)

	errStringToErr = map[string]error{
		ErrorDesc(ErrGRPCSyncing):                   ErrGRPCSyncing,
		ErrorDesc(ErrGRPCNotMemberOfChain):          ErrGRPCNotMemberOfChain,
		ErrorDesc(ErrGRPCInvalidConfigVersion):      ErrGRPCInvalidConfigVersion,
		ErrorDesc(ErrGRPCNotHead):                   ErrGRPCNotHead,
		ErrorDesc(ErrGRPCUncommittedRead):           ErrGRPCUncommittedRead,
		ErrorDesc(ErrGRPCKeyNotFound):               ErrGRPCKeyNotFound,
		ErrorDesc(ErrGRPCEmptyKey):                  ErrGRPCEmptyKey,
		ErrorDesc(ErrGRPCConflict):                  ErrGRPCConflict,
		ErrorDesc(ErrGRPCNoMembers):                 ErrGRPCNoMembers,
		ErrorDesc(ErrGRPCCoordinatorUnavailable):    ErrGRPCCoordinatorUnavailable,
		ErrorDesc(ErrGRPCNodeExists):                ErrGRPCNodeExists,
		ErrorDesc(ErrGRPCLeadershipLost):            ErrGRPCLeadershipLost,
		ErrorDesc(ErrGRPCNotLeader):                 ErrGRPCNotLeader,
		ErrorDesc(ErrGRPCEnqueueTimeout):            ErrGRPCEnqueueTimeout,
		ErrorDesc(ErrGRPCConfigurationUpdateFailed): ErrGRPCConfigurationUpdateFailed,
		ErrorDesc(ErrGRPCNoLeader):                  ErrGRPCNoLeader,
	}
)

type KeychainError struct {
	code codes.Code
	desc string
}

func (e KeychainError) Code() codes.Code {
	return e.code
}

func (e KeychainError) Error() string {
	return e.desc
}

func ErrorDesc(err error) string {
	if s, ok := status.FromError(err); ok {
		return s.Message()
	}
	return err.Error()
}

func Error(err error) error {
	if err == nil {
		return nil
	}

	verr, ok := errStringToErr[ErrorDesc(err)]
	if !ok {
		return err
	}

	ev, ok := status.FromError(verr)
	var desc string
	if ok {
		desc = ev.Message()
	} else {
		desc = verr.Error()
	}

	return KeychainError{code: ev.Code(), desc: desc}
}
