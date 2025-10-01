package grpc

import (
	"errors"

	"github.com/jmsadair/keychain/chain/node"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errStringToErr = map[string]error{
	node.ErrNotHead.Error():              node.ErrNotHead,
	node.ErrNotMemberOfChain.Error():     node.ErrNotMemberOfChain,
	node.ErrSyncing.Error():              node.ErrSyncing,
	node.ErrInvalidConfigVersion.Error(): node.ErrInvalidConfigVersion,
	node.ErrKeyDoesNotExist.Error():      node.ErrKeyDoesNotExist,
}

func parseGrpcErr(err error) error {
	if err == nil {
		return nil
	}
	s, ok := status.FromError(err)
	if !ok {
		return err
	}
	parsed, ok := errStringToErr[s.Message()]
	if !ok {
		return err
	}
	return parsed
}

func gRPCError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, node.ErrKeyDoesNotExist):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, node.ErrSyncing):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, node.ErrNotMemberOfChain), errors.Is(err, node.ErrInvalidConfigVersion), errors.Is(err, node.ErrNotHead):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
