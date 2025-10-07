package client

import (
	"github.com/jmsadair/keychain/chain/node"
	"google.golang.org/grpc/status"
)

var errStringToErr = map[string]error{
	node.ErrNotHead.Error():              node.ErrNotHead,
	node.ErrNotMemberOfChain.Error():     node.ErrNotMemberOfChain,
	node.ErrSyncing.Error():              node.ErrSyncing,
	node.ErrInvalidConfigVersion.Error(): node.ErrInvalidConfigVersion,
	node.ErrKeyDoesNotExist.Error():      node.ErrKeyDoesNotExist,
	node.ErrNotCommitted.Error():         node.ErrNotCommitted,
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
