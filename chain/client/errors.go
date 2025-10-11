package client

import (
	"context"

	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/chain/storage"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func ChainClientErrorInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err == nil {
			return nil
		}
		st, ok := status.FromError(err)
		if !ok {
			return err
		}

		for _, detail := range st.Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok {
				switch info.Reason {
				case "node.ErrSyncing":
					return node.ErrSyncing
				case "node.ErrNotHead":
					return node.ErrNotHead
				case "storage.ErrKeyNotFound":
					return storage.ErrKeyNotFound
				case "storage.ErrConflict":
					return storage.ErrConflict
				}
			}
		}

		return err
	}
}
