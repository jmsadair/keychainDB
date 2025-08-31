package transport

import (
	"github.com/jmsadair/keychain/chain/storage"
	"google.golang.org/grpc"

	pb "github.com/jmsadair/keychain/proto/pbchain"
)

// KeyValueReceiveStream wraps a gRPC client stream for receiving key-value pairs.
type KeyValueReceiveStream struct {
	Stream grpc.ClientStream
}

// Receive reads the next key-value pair from the stream.
func (kvr *KeyValueReceiveStream) Receive() (*storage.KeyValuePair, error) {
	var msg *pb.KeyValuePair
	if err := kvr.Stream.RecvMsg(msg); err != nil {
		return nil, err
	}
	return &storage.KeyValuePair{Key: msg.GetKey(), Value: msg.GetValue(), Version: msg.GetVersion(), Committed: msg.GetIsCommitted()}, nil
}

// KeyValueSendStream wraps a gRPC server stream for sending key-value pairs.
type KeyValueSendStream struct {
	Stream grpc.ServerStream
}

// Send writes a key-value pair to the stream.
func (kvs *KeyValueSendStream) Send(kv *storage.KeyValuePair) error {
	msg := &pb.KeyValuePair{Key: kv.Key, Value: kv.Value, Version: kv.Version, IsCommitted: kv.Committed}
	return kvs.Stream.SendMsg(msg)
}
