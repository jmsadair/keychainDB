package chain

import (
	"github.com/jmsadair/zebraos/internal/storage"
	pb "github.com/jmsadair/zebraos/proto/pbchain"
	"google.golang.org/grpc"
)

type KeyValueReciever struct {
	Stream grpc.ClientStream
}

func (kvr *KeyValueReciever) Recieve() (*storage.KeyValuePair, error) {
	var msg *pb.KeyValuePair
	if err := kvr.Stream.RecvMsg(msg); err != nil {
		return nil, err
	}
	return &storage.KeyValuePair{Key: msg.GetKey(), Value: msg.GetValue(), Version: msg.GetVersion(), Committed: msg.GetIsCommited()}, nil
}

type KeyValueSender struct {
	Stream grpc.ServerStream
}

func (kvs *KeyValueSender) Send(kv *storage.KeyValuePair) error {
	msg := &pb.KeyValuePair{Key: kv.Key, Value: kv.Value, Version: kv.Version, IsCommited: kv.Committed}
	return kvs.Stream.SendMsg(msg)
}
