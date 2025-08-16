package chain

import (
	pb "github.com/jmsadair/zebraos/proto/pbchain"
	"github.com/jmsadair/zebraos/storage"
	"google.golang.org/grpc"
)

type keyValueRecieveStream struct {
	stream grpc.ClientStream
}

func (kvr *keyValueRecieveStream) Recieve() (*storage.KeyValuePair, error) {
	var msg *pb.KeyValuePair
	if err := kvr.stream.RecvMsg(msg); err != nil {
		return nil, err
	}
	return &storage.KeyValuePair{Key: msg.GetKey(), Value: msg.GetValue(), Version: msg.GetVersion(), Committed: msg.GetIsCommited()}, nil
}

type keyValueSendStream struct {
	stream grpc.ServerStream
}

func (kvs *keyValueSendStream) Send(kv *storage.KeyValuePair) error {
	msg := &pb.KeyValuePair{Key: kv.Key, Value: kv.Value, Version: kv.Version, IsCommited: kv.Committed}
	return kvs.stream.SendMsg(msg)
}
