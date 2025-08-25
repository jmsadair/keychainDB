package storage

import (
	"encoding/binary"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	pb "github.com/jmsadair/keychain/proto/pbcoordinator"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func logToBytes(log *raft.Log) ([]byte, error) {
	protoLog := &pb.Log{
		Index:      log.Index,
		Term:       log.Term,
		Type:       uint32(log.Type),
		Data:       log.Data,
		Extensions: log.Extensions,
		AppendedAt: timestamppb.New(log.AppendedAt),
	}
	return proto.Marshal(protoLog)
}

func bytesToLog(b []byte, log *raft.Log) error {
	protoLog := &pb.Log{}
	if err := proto.Unmarshal(b, protoLog); err != nil {
		return err
	}

	log.Index = protoLog.Index
	log.Term = protoLog.Term
	log.Type = raft.LogType(protoLog.Type)
	log.Data = protoLog.Data
	log.Extensions = protoLog.Extensions
	log.AppendedAt = protoLog.AppendedAt.AsTime()

	return nil
}

type PersistentLog struct {
	db *badger.DB
}

func NewPersistentLog(dbpath string) (*PersistentLog, error) {
	db, err := badger.Open(badger.DefaultOptions(dbpath))
	if err != nil {
		return nil, err
	}
	return &PersistentLog{db: db}, nil
}

func (pl *PersistentLog) FirstIndex() (uint64, error) {
	var first uint64
	firstPtr := &first

	err := pl.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Rewind()
		if it.Valid() {
			*firstPtr = binary.BigEndian.Uint64(it.Item().Key())
		}
		return nil
	})

	return *firstPtr, err
}

func (pl *PersistentLog) LastIndex() (uint64, error) {
	var last uint64
	lastPtr := &last

	err := pl.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		it.Rewind()
		if it.Valid() {
			*lastPtr = binary.BigEndian.Uint64(it.Item().Key())
		}

		return nil
	})

	return *lastPtr, err
}

func (pl *PersistentLog) GetLog(index uint64, log *raft.Log) error {
	return pl.db.View(func(txn *badger.Txn) error {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, index)
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return bytesToLog(value, log)
	})
}

func (pl *PersistentLog) StoreLog(log *raft.Log) error {
	return pl.StoreLogs([]*raft.Log{log})
}

func (pl *PersistentLog) StoreLogs(logs []*raft.Log) error {
	return pl.db.Update(func(txn *badger.Txn) error {
		for _, log := range logs {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, log.Index)
			value, err := logToBytes(log)
			if err != nil {
				return err
			}
			txn.Set(key, value)
		}
		return nil
	})
}

func (pl *PersistentLog) DeleteRange(min uint64, max uint64) error {
	return pl.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		minKey := make([]byte, 8)
		binary.BigEndian.PutUint64(minKey, min)
		for it.Seek(minKey); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			if binary.BigEndian.Uint64(key) > max {
				break
			}
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		return nil
	})
}
