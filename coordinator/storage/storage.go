package storage

import (
	"encoding/binary"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	pb "github.com/jmsadair/keychain/proto/pbcoordinator"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type keyType uint8

const (
	configuration keyType = iota
	logEntry
)

func newLogKey(key uint64) []byte {
	b := make([]byte, 9)
	b[0] = byte(logEntry)
	binary.BigEndian.PutUint64(b[1:], key)
	return b
}

func newConfigurationKey(key uint64) []byte {
	b := make([]byte, 9)
	b[0] = byte(configuration)
	binary.BigEndian.PutUint64(b[1:], key)
	return b
}

func uint64FromKey(b []byte) uint64 {
	return binary.BigEndian.Uint64(b[1:])
}

func logToBytes(log *raft.Log) ([]byte, error) {
	protoLog := &pb.Log{
		Index:      log.Index,
		Term:       log.Term,
		Type:       uint32(log.Type),
		Data:       log.Data,
		Extensions: log.Extensions,
		AppendedAt: timestamppb.New(log.AppendedAt.UTC()),
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

// PersistentStorage is a persistent storage system for raft log entries and metadata.
type PersistentStorage struct {
	db *badger.DB
}

// NewPersistentStorage creates a new log at the provided path.
func NewPersistentStorage(dbpath string) (*PersistentStorage, error) {
	db, err := badger.Open(badger.DefaultOptions(dbpath))
	if err != nil {
		return nil, err
	}
	return &PersistentStorage{db: db}, nil
}

// FirstIndex returns the index of the first log entry. If there are no log entries, then zero is returned.
func (pl *PersistentStorage) FirstIndex() (uint64, error) {
	var first uint64
	firstPtr := &first

	err := pl.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		minKey := newLogKey(uint64(0))
		it.Seek(minKey)
		if it.ValidForPrefix([]byte{byte(logEntry)}) {
			*firstPtr = uint64FromKey(it.Item().Key())
		}

		return nil
	})

	return *firstPtr, err
}

// LastIndex returns the index of the last log entry. If there are no log entries, then zero is returned.
func (pl *PersistentStorage) LastIndex() (uint64, error) {
	var last uint64
	lastPtr := &last

	err := pl.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		maxKey := newLogKey(^uint64(0))
		it.Seek(maxKey)
		if it.ValidForPrefix([]byte{byte(logEntry)}) {
			*lastPtr = uint64FromKey(it.Item().Key())
		}

		return nil
	})

	return *lastPtr, err
}

// GetLog gets the log entry at the provided index.
func (pl *PersistentStorage) GetLog(index uint64, log *raft.Log) error {
	return pl.db.View(func(txn *badger.Txn) error {
		key := newLogKey(index)
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

// StoreLog stores the provided log entry in the log.
func (pl *PersistentStorage) StoreLog(log *raft.Log) error {
	return pl.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
func (pl *PersistentStorage) StoreLogs(logs []*raft.Log) error {
	return pl.db.Update(func(txn *badger.Txn) error {
		for _, log := range logs {
			key := newLogKey(log.Index)
			value, err := logToBytes(log)
			if err != nil {
				return err
			}
			txn.Set(key, value)
		}
		return nil
	})
}

// DeleteRange deletes all log entries with an index in the provided range inclusive.
func (pl *PersistentStorage) DeleteRange(min uint64, max uint64) error {
	return pl.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		minKey := newLogKey(min)
		for it.Seek(minKey); it.ValidForPrefix([]byte{byte(logEntry)}); it.Next() {
			key := it.Item().KeyCopy(nil)
			index := uint64FromKey(key)
			if index > max {
				break
			}
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		return nil
	})
}
