package storage

import (
	"encoding/binary"
	"errors"

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

func newLogKey(index uint64) []byte {
	b := make([]byte, 9)
	b[0] = byte(logEntry)
	binary.BigEndian.PutUint64(b[1:], index)
	return b
}

func newConfigurationKey(key []byte) []byte {
	b := make([]byte, 0, len(key)+1)
	b = append(b, byte(configuration))
	return append(b, key...)
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

// Close closes the storage.
func (ps *PersistentStorage) Close() error {
	return ps.db.Close()
}

// FirstIndex returns the index of the first log entry. If there are no log entries, then zero is returned.
func (ps *PersistentStorage) FirstIndex() (uint64, error) {
	var first uint64
	firstPtr := &first

	err := ps.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		minLogKey := newLogKey(uint64(0))
		it.Seek(minLogKey)
		if it.ValidForPrefix([]byte{byte(logEntry)}) {
			*firstPtr = uint64FromKey(it.Item().Key())
		}

		return nil
	})

	return *firstPtr, err
}

// LastIndex returns the index of the last log entry. If there are no log entries, then zero is returned.
func (ps *PersistentStorage) LastIndex() (uint64, error) {
	var last uint64
	lastPtr := &last

	err := ps.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		maxLogKey := newLogKey(^uint64(0))
		it.Seek(maxLogKey)
		if it.ValidForPrefix([]byte{byte(logEntry)}) {
			*lastPtr = uint64FromKey(it.Item().Key())
		}

		return nil
	})

	return *lastPtr, err
}

// GetLog gets the log entry at the provided index.
func (ps *PersistentStorage) GetLog(index uint64, log *raft.Log) error {
	return ps.db.View(func(txn *badger.Txn) error {
		logKey := newLogKey(index)
		item, err := txn.Get(logKey)
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
func (ps *PersistentStorage) StoreLog(log *raft.Log) error {
	return ps.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multipse log entries.
func (ps *PersistentStorage) StoreLogs(logs []*raft.Log) error {
	return ps.db.Update(func(txn *badger.Txn) error {
		for _, log := range logs {
			logKey := newLogKey(log.Index)
			value, err := logToBytes(log)
			if err != nil {
				return err
			}
			txn.Set(logKey, value)
		}
		return nil
	})
}

// DeleteRange deletes all log entries with an index in the provided range inclusive.
func (ps *PersistentStorage) DeleteRange(min uint64, max uint64) error {
	return ps.db.Update(func(txn *badger.Txn) error {
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

// Set sets the value associated with the provided key.
func (ps *PersistentStorage) Set(key []byte, value []byte) error {
	return ps.db.Update(func(txn *badger.Txn) error {
		configurationKey := newConfigurationKey(key)
		return txn.Set(configurationKey, value)
	})
}

// Get returns the value associated with the provided key or an empty byte slice if the key does not exist.
func (ps *PersistentStorage) Get(key []byte) ([]byte, error) {
	value := []byte{}
	err := ps.db.View(func(txn *badger.Txn) error {
		configurationKey := newConfigurationKey(key)
		item, err := txn.Get(configurationKey)
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return value, err
}

// SetUint64 sets the value associated with the provided key.
func (ps *PersistentStorage) SetUint64(key []byte, value uint64) error {
	return ps.db.Update(func(txn *badger.Txn) error {
		configurationKey := newConfigurationKey(key)
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, value)
		return txn.Set(configurationKey, b)
	})
}

// GetUint64 returns the value associated with the provided key or zero if the key does not exist.
func (ps *PersistentStorage) GetUint64(key []byte) (uint64, error) {
	var value uint64
	valuePtr := &value

	err := ps.db.View(func(txn *badger.Txn) error {
		configurationKey := newConfigurationKey(key)
		item, err := txn.Get(configurationKey)
		if err != nil {
			return nil
		}
		b, err := item.ValueCopy(nil)
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		*valuePtr = binary.BigEndian.Uint64(b)
		return nil
	})

	return value, err
}
