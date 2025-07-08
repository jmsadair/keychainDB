package storage

import (
	"context"
	"errors"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto/v2/z"
)

const (
	// Default umber of go routines used to concurrently stream the key-value pairs from storage.
	defaultStreamConcurrency = 25
	// Default logging prefix using by the storage.
	defaultLoggingPrefix = "Storage.Streaming"
)

// ErrDirtyRead is returned when a committed read is performed
// and there are one or more uncommitted writes.
var ErrDirtyRead = errors.New("latest object version is dirty")

// PersistantStorage is a disk-backed key-value storage system that is
// capable of performing transactional reads and writes.
type PersistantStorage struct {
	db *badger.DB
}

// NewPersistantStorage opens the storage located at the provided path.
// If the storage does not exist, one will be created.
func NewPersistantStorage(dbpath string) (*PersistantStorage, error) {
	db, err := badger.Open(badger.DefaultOptions(dbpath))
	if err != nil {
		return nil, err
	}
	return &PersistantStorage{db: db}, nil
}

// Close will close the storage.
// It is critical that this is called after the storage is done being used to ensure all updates are written to disk.
func (ps *PersistantStorage) Close() {
	ps.db.Close()
}

// UncommittedWriteNewVersion will transactionally generate a new version for the key and write the key to storage
// but will not commit it.
func (ps *PersistantStorage) UncommittedWriteNewVersion(key string, value []byte) (uint64, error) {
	return ps.write(key, value, 0, false, true)
}

// UncommittedWrite will transactionally write the provided version of key to storage but will not commit it.
func (ps *PersistantStorage) UncommittedWrite(key string, value []byte, version uint64) error {
	_, err := ps.write(key, value, version, false, false)
	return err
}

// CommittedWrite will transactionally write the provided version of key to storage and will immediately commit it.
func (ps *PersistantStorage) CommittedWrite(key string, value []byte, version uint64) error {
	_, err := ps.write(key, value, version, true, false)
	return err
}

// CommitVersion will transactionally commit the provided version of the key.
// All versions of the key earlier than the committed version will be deleted.
// Commiting a version earlier than the currently committed version is a no-op.
func (ps *PersistantStorage) CommitVersion(key string, version uint64) error {
	err := ps.db.Update(func(txn *badger.Txn) error {
		metadataKey := NewInternalKey(key, true, 0)
		metadataKeyBytes, err := metadataKey.Bytes()
		if err != nil {
			return err
		}

		item, err := txn.Get(metadataKeyBytes)
		if err != nil {
			return err
		}
		keyMetadataBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyMetadata, err := NewKeyMetadataFromBytes(keyMetadataBytes)
		if err != nil {
			return err
		}

		if err := keyMetadata.CommitVersion(version); err != nil {
			return err
		}
		keyMetadataBytes, err = keyMetadata.Bytes()
		if err != nil {
			return err
		}
		if err := txn.Set(metadataKeyBytes, keyMetadataBytes); err != nil {
			return err
		}

		return nil
	})

	return err
}

// CommittedRead will transactionally read the value of the key.
// If there are uncommitted writes, an error will be returned.
func (ps *PersistantStorage) CommittedRead(key string) ([]byte, error) {
	var value []byte

	err := ps.db.View(func(txn *badger.Txn) error {
		metadataKey := NewInternalKey(key, true, 0)
		metadataKeyBytes, err := metadataKey.Bytes()
		if err != nil {
			return err
		}

		item, err := txn.Get(metadataKeyBytes)
		if err != nil {
			return err
		}
		keyMetadataBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		keyMetadata, err := NewKeyMetadataFromBytes(keyMetadataBytes)
		if err != nil {
			return err
		}

		if keyMetadata.IsDirty() {
			return ErrDirtyRead
		}

		dataKey := NewInternalKey(key, false, keyMetadata.LastCommitted)
		dataKeyBytes, err := dataKey.Bytes()
		if err != nil {
			return err
		}
		item, err = txn.Get(dataKeyBytes)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}

		return nil
	})

	return value, err
}

// Stream will concurrently iterate over a snapshot of the storage, pick up the key-value
// pairs in batches, and call the provided sendFunc function. If at any point during this
// process an error occurs, the entire process is terminated and the error is returned.
func (ps *PersistantStorage) Stream(sendFunc func(map[string][]byte) error) error {
	send := func(buf *z.Buffer) error {
		kvList, err := badger.BufferToKVList(buf)
		if err != nil {
			return err
		}
		kvMap := make(map[string][]byte, len(kvList.GetKv()))
		for _, kv := range kvList.GetKv() {
			kvMap[string(kv.GetKey())] = kv.GetValue()
		}
		return sendFunc(kvMap)
	}

	stream := ps.db.NewStream()
	stream.Send = send
	stream.LogPrefix = defaultLoggingPrefix
	stream.NumGo = defaultStreamConcurrency

	return stream.Orchestrate(context.Background())
}

func (ps *PersistantStorage) write(key string, value []byte, version uint64, commit bool, newVersion bool) (uint64, error) {
	err := ps.db.Update(func(txn *badger.Txn) error {
		metadataKey := NewInternalKey(key, true, 0)
		metadataKeyBytes, err := metadataKey.Bytes()
		if err != nil {
			return err
		}

		var keyMetadata *KeyMetadata
		item, err := txn.Get(metadataKeyBytes)
		switch {
		case errors.Is(err, badger.ErrKeyNotFound):
			keyMetadata = NewKeyMetadata(key)
		case err != nil:
			return err
		default:
			keyMetadataBytes, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyMetadata, err = NewKeyMetadataFromBytes(keyMetadataBytes)
			if err != nil {
				return err
			}
		}

		if newVersion {
			version = keyMetadata.IncrementVersion()
		} else if err := keyMetadata.AddVersion(version); err != nil {
			return err
		}
		if commit {
			if err := keyMetadata.CommitVersion(version); err != nil {
				return err
			}
		}

		dataKey := NewInternalKey(key, false, version)
		dataKeyBytes, err := dataKey.Bytes()
		if err != nil {
			return err
		}
		if err = txn.Set(dataKeyBytes, value); err != nil {
			return err
		}

		keyMetadataBytes, err := keyMetadata.Bytes()
		if err != nil {
			return err
		}
		if err := txn.Set(metadataKeyBytes, keyMetadataBytes); err != nil {
			return err
		}

		return nil
	})

	return version, err
}
