package storage

import (
	"context"
	"errors"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto/v2/z"
)

const (
	defaultStreamConcurrency = 25
	defaultLoggingPrefix     = "Storage.Streaming"
)

const (
	keyMetadataByte = 0
	committedMask   = 1
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
	var err error
	var version uint64

	err = ps.db.Update(func(txn *badger.Txn) error {
		version, err = write(txn, key, value, 0, false, true)
		return err
	})

	return version, err
}

// UncommittedWrite will transactionally write the provided version of key to storage but will not commit it.
func (ps *PersistantStorage) UncommittedWrite(key string, value []byte, version uint64) error {
	return ps.db.Update(func(txn *badger.Txn) error {
		_, err := write(txn, key, value, version, false, false)
		return err
	})
}

// CommittedWrite will transactionally write the provided version of key to storage and will immediately commit it.
func (ps *PersistantStorage) CommittedWrite(key string, value []byte, version uint64) error {
	return ps.db.Update(func(txn *badger.Txn) error {
		_, err := write(txn, key, value, version, true, false)
		return err
	})
}

// CommitVersion will transactionally commit the provided version of the key.
// All versions of the key earlier than the committed version will be deleted.
// Commiting a version earlier than the currently committed version is a no-op.
func (ps *PersistantStorage) CommitVersion(key string, version uint64) error {
	return ps.db.Update(func(txn *badger.Txn) error {
		return commit(txn, key, version)
	})
}

// CommittedRead will transactionally read the value of the key.
// If there are uncommitted writes, an error will be returned.
func (ps *PersistantStorage) CommittedRead(key string) ([]byte, error) {
	var err error
	var value []byte

	err = ps.db.View(func(txn *badger.Txn) error {
		value, err = read(txn, key)
		return err
	})

	return value, err
}

// PropagateKeys will concurrently iterate over a snapshot of the storage, pick up the key-value
// pairs in batches, and call the provided sendFunc function. If committedOnly is true, only committed
// versions of key-value pairs will be sent. Otherwise, only uncommitted versions will be sent. If at
// any point during this process an error occurs, the entire process is terminated and the error is returned.
func (ps *PersistantStorage) PropagateKeys(sendFunc func(map[string][]byte) error, committedOnly bool) error {
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

	chooseKey := func(item *badger.Item) bool {
		if committedOnly {
			return item.UserMeta()&committedMask == 1
		}
		return item.UserMeta()&committedMask != 1
	}

	stream := ps.db.NewStream()
	stream.Send = send
	stream.ChooseKey = chooseKey
	stream.LogPrefix = defaultLoggingPrefix
	stream.NumGo = defaultStreamConcurrency

	return stream.Orchestrate(context.Background())
}

// RecievePropagatedKeysCommitted accepts a map of key-value pairs and will perform a committed write
// for each key-value pair unless a newer version has already been committed.
func (ps *PersistantStorage) RecievePropagatedKeysCommitted(keyValuePairs map[string][]byte) error {
	return ps.db.Update(func(txn *badger.Txn) error {
		for key, value := range keyValuePairs {
			if err := recievePropagatedKeys(txn, key, value, true); err != nil {
				return err
			}
		}
		return nil
	})
}

// RecievePropagatedKeysUncommitted accepts a map of key-value pairs and will perform a uncommitted write
// for each key-value pair unless a newer version has already been committed or the version already exists.
func (ps *PersistantStorage) RecievePropagatedKeysUncommitted(keyValuePairs map[string][]byte) error {
	return ps.db.Update(func(txn *badger.Txn) error {
		for key, value := range keyValuePairs {
			if err := recievePropagatedKeys(txn, key, value, false); err != nil {
				return err
			}
		}
		return nil
	})
}

func recievePropagatedKeys(txn *badger.Txn, key string, value []byte, shouldCommit bool) error {
	internalKey, err := NewInternalKeyFromBytes([]byte(key))
	if err != nil {
		return err
	}

	// Key metadata should not be backfilled.
	// As keys are backfilled, the metadata will be correctly created and updated.
	// This is intentional to avoid having to deal with conflicts in the metadata.
	if internalKey.IsMetadataKey {
		return nil
	}

	// Write the object to disk unless:
	// 1. This version or a newer version of the object has already been committed.
	// 2. The version of the object exists and has not been committed and committing
	//    the object has not been requested.
	existingObjectMetadata, err := getOrCreateObjectMetadata(txn, internalKey.Key)
	if err != nil {
		return err
	}
	if existingObjectMetadata.LastCommitted >= internalKey.Version {
		return nil
	} else if shouldCommit && existingObjectMetadata.HasVersion(internalKey.Version) {
		return commit(txn, key, internalKey.Version)
	} else if existingObjectMetadata.HasVersion(internalKey.Version) {
		return nil
	}
	_, err = write(txn, internalKey.Key, value, internalKey.Version, shouldCommit, false)
	return err
}

func commit(txn *badger.Txn, key string, version uint64) error {
	objectMetadata, err := getOrCreateObjectMetadata(txn, key)
	if err != nil {
		return err
	}

	// Remove all versions of the object older than the version being committed.
	// Update the metadata on the committed version of the key to reflect that it is committed.
	for _, oldVersion := range objectMetadata.Versions {
		dataKey, err := createDataKey(key, version)
		if err != nil {
			return err
		}
		if oldVersion == version {
			item, err := txn.Get(dataKey)
			if err != nil {
				return err
			}
			dataCopy, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			e := badger.NewEntry(dataKey, dataCopy).WithMeta(keyMetadataByte | committedMask)
			if err := txn.SetEntry(e); err != nil {
				return err
			}
			break
		}
		if err := txn.Delete(dataKey); err != nil {
			return err
		}
	}

	// Update the metadata for the object to reflect the latest committed version.
	if err := objectMetadata.CommitVersion(version); err != nil {
		return err
	}
	objectMetadataKey, err := createObjectMetadataKey(key)
	if err != nil {
		return err
	}
	b, err := objectMetadata.Bytes()
	if err != nil {
		return err
	}
	if err := txn.Set(objectMetadataKey, b); err != nil {
		return err
	}

	return nil
}

func read(txn *badger.Txn, key string) ([]byte, error) {
	objectMetadata, err := getOrCreateObjectMetadata(txn, key)
	if err != nil {
		return nil, err
	}
	if objectMetadata.IsDirty() {
		return nil, ErrDirtyRead
	}

	dataKey, err := createDataKey(key, objectMetadata.LastCommitted)
	if err != nil {
		return nil, err
	}

	item, err := txn.Get(dataKey)
	if err != nil {
		return nil, err
	}
	value, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func write(txn *badger.Txn, key string, value []byte, version uint64, shouldCommit bool, newVersion bool) (uint64, error) {
	objectMetadata, err := getOrCreateObjectMetadata(txn, key)
	if err != nil {
		return 0, err
	}

	if newVersion {
		version = objectMetadata.NextVersion()
	}
	if err := objectMetadata.AddVersion(version); err != nil {
		return 0, err
	}

	objectMetadataBytes, err := objectMetadata.Bytes()
	if err != nil {
		return 0, err
	}
	objectMetadataKey, err := createObjectMetadataKey(key)
	if err != nil {
		return 0, err
	}
	if err := txn.Set(objectMetadataKey, objectMetadataBytes); err != nil {
		return 0, err
	}

	dataKey, err := createDataKey(key, version)
	if err != nil {
		return 0, err
	}
	if err = txn.Set(dataKey, value); err != nil {
		return 0, err
	}

	if shouldCommit {
		return version, commit(txn, key, version)
	}

	return version, nil
}

func createObjectMetadataKey(key string) ([]byte, error) {
	objectMetadataKey := NewInternalKey(key, true, 0)
	return objectMetadataKey.Bytes()
}

func createDataKey(key string, version uint64) ([]byte, error) {
	dataKey := NewInternalKey(key, false, version)
	return dataKey.Bytes()
}

func getOrCreateObjectMetadata(txn *badger.Txn, key string) (*ObjectMetadata, error) {
	objectMetadataKey, err := createObjectMetadataKey(key)
	if err != nil {
		return nil, err
	}

	var objectMetadata *ObjectMetadata
	item, err := txn.Get(objectMetadataKey)
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		objectMetadata = NewObjectMetadata(key)
	case err != nil:
		return nil, err
	default:
		objectMetadataBytes, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		objectMetadata, err = NewObjectMetadataFromBytes(objectMetadataBytes)
		if err != nil {
			return nil, err
		}
	}

	return objectMetadata, nil
}
