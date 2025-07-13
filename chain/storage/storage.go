package storage

import (
	"context"
	"errors"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto/v2/z"
)

// ErrDirtyRead is returned when a committed read is performed and there are one or more uncommitted writes.
var ErrDirtyRead = errors.New("latest object version is dirty")

// KeyFilter is a filter for selecting specific keys when listing keys from storage.
type KeyFilter int

const (
	// AllKeys will select all keys.
	AllKeys KeyFilter = iota
	// DirtyKeys will select only dirty keys.
	DirtyKeys
	// CommittedKeys will select only uncommitted keys.
	CommittedKeys
)

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

// UncommittedWriteNewVersion will transactionally generate a new version of the key and write the key-value pair to storage.
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

// CommittedWriteNewVersion will transactionally generate a new version of the key, write the key-value pair to storage,
// and immediately commit the new version.
func (ps *PersistantStorage) CommittedWriteNewVersion(key string, value []byte) (uint64, error) {
	var err error
	var version uint64

	ps.db.Update(func(txn *badger.Txn) error {
		version, err = write(txn, key, value, version, true, true)
		return err
	})

	return version, err
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

// SendKeys will iterate over all keys in storage, group the keys that satisfy the provided keyFilter into
// batches, and call the provided sendFunc with the batch as the argument. If at any point during the process
// an error occurs, the process will be terminated and the error will be returned.
func (ps *PersistantStorage) SendKeys(sendFunc func([]string) error, keyFilter KeyFilter) error {
	stream := ps.db.NewStream()

	stream.Send = func(buf *z.Buffer) error {
		kvList, err := badger.BufferToKVList(buf)
		if err != nil {
			return err
		}

		var keys []string
		for _, kv := range kvList.GetKv() {
			value := kv.GetValue()
			metadata, err := NewObjectMetadataFromBytes(value)
			if err != nil {
				return err
			}

			shouldSend := false
			switch keyFilter {
			case AllKeys:
				shouldSend = true
			case DirtyKeys:
				shouldSend = metadata.IsDirty()
			case CommittedKeys:
				shouldSend = !metadata.IsDirty()
			}

			if shouldSend {
				keys = append(keys, Key(kv.GetKey()).ClientKey())
			}
		}

		return sendFunc(keys)
	}
	stream.ChooseKey = func(item *badger.Item) bool {
		return Key(item.Key()).IsMetadata()
	}

	return stream.Orchestrate(context.Background())
}

func commit(txn *badger.Txn, key string, version uint64) error {
	md, err := getOrCreateMetadata(txn, key)
	if err != nil {
		return err
	}

	// Delete all versions of the key that are dirty up to and including the version being committed.
	// Update the value of the committed key to be the value of the version that was just committed.
	for _, oldVersion := range md.Versions {
		if oldVersion > version {
			break
		}
		dirtyKey := NewDirtyKey(key, oldVersion)
		if oldVersion == version {
			item, err := txn.Get(dirtyKey)
			if err != nil {
				return err
			}
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			committedKey := NewCommittedKey(key)
			if err := txn.Set(committedKey, value); err != nil {
				return err
			}
		}
		if err := txn.Delete(dirtyKey); err != nil {
			return err
		}
	}

	// Update the metadata to reflect the latest committed version.
	if err := md.CommitVersion(version); err != nil {
		return err
	}
	mdKey := NewMetadataKey(key)
	mdBytes, err := md.Bytes()
	if err != nil {
		return err
	}
	if err := txn.Set(mdKey, mdBytes); err != nil {
		return err
	}

	return nil
}

func read(txn *badger.Txn, key string) ([]byte, error) {
	md, err := getOrCreateMetadata(txn, key)
	if err != nil {
		return nil, err
	}
	if md.IsDirty() {
		return nil, ErrDirtyRead
	}

	committedKey := NewCommittedKey(key)
	item, err := txn.Get(committedKey)
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
	md, err := getOrCreateMetadata(txn, key)
	if err != nil {
		return 0, err
	}

	if newVersion {
		version = md.NextVersion()
	}
	if err := md.AddVersion(version); err != nil {
		return 0, err
	}

	mdBytes, err := md.Bytes()
	if err != nil {
		return 0, err
	}
	mdKey := NewMetadataKey(key)
	if err := txn.Set(mdKey, mdBytes); err != nil {
		return 0, err
	}

	dirtyKey := NewDirtyKey(key, version)
	if err = txn.Set(dirtyKey, value); err != nil {
		return 0, err
	}

	if shouldCommit {
		return version, commit(txn, key, version)
	}

	return version, nil
}

func getOrCreateMetadata(txn *badger.Txn, key string) (*ObjectMetadata, error) {
	mdKey := NewMetadataKey(key)
	item, err := txn.Get(mdKey)

	var md *ObjectMetadata
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		md = NewObjectMetadata(key)
	case err != nil:
		return nil, err
	default:
		mdBytes, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		md, err = NewObjectMetadataFromBytes(mdBytes)
		if err != nil {
			return nil, err
		}
	}

	return md, nil
}
