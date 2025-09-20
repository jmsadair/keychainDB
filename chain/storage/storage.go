package storage

import (
	"context"
	"errors"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/ristretto/v2/z"
)

var (
	// ErrDirtyRead is returned when a committed read is performed and there are one or more uncommitted writes.
	ErrDirtyRead = errors.New("latest object version is dirty")
	// ErrKeyDoesNotExist is returned when there is an attempt to read a non-existent key.
	ErrKeyDoesNotExist = errors.New("key does not exist")
)

// KeyFilter is a filter for selecting specific keys when listing keys from storage.
type KeyFilter int

const (
	// AllKeys will select all keys.
	AllKeys KeyFilter = iota
	// DirtyKeys will select only dirty keys.
	DirtyKeys
	// CommittedKeys will select only committed keys.
	CommittedKeys
)

// KeyValuePair represents a versioned key-value pair in storage.
type KeyValuePair struct {
	// Key is the client-provided key.
	Key string
	// Value is the data associated with the key.
	Value []byte
	// Version is the version number of this key-value pair.
	Version uint64
	// Committed indicates whether this version has been committed.
	Committed bool
}

// PersistentStorage is a disk-backed key-value storage system that is
// capable of performing transactional reads and writes.
type PersistentStorage struct {
	db *badger.DB
}

// NewPersistentStorage opens the storage located at the provided path.
// If the storage does not exist, one will be created.
func NewPersistentStorage(dbpath string) (*PersistentStorage, error) {
	db, err := badger.Open(badger.DefaultOptions(dbpath))
	if err != nil {
		return nil, err
	}
	return &PersistentStorage{db: db}, nil
}

// Close will close the storage.
// It is critical that this is called after the storage is done being used to ensure all updates are written to disk.
func (ps *PersistentStorage) Close() {
	ps.db.Close()
}

// UncommittedWriteNewVersion will transactionally generate a new version of the key and write the key-value pair to storage.
func (ps *PersistentStorage) UncommittedWriteNewVersion(key string, value []byte) (uint64, error) {
	var err error
	var version uint64

	err = ps.db.Update(func(txn *badger.Txn) error {
		version, err = write(txn, key, value, 0, false, true)
		return err
	})

	return version, err
}

// UncommittedWrite will transactionally write the provided version of key to storage but will not commit it.
func (ps *PersistentStorage) UncommittedWrite(key string, value []byte, version uint64) error {
	return ps.db.Update(func(txn *badger.Txn) error {
		_, err := write(txn, key, value, version, false, false)
		return err
	})
}

// CommittedWrite will transactionally write the provided version of key to storage and will immediately commit it.
func (ps *PersistentStorage) CommittedWrite(key string, value []byte, version uint64) error {
	return ps.db.Update(func(txn *badger.Txn) error {
		_, err := write(txn, key, value, version, true, false)
		return err
	})
}

// CommittedWriteNewVersion will transactionally generate a new version of the key, write the key-value pair to storage,
// and immediately commit the new version.
func (ps *PersistentStorage) CommittedWriteNewVersion(key string, value []byte) (uint64, error) {
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
func (ps *PersistentStorage) CommitVersion(key string, version uint64) error {
	return ps.db.Update(func(txn *badger.Txn) error {
		return commit(txn, key, version)
	})
}

// CommittedRead will transactionally read the value of the key.
// If there are uncommitted writes, an error will be returned.
func (ps *PersistentStorage) CommittedRead(key string) ([]byte, error) {
	var err error
	var value []byte

	err = ps.db.View(func(txn *badger.Txn) error {
		value, err = read(txn, key)
		return err
	})

	return value, err
}

// SendKeyValuePairs will iterate over the key-value pairs in storage that satisfy the provided keyFilter, group them
// into batches, and call the provided sendFunc with the batch as the argument. If at any point during the
// process an error occurs, the process will be terminated and the error will be returned.
func (ps *PersistentStorage) SendKeyValuePairs(ctx context.Context, sendFunc func(context.Context, []KeyValuePair) error, keyFilter KeyFilter) error {
	stream := ps.db.NewStream()

	switch keyFilter {
	case CommittedKeys:
		stream.Prefix = []byte{byte(committed)}
	case DirtyKeys:
		stream.Prefix = []byte{byte(dirty)}
	}

	stream.Send = func(buf *z.Buffer) error {
		kvList, err := badger.BufferToKVList(buf)
		if err != nil {
			return err
		}
		kvPairs := make([]KeyValuePair, 0, len(kvList.GetKv()))
		for _, kv := range kvList.GetKv() {
			k := internalKey(kv.GetKey())
			kvPairs = append(kvPairs, KeyValuePair{
				Key:       k.clientKey(),
				Value:     kv.GetValue(),
				Version:   k.version(),
				Committed: k.isCommitted(),
			})
		}

		return sendFunc(ctx, kvPairs)
	}

	// This is specifically for the case where all keys are being sent.
	// The client should never have access to metadata keys.
	stream.ChooseKey = func(item *badger.Item) bool {
		return !internalKey(item.Key()).isMetadata()
	}

	return stream.Orchestrate(ctx)
}

// CommitAll will commit all dirty keys from the current snapshot of the storage immediately.
// For each key that is committed, the provided onCommit callback will be invoked. If committing
// any of the keys fails, the process will be terminated and the error will be returned. This is
// not transactional. It is possible that some keys will have been committed while others will not
// have if an error occurs.
func (ps *PersistentStorage) CommitAll(ctx context.Context, onCommit func(ctx context.Context, key string, version uint64) error) error {
	stream := ps.db.NewStream()
	stream.Prefix = []byte{byte(dirty)}
	stream.Send = func(buf *z.Buffer) error {
		kvList, err := badger.BufferToKVList(buf)
		if err != nil {
			return err
		}
		for _, kv := range kvList.GetKv() {
			k := internalKey(kv.GetKey())
			key := k.clientKey()
			version := k.version()
			if err := ps.CommitVersion(key, version); err != nil {
				return err
			}
			if err := onCommit(ctx, key, version); err != nil {
				return err
			}
		}
		return nil
	}
	stream.ChooseKey = func(item *badger.Item) bool {
		return !internalKey(item.Key()).isMetadata()
	}

	return stream.Orchestrate(ctx)
}

func commit(txn *badger.Txn, key string, version uint64) error {
	md, err := getOrCreateMetadata(txn, key)
	if err != nil {
		return err
	}

	// Delete all versions of the key that are dirty up to and including the version being committed.
	// Update the value of the committed key to be the value of the version that was just committed.
	for _, oldVersion := range md.versions {
		if oldVersion > version {
			break
		}
		dirtyKey := newDirtyKey(key, oldVersion)
		if oldVersion == version {
			item, err := txn.Get(dirtyKey)
			if err != nil {
				return err
			}
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			committedKey := newCommittedKey(key, version)
			if err := txn.Set(committedKey, value); err != nil {
				return err
			}
		}
		if err := txn.Delete(dirtyKey); err != nil {
			return err
		}
	}

	// Update the metadata to reflect the latest committed version.
	if err := md.commitVersion(version); err != nil {
		return err
	}
	mdKey := newMetadataKey(key)
	mdBytes, err := md.bytes()
	if err != nil {
		return err
	}
	if err := txn.Set(mdKey, mdBytes); err != nil {
		return err
	}

	return nil
}

func read(txn *badger.Txn, key string) ([]byte, error) {
	md, err := getMetadata(txn, key)
	if err == badger.ErrKeyNotFound {
		return nil, ErrKeyDoesNotExist
	}
	if err != nil {
		return nil, err
	}
	if md.isDirty() {
		return nil, ErrDirtyRead
	}

	committedKey := newCommittedKey(key, md.lastCommitted)
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
		version = md.nextVersion()
	}
	if md.lastCommitted >= version {
		return 0, nil
	}
	if err := md.addVersion(version); err != nil {
		return 0, err
	}

	mdBytes, err := md.bytes()
	if err != nil {
		return 0, err
	}
	mdKey := newMetadataKey(key)
	if err := txn.Set(mdKey, mdBytes); err != nil {
		return 0, err
	}

	dirtyKey := newDirtyKey(key, version)
	if err = txn.Set(dirtyKey, value); err != nil {
		return 0, err
	}

	if shouldCommit {
		return version, commit(txn, key, version)
	}

	return version, nil
}

func getMetadata(txn *badger.Txn, key string) (*objectMetadata, error) {
	mdKey := newMetadataKey(key)
	item, err := txn.Get(mdKey)
	if err != nil {
		return nil, err
	}
	mdBytes, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	return newObjectMetadataFromBytes(mdBytes)
}

func getOrCreateMetadata(txn *badger.Txn, key string) (*objectMetadata, error) {
	mdKey := newMetadataKey(key)
	item, err := txn.Get(mdKey)

	var md *objectMetadata
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		md = newObjectMetadata(key)
	case err != nil:
		return nil, err
	default:
		mdBytes, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		md, err = newObjectMetadataFromBytes(mdBytes)
		if err != nil {
			return nil, err
		}
	}

	return md, nil
}
