package storage

import (
	"errors"

	badger "github.com/dgraph-io/badger/v4"
)

var (
	ErrDirtyRead           = errors.New("latest object version is dirty")
	ErrVersionDoesNotExist = errors.New("object version does not exist")
)

type PersistantStorage struct {
	db *badger.DB
}

func NewPersistantStorge(dbpath string) (*PersistantStorage, error) {
	db, err := badger.Open(badger.DefaultOptions(dbpath))
	if err != nil {
		return nil, err
	}
	return &PersistantStorage{db: db}, nil
}

func (ps *PersistantStorage) Close() {
	ps.db.Close()
}

func (ps *PersistantStorage) UncommitedWrite(key string, value []byte, version uint64) error {
	err := ps.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))

		var keyMetadata *KeyMetadata
		if errors.Is(err, badger.ErrKeyNotFound) {
			keyMetadata = NewKeyMetadata(key)
		} else {
			var b []byte
			if _, err = item.ValueCopy(b); err != nil {
				return err
			}
			keyMetadata, err = NewKeyMetadataFromBytes(b)
			if err != nil {
				return err
			}
		}
		keyMetadata.AddVersion(version)

		versionedKey := makeVersionedKey(key, version)
		if err = txn.Set([]byte(versionedKey), value); err != nil {
			return err
		}

		keyMetadataBytes, err := keyMetadata.Bytes()
		if err != nil {
			return err
		}
		if err := txn.Set([]byte(key), keyMetadataBytes); err != nil {
			return err
		}

		return txn.Commit()
	})

	return err
}

func (ps *PersistantStorage) CommitedWrite(key string, value []byte, version uint64) error {
	return nil
}

func (ps *PersistantStorage) CommitVersion(key string, version uint64) error {
	err := ps.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		var b []byte
		if _, err = item.ValueCopy(b); err != nil {
			return err
		}
		keyMetadata, err := NewKeyMetadataFromBytes(b)
		if err != nil {
			return err
		}

		keyMetadata.RemoveOlderVersions(version)
		b, err = keyMetadata.Bytes()
		if err != nil {
			return err
		}
		if err := txn.Set([]byte(key), b); err != nil {
			return err
		}

		return txn.Commit()
	})

	return err
}

func (ps *PersistantStorage) CommitedRead(key string) ([]byte, error) {
	var value []byte

	err := ps.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		var b []byte
		if _, err = item.ValueCopy(b); err != nil {
			return err
		}
		keyMetadata, err := NewKeyMetadataFromBytes(b)
		if err != nil {
			return err
		}

		if keyMetadata.IsDirty() {
			return ErrDirtyRead
		}

		versionedKey := makeVersionedKey(key, keyMetadata.NewestVersion())
		item, err = txn.Get([]byte(versionedKey))
		if err != nil {
			return err
		}
		if _, err := item.ValueCopy(value); err != nil {
			return err
		}

		return nil
	})

	return value, err
}
