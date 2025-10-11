package storage

import (
	"errors"

	"github.com/dgraph-io/badger/v4"
)

var (
	ErrUncommittedRead = errors.New("chainstorage: latest version is uncommitted")
	ErrKeyNotFound     = errors.New("chainstorage: key does not exist")
	ErrEmptyKey        = errors.New("chainstorage: key is empty")
	ErrConflict        = errors.New("chainstorage: conflicting transactions")
)

func handleError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, badger.ErrKeyNotFound):
		return ErrKeyNotFound
	case errors.Is(err, badger.ErrEmptyKey):
		return ErrEmptyKey
	case errors.Is(err, badger.ErrConflict):
		return ErrConflict
	case errors.Is(err, ErrUncommittedRead):
		return err
	default:
		return err
	}
}
