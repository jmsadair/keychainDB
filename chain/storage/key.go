package storage

import (
	"encoding/binary"
)

type keyType uint8

const (
	metadata keyType = iota
	committed
	dirty
)

const (
	keyTypeOffset = 0
	versionOffset = 1
)

// Key is an internal representation of a key that is meant to uniquely identify an item in the storage system.
type Key []byte

// NewMetadataKey creates a new key that is intended to identify object metadata.
func NewMetadataKey(key string) Key {
	b := []byte{byte(metadata)}
	return append(b, []byte(key)...)
}

// NewDirtyKey creates a new key that is intended to identify a an object that has not been committed.
func NewDirtyKey(key string, version uint64) Key {
	b := []byte{byte(dirty)}
	b = binary.BigEndian.AppendUint64(b, version)
	return append(b, []byte(key)...)
}

// NewCommittedKey creates a new key that is intended to identify a an object that has not been committed.
func NewCommittedKey(key string, version uint64) Key {
	b := []byte{byte(committed)}
	b = binary.BigEndian.AppendUint64(b, version)
	return append(b, []byte(key)...)
}

// IsMetadata returns true is this key identifies object metadata and false otherwise.
func (k Key) IsMetadata() bool {
	return k[keyTypeOffset]^byte(metadata) == 0
}

// IsCommitted returns true is this key identifies a version of an object that has been committed and false otherwise.
func (k Key) IsCommitted() bool {
	return k[keyTypeOffset]^byte(committed) == 0
}

// IsDirty returns true is this key identifies a version of an object that has not been committed and false otherwise.
func (k Key) IsDirty() bool {
	return k[keyTypeOffset]^byte(dirty) == 0
}

// Version returns the version of the key. Note that this only applies to committed and dirty key types.
// If the key is a metadata key, zero will always be returned.
func (k Key) Version() uint64 {
	if k.IsMetadata() {
		return 0
	}
	return binary.BigEndian.Uint64(k[versionOffset:])
}

// ClientKey returns the client key.
func (k Key) ClientKey() string {
	switch keyType(k[keyTypeOffset]) {
	case metadata:
		return string(k[keyTypeOffset+1:])
	case committed, dirty:
		return string(k[versionOffset+8:])
	}
	panic("unexpected key type")
}
