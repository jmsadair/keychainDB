package storage

import (
	"bytes"
	"encoding/binary"
)

const (
	isMetadataKeyOffset = 0
	versionOffset       = 1
	metdataLenBytes     = 9
)

// CanonicalKey is an internal representation of a key that is meant to uniquely identify an item in the storage system.
// | IsMetadataKey (uint8) | Version (uint64) | Client Key (variable length string) |
type CanonicalKey []byte

// NewCanonicalKey creates a new CanonicalKey instance.
func NewCanonicalKey(key string, isMetadataKey bool, version uint64) (CanonicalKey, error) {
	buf := new(bytes.Buffer)

	var isMetadataKeyByte byte
	if isMetadataKey {
		isMetadataKeyByte = 1
	}
	if err := buf.WriteByte(isMetadataKeyByte); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, version); err != nil {
		return nil, err
	}
	keyBytes := []byte(key)
	if _, err := buf.Write(keyBytes); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// NewMetadataKey creates a new CanonicalKey instance that is intended to identify object metadata.
func NewMetadataKey(key string) (CanonicalKey, error) {
	return NewCanonicalKey(key, true, 0)
}

// NewDataKey creates a new CanonicalKey instance that is intended to identify an object.
func NewDataKey(key string, version uint64) (CanonicalKey, error) {
	return NewCanonicalKey(key, false, version)
}

// IsMetadataKey returns true is this key identifies object metadata and false otherwise.
func (ck CanonicalKey) IsMetadataKey() bool {
	return ck[isMetadataKeyOffset]&1 == 1
}

// Version returns the version of the key.
// If the key is one that identifies object metadata, then the version will always be zero.
func (ck CanonicalKey) Version() uint64 {
	return binary.BigEndian.Uint64(ck[versionOffset:])
}

// Key returns the client key.
func (ck CanonicalKey) Key() string {
	return string(ck[metdataLenBytes:])
}
