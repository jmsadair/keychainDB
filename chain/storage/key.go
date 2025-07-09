package storage

import (
	"bytes"
	"encoding/binary"
)

// InternalKey is an internal representation of a key used by storage.
type InternalKey struct {
	// A key associated with an object.
	Key string
	// Indicates whether this key is a pointer to object metadata or not.
	IsMetadataKey bool
	// The version of the key. This is only applies to keys that point to client data.
	Version uint64
}

// NewInternalKey creates an InternalKey instance from the provided key and version.
func NewInternalKey(key string, isMetadataKey bool, version uint64) *InternalKey {
	return &InternalKey{Key: key, IsMetadataKey: isMetadataKey, Version: version}
}

// NewInternalKeyFromBytes creates an InternalKey instance from bytes.
func NewInternalKeyFromBytes(b []byte) (*InternalKey, error) {
	buf := bytes.NewReader(b)

	var keyLen uint16
	if err := binary.Read(buf, binary.BigEndian, &keyLen); err != nil {
		return nil, err
	}
	key := make([]byte, keyLen)
	if _, err := buf.Read(key); err != nil {
		return nil, err
	}
	var version uint64
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return nil, err
	}
	isMetadataKey, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}

	return &InternalKey{Key: string(key), Version: version, IsMetadataKey: isMetadataKey == 1}, nil
}

// Bytes converts an InternalKey instance into bytes.
func (ik *InternalKey) Bytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	keyBytes := []byte(ik.Key)
	if err := binary.Write(buf, binary.BigEndian, uint16(len(keyBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(keyBytes); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, ik.Version); err != nil {
		return nil, err
	}

	var isMetadataKey byte
	if ik.IsMetadataKey {
		isMetadataKey = 1
	}
	if err := buf.WriteByte(isMetadataKey); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
