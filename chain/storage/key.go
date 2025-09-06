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

type internalKey []byte

func newMetadataKey(key string) internalKey {
	b := []byte{byte(metadata)}
	return append(b, []byte(key)...)
}

func newDirtyKey(key string, version uint64) internalKey {
	b := []byte{byte(dirty)}
	b = binary.BigEndian.AppendUint64(b, version)
	return append(b, []byte(key)...)
}

func newCommittedKey(key string, version uint64) internalKey {
	b := []byte{byte(committed)}
	b = binary.BigEndian.AppendUint64(b, version)
	return append(b, []byte(key)...)
}

func (k internalKey) isMetadata() bool {
	return k[keyTypeOffset] == byte(metadata)
}

func (k internalKey) isCommitted() bool {
	return k[keyTypeOffset] == byte(committed)
}

func (k internalKey) isDirty() bool {
	return k[keyTypeOffset] == byte(dirty)
}

func (k internalKey) version() uint64 {
	if k.isMetadata() {
		return 0
	}
	return binary.BigEndian.Uint64(k[versionOffset:])
}

func (k internalKey) clientKey() string {
	switch keyType(k[keyTypeOffset]) {
	case metadata:
		return string(k[keyTypeOffset+1:])
	case committed, dirty:
		return string(k[versionOffset+8:])
	}
	panic("unexpected key type")
}
