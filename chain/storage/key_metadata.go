package storage

import (
	"errors"
	"fmt"
	"slices"

	pb "github.com/jmsadair/zebraos/proto/pbstorage"
	"google.golang.org/protobuf/proto"
)

var (
	// Returned when there is an attempt to add a version for a key and that version has existed or currently exists.
	ErrAddingUnexpectedVersion = errors.New("cannot add a key version that already exists or has existed")
	// Returned when there is an attempt to commit a version of a key that does not exist.
	ErrCommittingUnexpectedVersion = errors.New("cannot commit a key version that does not exist")
)

func makeVersionedKey(key string, version uint64) string {
	return fmt.Sprintf("%s-%d", key, version)
}

// KeyMetadata contains all metadata associated with a key.
type KeyMetadata struct {
	// The key that this metadata is associated with.
	Key string
	// The last committed version of the key. If no version
	// of the key has been committed before, this value will be zero.
	LastCommitted uint64
	// A sorted list of the versions of the key.
	Versions []uint64
}

// NewKeyMetadata will create a new KeyMetadata for the provided key.
func NewKeyMetadata(key string) *KeyMetadata {
	return &KeyMetadata{Key: key, Versions: []uint64{}}
}

// NewKeyMetadataFromBytes will create a new KeyMetadata from bytes.
func NewKeyMetadataFromBytes(b []byte) (*KeyMetadata, error) {
	var keyMetadataProto pb.KeyMetadata
	if err := proto.Unmarshal(b, &keyMetadataProto); err != nil {
		return nil, err
	}
	versions := keyMetadataProto.GetVersions()
	slices.Sort(versions)
	return &KeyMetadata{Key: keyMetadataProto.GetKey(), LastCommitted: keyMetadataProto.GetLastCommitted(), Versions: versions}, nil
}

// Bytes converts the metadata for the key into bytes.
func (km *KeyMetadata) Bytes() ([]byte, error) {
	keyMetdataProto := pb.KeyMetadata{Key: km.Key, LastCommitted: km.LastCommitted, Versions: km.Versions}
	return proto.Marshal(&keyMetdataProto)
}

// NewestVersion returns the newest version of the key that currently exists.
// If no versions of the key exist, then zero will be returned.
func (km *KeyMetadata) NewestVersion() uint64 {
	if len(km.Versions) == 0 {
		return 0
	}
	return km.Versions[len(km.Versions)-1]
}

// OldestVersion returns the oldest version of the key that currently exists.
// If no versions of the key exist, then zero will be returned.
func (km *KeyMetadata) OldestVersion() uint64 {
	if len(km.Versions) == 0 {
		return 0
	}
	return km.Versions[0]
}

// NextVersion will return the next version of the key that will be generated.
func (km *KeyMetadata) NextVersion() uint64 {
	return km.NewestVersion() + 1
}

// HasVersion will return true if the provided version currently exists and false otherwise.
func (km *KeyMetadata) HasVersion(version uint64) bool {
	for _, otherVersion := range km.Versions {
		if otherVersion == version {
			return true
		}
		if otherVersion > version {
			break
		}
	}
	return false
}

// IsDirty will return true if there are uncommitted versions of the key and false otherwise.
func (km *KeyMetadata) IsDirty() bool {
	return km.NewestVersion() > km.LastCommitted
}

// IncrementVersion will generate a new key version.
func (km *KeyMetadata) IncrementVersion() uint64 {
	nextVersion := km.NextVersion()
	km.Versions = append(km.Versions, nextVersion)
	return nextVersion
}

// Add adds a new version of the key.
// If the version is not newer than all existing versions, an error is returned.
func (km *KeyMetadata) AddVersion(version uint64) error {
	if km.NewestVersion() >= version {
		return ErrAddingUnexpectedVersion
	}
	km.Versions = append(km.Versions, version)
	return nil
}

// CommitVersion will commit the provided version of the key.
// When a version is committed, all older versions of the key are deleted.
// Committing a version older than the last committed version is a no-op.
func (km *KeyMetadata) CommitVersion(version uint64) error {
	if version <= km.LastCommitted {
		return nil
	}
	if !km.HasVersion(version) {
		return ErrCommittingUnexpectedVersion
	}
	km.LastCommitted = version
	for i, otherVersion := range km.Versions {
		if otherVersion == version {
			km.Versions = km.Versions[i:]
			break
		}
	}
	return nil
}
