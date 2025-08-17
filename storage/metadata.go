package storage

import (
	"errors"
	"slices"

	pb "github.com/jmsadair/keychain/proto/pbstorage"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrAddingUnexpectedVersion is returned when there is an attempt to add a version for
	// an object and that version has existed or currently exists.
	ErrAddingUnexpectedVersion = errors.New("cannot add an object version that already exists or has existed")
	// ErrCommittingUnexpectedVersion is returned when there is an attempt to commit a version
	// of a object that does not exist.
	ErrCommittingUnexpectedVersion = errors.New("cannot commit an object version that does not exist")
)

// ObjectMetadata contains all metadata associated with an object.
type ObjectMetadata struct {
	// The key of the object that this metadata is associated with.
	Key string
	// The last committed version of this object.
	// If no version of the object has been committed before, this value will be zero.
	LastCommitted uint64
	// A sorted list of the versions of the object.
	Versions []uint64
}

// NewObjectMetadata will create a new ObjectMetadata for the provided key.
func NewObjectMetadata(key string) *ObjectMetadata {
	return &ObjectMetadata{Key: key, Versions: []uint64{}}
}

// NewObjectMetadataFromBytes will create a new ObjectMetadata from the provided bytes.
func NewObjectMetadataFromBytes(b []byte) (*ObjectMetadata, error) {
	var objectMetadataProto pb.ObjectMetadata
	if err := proto.Unmarshal(b, &objectMetadataProto); err != nil {
		return nil, err
	}
	versions := objectMetadataProto.GetVersions()
	slices.Sort(versions)
	return &ObjectMetadata{
		Key:           objectMetadataProto.GetKey(),
		LastCommitted: objectMetadataProto.GetLastCommitted(),
		Versions:      versions,
	}, nil
}

// Bytes converts the ObjectMetadata instance into bytes.
func (om *ObjectMetadata) Bytes() ([]byte, error) {
	keyMetadataProto := pb.ObjectMetadata{Key: om.Key, LastCommitted: om.LastCommitted, Versions: om.Versions}
	return proto.Marshal(&keyMetadataProto)
}

// NewestVersion returns the newest version of the object that currently exists.
// If no versions of the object exist, then zero will be returned.
func (om *ObjectMetadata) NewestVersion() uint64 {
	if len(om.Versions) == 0 {
		return 0
	}
	return om.Versions[len(om.Versions)-1]
}

// OldestVersion returns the oldest version of the object that currently exists.
// If no versions of the object exist, then zero will be returned.
func (om *ObjectMetadata) OldestVersion() uint64 {
	if len(om.Versions) == 0 {
		return 0
	}
	return om.Versions[0]
}

// NextVersion will return the next version of the object that will be generated.
func (om *ObjectMetadata) NextVersion() uint64 {
	return om.NewestVersion() + 1
}

// HasVersion will return true if the provided version currently exists and false otherwise.
func (om *ObjectMetadata) HasVersion(version uint64) bool {
	for _, otherVersion := range om.Versions {
		if otherVersion == version {
			return true
		}
		if otherVersion > version {
			break
		}
	}
	return false
}

// IsDirty will return true if there are uncommitted versions of the object and false otherwise.
func (om *ObjectMetadata) IsDirty() bool {
	return om.NewestVersion() > om.LastCommitted
}

// AddVersion adds a new version of the object.
// If the version is not newer than all existing versions, an error is returned.
func (om *ObjectMetadata) AddVersion(version uint64) error {
	if om.NewestVersion() >= version {
		return ErrAddingUnexpectedVersion
	}
	om.Versions = append(om.Versions, version)
	return nil
}

// CommitVersion will commit the provided version of the object.
// When a version is committed, all older versions of the object are deleted.
// Committing a version older than the last committed version is a no-op.
func (om *ObjectMetadata) CommitVersion(version uint64) error {
	if version <= om.LastCommitted {
		return nil
	}
	if !om.HasVersion(version) {
		return ErrCommittingUnexpectedVersion
	}
	om.LastCommitted = version
	for i, otherVersion := range om.Versions {
		if otherVersion == version {
			om.Versions = om.Versions[i:]
			break
		}
	}
	return nil
}
