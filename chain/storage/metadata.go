package storage

import (
	"errors"
	"slices"

	pb "github.com/jmsadair/keychainDB/proto/storage"
	"google.golang.org/protobuf/proto"
)

var (
	errAddingUnexpectedVersion     = errors.New("cannot add an object version that already exists or has existed")
	errCommittingUnexpectedVersion = errors.New("cannot commit an object version that does not exist")
)

type objectMetadata struct {
	key           string
	lastCommitted uint64
	versions      []uint64
}

func newObjectMetadata(key string) *objectMetadata {
	return &objectMetadata{key: key, versions: []uint64{}}
}

func newObjectMetadataFromBytes(b []byte) (*objectMetadata, error) {
	var objectMetadataProto pb.ObjectMetadata
	if err := proto.Unmarshal(b, &objectMetadataProto); err != nil {
		return nil, err
	}
	versions := objectMetadataProto.GetVersions()
	slices.Sort(versions)
	return &objectMetadata{
		key:           objectMetadataProto.GetKey(),
		lastCommitted: objectMetadataProto.GetLastCommitted(),
		versions:      versions,
	}, nil
}

func (om *objectMetadata) bytes() ([]byte, error) {
	keyMetadataProto := pb.ObjectMetadata{Key: om.key, LastCommitted: om.lastCommitted, Versions: om.versions}
	return proto.Marshal(&keyMetadataProto)
}

func (om *objectMetadata) newestVersion() uint64 {
	if len(om.versions) == 0 {
		return 0
	}
	return om.versions[len(om.versions)-1]
}

func (om *objectMetadata) oldestVersion() uint64 {
	if len(om.versions) == 0 {
		return 0
	}
	return om.versions[0]
}

func (om *objectMetadata) nextVersion() uint64 {
	return om.newestVersion() + 1
}

func (om *objectMetadata) hasVersion(version uint64) bool {
	for _, otherVersion := range om.versions {
		if otherVersion == version {
			return true
		}
		if otherVersion > version {
			break
		}
	}
	return false
}

func (om *objectMetadata) isDirty() bool {
	return om.newestVersion() > om.lastCommitted
}

func (om *objectMetadata) addVersion(version uint64) error {
	if om.newestVersion() >= version {
		return errAddingUnexpectedVersion
	}
	om.versions = append(om.versions, version)
	return nil
}

func (om *objectMetadata) commitVersion(version uint64) error {
	if version <= om.lastCommitted {
		return nil
	}
	if !om.hasVersion(version) {
		return errCommittingUnexpectedVersion
	}
	om.lastCommitted = version
	for i, otherVersion := range om.versions {
		if otherVersion == version {
			om.versions = om.versions[i:]
			break
		}
	}
	return nil
}
