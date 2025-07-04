package storage

import (
	"fmt"
	"slices"

	pb "github.com/jmsadair/zebraos/proto/pbstorage"
	"google.golang.org/protobuf/proto"
)

func makeVersionedKey(key string, version uint64) string {
	return fmt.Sprintf("%s-%d", key, version)
}

type KeyMetadata struct {
	key      string
	versions []uint64
}

func NewKeyMetadata(key string) *KeyMetadata {
	return &KeyMetadata{key: key, versions: []uint64{}}
}

func NewKeyMetadataFromBytes(b []byte) (*KeyMetadata, error) {
	var keyMetadataProto pb.KeyMetadata
	if err := proto.Unmarshal(b, &keyMetadataProto); err != nil {
		return nil, err
	}
	key := keyMetadataProto.GetKey()
	versions := keyMetadataProto.GetVersions()
	slices.Sort(versions)
	return &KeyMetadata{key: key, versions: versions}, nil
}

func (km *KeyMetadata) Bytes() ([]byte, error) {
	keyMetdataProto := pb.KeyMetadata{Key: km.key, Versions: km.versions}
	return proto.Marshal(&keyMetdataProto)
}

func (km *KeyMetadata) NewestVersion() uint64 {
	if len(km.versions) == 0 {
		return 0
	}
	return km.versions[len(km.versions)-1]
}

func (km *KeyMetadata) OldestVersion() uint64 {
	if len(km.versions) == 0 {
		return 0
	}
	return km.versions[0]
}

func (km *KeyMetadata) NextVersion() uint64 {
	return km.NewestVersion() + 1
}

func (km *KeyMetadata) HasVersion(version uint64) bool {
	for _, otherVersion := range km.versions {
		if otherVersion == version {
			return true
		}
		if otherVersion > version {
			break
		}
	}
	return false
}

func (km *KeyMetadata) IsDirty() bool {
	return len(km.versions) > 1
}

func (km *KeyMetadata) IncrementVersion() uint64 {
	nextVersion := km.NextVersion()
	km.versions = append(km.versions, nextVersion)
	return nextVersion
}

func (km *KeyMetadata) AddVersion(version uint64) {
	km.versions = append(km.versions, version)
}

func (km *KeyMetadata) RemoveOlderVersions(version uint64) {
	for i, otherVersion := range km.versions {
		if otherVersion >= version {
			km.versions = km.versions[i:]
			break
		}
	}
}
