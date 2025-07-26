package storage

import (
	"testing"

	pb "github.com/jmsadair/zebraos/proto/pbstorage"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestNewObjectMetadata(t *testing.T) {
	key := "key"
	objectMetadata := NewObjectMetadata(key)
	require.Equal(t, key, objectMetadata.Key)
	require.Equal(t, uint64(0), objectMetadata.LastCommitted)
	require.Equal(t, []uint64{}, objectMetadata.Versions)
}

func TestNewObjectMetadataFromBytes(t *testing.T) {
	key := "key"
	versions := []uint64{2, 3}
	lastCommited := uint64(2)

	objectMetadataProto := pb.ObjectMetadata{Key: key, LastCommitted: lastCommited, Versions: versions}
	b, err := proto.Marshal(&objectMetadataProto)
	require.NoError(t, err)

	objectMetadata, err := NewObjectMetadataFromBytes(b)
	require.NoError(t, err)
	require.Equal(t, key, objectMetadata.Key)
	require.Equal(t, lastCommited, objectMetadata.LastCommitted)
	require.Equal(t, versions, objectMetadata.Versions)
}

func TestObjectMetadataBytes(t *testing.T) {
	key := "key"
	versions := []uint64{2, 3}
	lastCommited := uint64(2)

	objectMetadata := ObjectMetadata{Key: key, LastCommitted: lastCommited, Versions: versions}
	b, err := objectMetadata.Bytes()
	require.NoError(t, err)

	var objectMetadataProto pb.ObjectMetadata
	err = proto.Unmarshal(b, &objectMetadataProto)
	require.NoError(t, err)
	require.Equal(t, key, objectMetadataProto.GetKey())
	require.Equal(t, lastCommited, objectMetadataProto.GetLastCommitted())
	require.Equal(t, versions, objectMetadataProto.GetVersions())
}

func TestObjectMetadataHasVersion(t *testing.T) {
	key := "key"
	objectMetadata := NewObjectMetadata(key)

	require.NoError(t, objectMetadata.AddVersion(1))
	require.NoError(t, objectMetadata.AddVersion(2))
	require.True(t, objectMetadata.HasVersion(1))
	require.True(t, objectMetadata.HasVersion(2))
	require.False(t, objectMetadata.HasVersion(3))
}

func TestObjectMetadataAddVersion(t *testing.T) {
	key := "key"
	objectMetadata := NewObjectMetadata(key)

	require.NoError(t, objectMetadata.AddVersion(1))
	require.Equal(t, uint64(1), objectMetadata.OldestVersion())
	require.Equal(t, uint64(1), objectMetadata.NewestVersion())
	require.Equal(t, uint64(2), objectMetadata.NextVersion())

	require.NoError(t, objectMetadata.AddVersion(2))
	require.Equal(t, uint64(1), objectMetadata.OldestVersion())
	require.Equal(t, uint64(2), objectMetadata.NewestVersion())
	require.Equal(t, uint64(3), objectMetadata.NextVersion())

	require.NoError(t, objectMetadata.AddVersion(4))
	require.Equal(t, uint64(1), objectMetadata.OldestVersion())
	require.Equal(t, uint64(4), objectMetadata.NewestVersion())
	require.Equal(t, uint64(5), objectMetadata.NextVersion())
}

func TestObjectMetadataCommitVersion(t *testing.T) {
	key := "key"
	objectMetadata := NewObjectMetadata(key)

	require.NoError(t, objectMetadata.AddVersion(1))
	require.NoError(t, objectMetadata.AddVersion(2))
	require.NoError(t, objectMetadata.AddVersion(3))
	require.NoError(t, objectMetadata.AddVersion(4))
	require.Equal(t, uint64(0), objectMetadata.LastCommitted)

	require.NoError(t, objectMetadata.CommitVersion(3))
	require.Equal(t, uint64(3), objectMetadata.OldestVersion())
	require.Equal(t, uint64(4), objectMetadata.NewestVersion())
	require.Equal(t, uint64(5), objectMetadata.NextVersion())
	require.True(t, objectMetadata.IsDirty())
	require.Equal(t, uint64(3), objectMetadata.LastCommitted)

	require.NoError(t, objectMetadata.CommitVersion(4))
	require.Equal(t, uint64(4), objectMetadata.OldestVersion())
	require.Equal(t, uint64(4), objectMetadata.NewestVersion())
	require.Equal(t, uint64(5), objectMetadata.NextVersion())
	require.False(t, objectMetadata.IsDirty())
	require.Equal(t, uint64(4), objectMetadata.LastCommitted)
}
