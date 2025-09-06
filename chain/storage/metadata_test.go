package storage

import (
	"testing"

	pb "github.com/jmsadair/keychain/proto/pbstorage"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestNewObjectMetadata(t *testing.T) {
	key := "key"
	md := newObjectMetadata(key)
	require.Equal(t, key, md.key)
	require.Equal(t, uint64(0), md.lastCommitted)
	require.Equal(t, []uint64{}, md.versions)
}

func TestNewObjectMetadataFromBytes(t *testing.T) {
	key := "key"
	versions := []uint64{2, 3}
	lastCommited := uint64(2)

	mdProto := pb.ObjectMetadata{Key: key, LastCommitted: lastCommited, Versions: versions}
	b, err := proto.Marshal(&mdProto)
	require.NoError(t, err)

	md, err := newObjectMetadataFromBytes(b)
	require.NoError(t, err)
	require.Equal(t, key, md.key)
	require.Equal(t, lastCommited, md.lastCommitted)
	require.Equal(t, versions, md.versions)
}

func TestObjectMetadataBytes(t *testing.T) {
	key := "key"
	versions := []uint64{2, 3}
	lastCommited := uint64(2)

	md := objectMetadata{key: key, lastCommitted: lastCommited, versions: versions}
	b, err := md.bytes()
	require.NoError(t, err)

	var mdProto pb.ObjectMetadata
	err = proto.Unmarshal(b, &mdProto)
	require.NoError(t, err)
	require.Equal(t, key, mdProto.GetKey())
	require.Equal(t, lastCommited, mdProto.GetLastCommitted())
	require.Equal(t, versions, mdProto.GetVersions())
}

func TestObjectMetadataHasVersion(t *testing.T) {
	key := "key"
	md := newObjectMetadata(key)

	require.NoError(t, md.addVersion(1))
	require.NoError(t, md.addVersion(2))
	require.True(t, md.hasVersion(1))
	require.True(t, md.hasVersion(2))
	require.False(t, md.hasVersion(3))
}

func TestObjectMetadataAddVersion(t *testing.T) {
	key := "key"
	md := newObjectMetadata(key)

	require.NoError(t, md.addVersion(1))
	require.Equal(t, uint64(1), md.oldestVersion())
	require.Equal(t, uint64(1), md.newestVersion())
	require.Equal(t, uint64(2), md.nextVersion())

	require.NoError(t, md.addVersion(2))
	require.Equal(t, uint64(1), md.oldestVersion())
	require.Equal(t, uint64(2), md.newestVersion())
	require.Equal(t, uint64(3), md.nextVersion())

	require.NoError(t, md.addVersion(4))
	require.Equal(t, uint64(1), md.oldestVersion())
	require.Equal(t, uint64(4), md.newestVersion())
	require.Equal(t, uint64(5), md.nextVersion())
}

func TestObjectMetadataCommitVersion(t *testing.T) {
	key := "key"
	md := newObjectMetadata(key)

	require.NoError(t, md.addVersion(1))
	require.NoError(t, md.addVersion(2))
	require.NoError(t, md.addVersion(3))
	require.NoError(t, md.addVersion(4))
	require.Equal(t, uint64(0), md.lastCommitted)

	require.NoError(t, md.commitVersion(3))
	require.Equal(t, uint64(3), md.oldestVersion())
	require.Equal(t, uint64(4), md.newestVersion())
	require.Equal(t, uint64(5), md.nextVersion())
	require.True(t, md.isDirty())
	require.Equal(t, uint64(3), md.lastCommitted)

	require.NoError(t, md.commitVersion(4))
	require.Equal(t, uint64(4), md.oldestVersion())
	require.Equal(t, uint64(4), md.newestVersion())
	require.Equal(t, uint64(5), md.nextVersion())
	require.False(t, md.isDirty())
	require.Equal(t, uint64(4), md.lastCommitted)
}
