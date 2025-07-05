package storage

import (
	"testing"

	pb "github.com/jmsadair/zebraos/proto/pbstorage"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestNewKeyMetadata(t *testing.T) {
	key := "key"
	keyMetadata := NewKeyMetadata(key)
	require.Equal(t, key, keyMetadata.Key)
	require.Equal(t, uint64(0), keyMetadata.LastCommitted)
	require.Equal(t, []uint64{}, keyMetadata.Versions)
}

func TestNewKeyMetadataFromBytes(t *testing.T) {
	key := "key"
	versions := []uint64{2, 3}
	lastCommited := uint64(2)

	keyMetadataProto := pb.KeyMetadata{Key: key, LastCommitted: lastCommited, Versions: versions}
	b, err := proto.Marshal(&keyMetadataProto)
	require.NoError(t, err)

	keyMetadata, err := NewKeyMetadataFromBytes(b)
	require.NoError(t, err)
	require.Equal(t, key, keyMetadata.Key)
	require.Equal(t, lastCommited, keyMetadata.LastCommitted)
	require.Equal(t, versions, keyMetadata.Versions)
}

func TestKeyMetadataBytes(t *testing.T) {
	key := "key"
	versions := []uint64{2, 3}
	lastCommited := uint64(2)

	keyMetadata := KeyMetadata{Key: key, LastCommitted: lastCommited, Versions: versions}
	b, err := keyMetadata.Bytes()
	require.NoError(t, err)

	var keyMetadataProto pb.KeyMetadata
	err = proto.Unmarshal(b, &keyMetadataProto)
	require.NoError(t, err)
	require.Equal(t, key, keyMetadataProto.GetKey())
	require.Equal(t, lastCommited, keyMetadataProto.GetLastCommitted())
	require.Equal(t, versions, keyMetadataProto.GetVersions())
}

func TestKeyMetadataHasVersion(t *testing.T) {
	key := "key"
	keyMetadata := NewKeyMetadata(key)

	require.NoError(t, keyMetadata.AddVersion(1))
	require.NoError(t, keyMetadata.AddVersion(2))
	require.True(t, keyMetadata.HasVersion(1))
	require.True(t, keyMetadata.HasVersion(2))
	require.False(t, keyMetadata.HasVersion(3))
}

func TestKeyMetadataAddVersion(t *testing.T) {
	key := "key"
	keyMetadata := NewKeyMetadata(key)

	require.NoError(t, keyMetadata.AddVersion(1))
	require.Equal(t, uint64(1), keyMetadata.OldestVersion())
	require.Equal(t, uint64(1), keyMetadata.NewestVersion())
	require.Equal(t, uint64(2), keyMetadata.NextVersion())

	require.NoError(t, keyMetadata.AddVersion(2))
	require.Equal(t, uint64(1), keyMetadata.OldestVersion())
	require.Equal(t, uint64(2), keyMetadata.NewestVersion())
	require.Equal(t, uint64(3), keyMetadata.NextVersion())

	require.NoError(t, keyMetadata.AddVersion(4))
	require.Equal(t, uint64(1), keyMetadata.OldestVersion())
	require.Equal(t, uint64(4), keyMetadata.NewestVersion())
	require.Equal(t, uint64(5), keyMetadata.NextVersion())
}

func TestKeyMetadataCommitVersion(t *testing.T) {
	key := "key"
	keyMetadata := NewKeyMetadata(key)

	require.NoError(t, keyMetadata.AddVersion(1))
	require.NoError(t, keyMetadata.AddVersion(2))
	require.NoError(t, keyMetadata.AddVersion(3))
	require.NoError(t, keyMetadata.AddVersion(4))
	require.Equal(t, uint64(0), keyMetadata.LastCommitted)

	require.NoError(t, keyMetadata.CommitVersion(3))
	require.Equal(t, uint64(3), keyMetadata.OldestVersion())
	require.Equal(t, uint64(4), keyMetadata.NewestVersion())
	require.Equal(t, uint64(5), keyMetadata.NextVersion())
	require.True(t, keyMetadata.IsDirty())
	require.Equal(t, uint64(3), keyMetadata.LastCommitted)

	require.NoError(t, keyMetadata.CommitVersion(4))
	require.Equal(t, uint64(4), keyMetadata.OldestVersion())
	require.Equal(t, uint64(4), keyMetadata.NewestVersion())
	require.Equal(t, uint64(5), keyMetadata.NextVersion())
	require.False(t, keyMetadata.IsDirty())
	require.Equal(t, uint64(4), keyMetadata.LastCommitted)
}
