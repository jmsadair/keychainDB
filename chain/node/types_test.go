package node

import (
	"testing"

	"github.com/jmsadair/keychain/chain/storage"
	"github.com/stretchr/testify/require"
)

func TestWriteRequestToFromProto(t *testing.T) {
	req := &WriteRequest{
		Key:           "key",
		Value:         []byte("value"),
		Version:       42,
		ConfigVersion: 3,
	}
	pbReq := req.Proto()
	var result WriteRequest
	result.FromProto(pbReq)

	require.Equal(t, req.Key, result.Key)
	require.Equal(t, req.Value, result.Value)
	require.Equal(t, req.Version, result.Version)
	require.Equal(t, req.ConfigVersion, result.ConfigVersion)
}

func TestReadRequestToFromProto(t *testing.T) {
	req := &ReadRequest{
		Key:           "key",
		ConfigVersion: 1,
	}
	pbReq := req.Proto()
	var result ReadRequest
	result.FromProto(pbReq)

	require.Equal(t, req.Key, result.Key)
	require.Equal(t, req.ConfigVersion, result.ConfigVersion)
}

func TestCommitRequestToFromProto(t *testing.T) {
	req := &CommitRequest{
		Key:           "key",
		Version:       123,
		ConfigVersion: 10,
	}
	pbReq := req.Proto()
	var result CommitRequest
	result.FromProto(pbReq)

	require.Equal(t, req.Key, result.Key)
	require.Equal(t, req.Version, result.Version)
	require.Equal(t, req.ConfigVersion, result.ConfigVersion)
}

func TestPropagateRequestToFromProto(t *testing.T) {
	tests := []struct {
		filter storage.KeyFilter
	}{
		{storage.AllKeys},
		{storage.CommittedKeys},
		{storage.DirtyKeys},
	}

	for _, tt := range tests {
		req := &PropagateRequest{
			KeyFilter:     tt.filter,
			ConfigVersion: 1,
		}
		pbReq := req.Proto()
		var result PropagateRequest
		result.FromProto(pbReq)

		require.Equal(t, req.KeyFilter, result.KeyFilter)
		require.Equal(t, req.ConfigVersion, result.ConfigVersion)
	}
}

func TestPingResponseToFromProto(t *testing.T) {
	req := &PingResponse{
		Status:  Syncing,
		Version: 9,
	}
	pbReq := req.Proto()
	var result PingResponse
	result.FromProto(pbReq)

	require.Equal(t, req.Status, result.Status)
	require.Equal(t, req.Version, result.Version)
}
