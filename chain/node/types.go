package node

import (
	"github.com/jmsadair/keychain/chain/storage"
	pb "github.com/jmsadair/keychain/proto/pbchain"
)

// ReplicateRequest represents a request to write a versioned key-value pair.
type ReplicateRequest struct {
	// The key being written.
	Key string
	// The value being written.
	Value []byte
	// The configuration version of the client.
	ConfigVersion uint64
}

// Proto converts a ReplicateRequest to its protobuf message equivalent.
func (r *ReplicateRequest) Proto() *pb.ReplicateRequest {
	return &pb.ReplicateRequest{
		Key:           r.Key,
		Value:         r.Value,
		ConfigVersion: r.ConfigVersion,
	}
}

// FromProto converts a ReplicateRequest protobuf message to a ReplicateRequest.
func (r *ReplicateRequest) FromProto(pbReq *pb.ReplicateRequest) {
	r.Key = pbReq.GetKey()
	r.Value = pbReq.GetValue()
	r.ConfigVersion = pbReq.GetConfigVersion()
}

// ReplicateResponse represents a response to a write request.
type ReplicateResponse struct{}

// Proto converts a ReplicateResponse to its protobuf message equivalent.
func (r *ReplicateResponse) Proto() *pb.ReplicateResponse {
	return &pb.ReplicateResponse{}
}

// FromProto converts a ReplicateResponse protobuf message to a ReplicateResponse.
func (r *ReplicateResponse) FromProto(pbReq *pb.ReplicateResponse) {}

// WriteRequest represents a request to write a versioned key-value pair.
type WriteRequest struct {
	// The key being written.
	Key string
	// The value being written.
	Value []byte
	// The version of this key-value pair.
	Version uint64
	// The configuration version of the client.
	ConfigVersion uint64
}

// Proto converts a WriteRequest to its protobuf message equivalent.
func (r *WriteRequest) Proto() *pb.WriteRequest {
	return &pb.WriteRequest{
		Key:           r.Key,
		Value:         r.Value,
		Version:       r.Version,
		ConfigVersion: r.ConfigVersion,
	}
}

// FromProto converts a WriteRequest protobuf message to a WriteRequest.
func (r *WriteRequest) FromProto(pbReq *pb.WriteRequest) {
	r.Key = pbReq.GetKey()
	r.Value = pbReq.GetValue()
	r.Version = pbReq.GetVersion()
	r.ConfigVersion = pbReq.GetConfigVersion()
}

// WriteResponse represents a response to a write request.
type WriteResponse struct{}

// Proto converts a WriteResponse to its protobuf message equivalent.
func (r *WriteResponse) Proto() *pb.WriteResponse {
	return &pb.WriteResponse{}
}

// FromProto converts a WriteResponse protobuf message to a WriteResponse.
func (r *WriteResponse) FromProto(pbReq *pb.WriteResponse) {}

// ReadRequest is a request to read a key-value pair.
type ReadRequest struct {
	// The key to read.
	Key string
	// The configuration version of the client.
	ConfigVersion uint64
}

// Proto converts a ReadRequest to its protobuf message equivalent.
func (r *ReadRequest) Proto() *pb.ReadRequest {
	return &pb.ReadRequest{
		Key:           r.Key,
		ConfigVersion: r.ConfigVersion,
	}
}

// FromProto converts a ReadRequest protobuf message to a WriteRequest.
func (r *ReadRequest) FromProto(pbReq *pb.ReadRequest) {
	r.Key = pbReq.GetKey()
	r.ConfigVersion = pbReq.ConfigVersion
}

// ReadResponse is a response to a ReadRequest and contains the read value.
type ReadResponse struct {
	// The value read.
	Value []byte
}

// Proto converts a ReadResponse to its protobuf message equivalent.
func (r *ReadResponse) Proto() *pb.ReadResponse {
	return &pb.ReadResponse{
		Value: r.Value,
	}
}

// FromProto converts a ReadResponse protobuf message to a ReadResponse.
func (r *ReadResponse) FromProto(pbResp *pb.ReadResponse) {
	r.Value = pbResp.GetValue()
}

// CommitRequest is a request to commit a specific version of a key-value pair.
type CommitRequest struct {
	// The key to commit.
	Key string
	// The version of the key to commit.
	Version uint64
	// The configuration version of the client.
	ConfigVersion uint64
}

// Proto converts a CommitRequest to its protobuf message equivalent.
func (r *CommitRequest) Proto() *pb.CommitRequest {
	return &pb.CommitRequest{
		Key:           r.Key,
		Version:       r.Version,
		ConfigVersion: r.ConfigVersion,
	}
}

// FromProto converts a CommitRequest protobuf message to a CommitRequest.
func (r *CommitRequest) FromProto(pbReq *pb.CommitRequest) {
	r.Key = pbReq.GetKey()
	r.Version = pbReq.GetVersion()
	r.ConfigVersion = pbReq.GetConfigVersion()
}

// CommitResponse is a response to a CommitRequest
type CommitResponse struct{}

// Proto converts a CommitResponse to its protobuf message equivalent.
func (r *CommitResponse) Proto() *pb.CommitResponse {
	return &pb.CommitResponse{}
}

// FromProto converts a CommitResponse protobuf message to a CommitResponse.
func (r *CommitResponse) FromProto(pbReq *pb.CommitResponse) {}

// PropagateRequest is a request made from one member of a chain to another to initiate a stream
// of key-value pairs that match the specified filter.
type PropagateRequest struct {
	// A filter that specifies which keys should be propagated.
	KeyFilter storage.KeyFilter
	// The configuration version of the client.
	ConfigVersion uint64
}

// Proto converts a PropagateRequest to its protobuf message equivalent.
func (r *PropagateRequest) Proto() *pb.PropagateRequest {
	var keyType pb.KeyType
	switch r.KeyFilter {
	case storage.AllKeys:
		keyType = pb.KeyType_KEYTYPE_ALL
	case storage.CommittedKeys:
		keyType = pb.KeyType_KEYTYPE_COMMITTED
	case storage.DirtyKeys:
		keyType = pb.KeyType_KEYTYPE_DIRTY
	}
	return &pb.PropagateRequest{
		KeyType:       keyType,
		ConfigVersion: r.ConfigVersion,
	}
}

// FromProto converts a PropagateRequest protobuf message to a PropagateRequest.
func (r *PropagateRequest) FromProto(pbReq *pb.PropagateRequest) {
	switch pbReq.GetKeyType() {
	case pb.KeyType_KEYTYPE_ALL:
		r.KeyFilter = storage.AllKeys
	case pb.KeyType_KEYTYPE_COMMITTED:
		r.KeyFilter = storage.CommittedKeys
	case pb.KeyType_KEYTYPE_DIRTY:
		r.KeyFilter = storage.DirtyKeys
	}
	r.ConfigVersion = pbReq.GetConfigVersion()
}

// UpdateConfigurationRequest is a request to update the chain configuration.
type UpdateConfigurationRequest struct {
	// The new configuration.
	Configuration *Configuration
}

// Proto converts a UpdateConfigurationRequest to its protobuf message equivalent.
func (r *UpdateConfigurationRequest) Proto() *pb.UpdateConfigurationRequest {
	return &pb.UpdateConfigurationRequest{
		Configuration: r.Configuration.Proto(),
	}
}

// FromProto converts a UpdateConfigurationRequest protobuf message to a UpdateConfigurationRequest.
func (r *UpdateConfigurationRequest) FromProto(pbReq *pb.UpdateConfigurationRequest) {
	r.Configuration = NewConfigurationFromProto(pbReq.GetConfiguration())
}

// UpdateConfigurationResponse is a response to an UpdateConfigurationRequest.
type UpdateConfigurationResponse struct{}

// Proto converts a UpdateConfigurationResponse to its protobuf message equivalent.
func (r *UpdateConfigurationResponse) Proto() *pb.UpdateConfigurationResponse {
	return &pb.UpdateConfigurationResponse{}
}

// FromProto converts a UpdateConfigurationResponse protobuf message to a UpdateConfigurationResponse.
func (r *UpdateConfigurationResponse) FromProto(pbReq *pb.UpdateConfigurationResponse) {}

// PingRequest is a request made by the coordinator to check if a chain member is alive
// and has an up-to-date configuration.
type PingRequest struct{}

// Proto converts a PingRequest to its protobuf message equivalent.
func (r *PingRequest) Proto() *pb.PingRequest {
	return &pb.PingRequest{}
}

// FromProto converts a PingRequest protobuf message to a PingRequest.
func (r *PingRequest) FromProto(pbReq *pb.PingRequest) {}

// PingResponse is a response to a PingRequest.
type PingResponse struct {
	// The current status of the chain member.
	Status Status
	// The version of the configuration it has.
	Version uint64
}

// Proto converts a PingResponse to its protobuf message equivalent.
func (r *PingResponse) Proto() *pb.PingResponse {
	return &pb.PingResponse{
		Status:        int32(r.Status),
		ConfigVersion: r.Version,
	}
}

// FromProto converts a PingResponse protobuf message to a PingResponse.
func (r *PingResponse) FromProto(pbResp *pb.PingResponse) {
	r.Status = Status(pbResp.GetStatus())
	r.Version = pbResp.GetConfigVersion()
}
