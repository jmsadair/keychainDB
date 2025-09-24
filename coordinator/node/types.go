package node

import (
	chainnode "github.com/jmsadair/keychain/chain/node"
	pb "github.com/jmsadair/keychain/proto/pbcoordinator"
)

// ReadChainConfigurationRequest is a request to read the chain configuration.
type ReadChainConfigurationRequest struct{}

// Proto converts a ReadChainConfigurationRequest to its protobuf message equivalent.
func (r *ReadChainConfigurationRequest) Proto() *pb.ReadChainConfigurationRequest {
	return &pb.ReadChainConfigurationRequest{}
}

// FromProto converts a ReadChainConfigurationRequest protobuf message to a ReadChainConfigurationRequest.
func (r *ReadChainConfigurationRequest) FromProto(pbReq *pb.ReadChainConfigurationRequest) {}

// ReadChainConfigurationResponse is a response to a request to read the chain configuration.
type ReadChainConfigurationResponse struct {
	Configuration *chainnode.Configuration
}

// Proto converts a ReadChainConfigurationResponse to its protobuf message equivalent.
func (r *ReadChainConfigurationResponse) Proto() *pb.ReadChainConfigurationResponse {
	return &pb.ReadChainConfigurationResponse{Configuration: r.Configuration.Proto()}
}

// FromProto converts a ReadChainConfigurationResponse protobuf message to a ReadChainConfigurationResponse.
func (r *ReadChainConfigurationResponse) FromProto(pbReq *pb.ReadChainConfigurationResponse) {
	r.Configuration = chainnode.NewConfigurationFromProto(pbReq.GetConfiguration())
}
