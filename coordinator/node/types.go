package node

import (
	chainnode "github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/coordinator/raft"
	pb "github.com/jmsadair/keychain/proto/coordinator"
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

// AddMemberRequest is a request to add a member to the chain.
type AddMemberRequest struct {
	ID      string
	Address string
}

// AddMemberResponse is a response to an AddMemberRequest.
type AddMemberResponse struct{}

// RemoveMemberRequest is a request to remove a member from the chain.
type RemoveMemberRequest struct {
	ID string
}

// RemoveMemberResponse is a response to a RemoveMemberRequest.
type RemoveMemberResponse struct{}

// JoinClusterRequest is a request to join the coordinator cluster.
type JoinClusterRequest struct {
	ID      string
	Address string
}

// JoinClusterResponse is a response to a JoinClusterRequest.
type JoinClusterResponse struct{}

// RemoveFromClusterRequest is a request to remove a node from the coordinator cluster.
type RemoveFromClusterRequest struct {
	ID string
}

// RemoveFromClusterResponse is a response to a RemoveFromClusterRequest.
type RemoveFromClusterResponse struct{}

// ClusterStatusRequest is a request to get the coordinator cluster status.
type ClusterStatusRequest struct{}

// ClusterStatusResponse is a response to a ClusterStatusRequest.
type ClusterStatusResponse struct {
	Status raft.Status
}

// Proto converts an AddMemberRequest to its protobuf message equivalent.
func (r *AddMemberRequest) Proto() *pb.AddMemberRequest {
	return &pb.AddMemberRequest{Id: r.ID, Address: r.Address}
}

// FromProto converts an AddMemberRequest protobuf message to an AddMemberRequest.
func (r *AddMemberRequest) FromProto(pbReq *pb.AddMemberRequest) {
	r.ID = pbReq.GetId()
	r.Address = pbReq.GetAddress()
}

// Proto converts an AddMemberResponse to its protobuf message equivalent.
func (r *AddMemberResponse) Proto() *pb.AddMemberResponse {
	return &pb.AddMemberResponse{}
}

// FromProto converts an AddMemberResponse protobuf message to an AddMemberResponse.
func (r *AddMemberResponse) FromProto(pbResp *pb.AddMemberResponse) {
}

// Proto converts a RemoveMemberRequest to its protobuf message equivalent.
func (r *RemoveMemberRequest) Proto() *pb.RemoveMemberRequest {
	return &pb.RemoveMemberRequest{Id: r.ID}
}

// FromProto converts a RemoveMemberRequest protobuf message to a RemoveMemberRequest.
func (r *RemoveMemberRequest) FromProto(pbReq *pb.RemoveMemberRequest) {
	r.ID = pbReq.GetId()
}

// Proto converts a RemoveMemberResponse to its protobuf message equivalent.
func (r *RemoveMemberResponse) Proto() *pb.RemoveMemberResponse {
	return &pb.RemoveMemberResponse{}
}

// FromProto converts a RemoveMemberResponse protobuf message to a RemoveMemberResponse.
func (r *RemoveMemberResponse) FromProto(pbResp *pb.RemoveMemberResponse) {
}

// Proto converts a JoinClusterRequest to its protobuf message equivalent.
func (r *JoinClusterRequest) Proto() *pb.JoinClusterRequest {
	return &pb.JoinClusterRequest{Id: r.ID, Address: r.Address}
}

// FromProto converts a JoinClusterRequest protobuf message to a JoinClusterRequest.
func (r *JoinClusterRequest) FromProto(pbReq *pb.JoinClusterRequest) {
	r.ID = pbReq.GetId()
	r.Address = pbReq.GetAddress()
}

// Proto converts a JoinClusterResponse to its protobuf message equivalent.
func (r *JoinClusterResponse) Proto() *pb.JoinClusterResponse {
	return &pb.JoinClusterResponse{}
}

// FromProto converts a JoinClusterResponse protobuf message to a JoinClusterResponse.
func (r *JoinClusterResponse) FromProto(pbResp *pb.JoinClusterResponse) {
}

// Proto converts a RemoveFromClusterRequest to its protobuf message equivalent.
func (r *RemoveFromClusterRequest) Proto() *pb.RemoveFromClusterRequest {
	return &pb.RemoveFromClusterRequest{Id: r.ID}
}

// FromProto converts a RemoveFromClusterRequest protobuf message to a RemoveFromClusterRequest.
func (r *RemoveFromClusterRequest) FromProto(pbReq *pb.RemoveFromClusterRequest) {
	r.ID = pbReq.GetId()
}

// Proto converts a RemoveFromClusterResponse to its protobuf message equivalent.
func (r *RemoveFromClusterResponse) Proto() *pb.RemoveFromClusterResponse {
	return &pb.RemoveFromClusterResponse{}
}

// FromProto converts a RemoveFromClusterResponse protobuf message to a RemoveFromClusterResponse.
func (r *RemoveFromClusterResponse) FromProto(pbResp *pb.RemoveFromClusterResponse) {
}

// Proto converts a ClusterStatusRequest to its protobuf message equivalent.
func (r *ClusterStatusRequest) Proto() *pb.ClusterStatusRequest {
	return &pb.ClusterStatusRequest{}
}

// FromProto converts a ClusterStatusRequest protobuf message to a ClusterStatusRequest.
func (r *ClusterStatusRequest) FromProto(pbReq *pb.ClusterStatusRequest) {
}

// Proto converts a ClusterStatusResponse to its protobuf message equivalent.
func (r *ClusterStatusResponse) Proto() *pb.ClusterStatusResponse {
	return &pb.ClusterStatusResponse{
		Members: r.Status.Members,
		Leader:  r.Status.Leader,
	}
}

// FromProto converts a ClusterStatusResponse protobuf message to a ClusterStatusResponse.
func (r *ClusterStatusResponse) FromProto(pbResp *pb.ClusterStatusResponse) {
	r.Status = raft.Status{
		Members: pbResp.GetMembers(),
		Leader:  pbResp.GetLeader(),
	}
}
