package coordinator

import (
	"context"
	"github.com/jmsadair/keychainDB/chain"
	chainpb "github.com/jmsadair/keychainDB/proto/chain"
	pb "github.com/jmsadair/keychainDB/proto/coordinator"
	"github.com/stretchr/testify/mock"
)

type mockRaft struct {
	mock.Mock
}

func (m *mockRaft) JoinCluster(ctx context.Context, id, address string) error {
	return m.MethodCalled("JoinCluster", ctx, id, address).Error(0)
}

func (m *mockRaft) RemoveFromCluster(ctx context.Context, id string) error {
	return m.MethodCalled("RemoveFromCluster", ctx, id).Error(0)
}

func (m *mockRaft) AddMember(ctx context.Context, id, address string) (*chain.Configuration, error) {
	args := m.MethodCalled("AddMember", ctx, id, address)
	return args.Get(0).(*chain.Configuration), args.Error(1)
}

func (m *mockRaft) RemoveMember(ctx context.Context, id string) (*chain.Configuration, *chain.ChainMember, error) {
	args := m.MethodCalled("RemoveMember", ctx, id)
	return args.Get(0).(*chain.Configuration), args.Get(1).(*chain.ChainMember), args.Error(2)
}

func (m *mockRaft) GetMembers(ctx context.Context) (*chain.Configuration, error) {
	args := m.MethodCalled("GetMembers", ctx)
	return args.Get(0).(*chain.Configuration), args.Error(1)
}

func (m *mockRaft) LeaderCh() <-chan bool {
	return m.MethodCalled("LeaderCh").Get(0).(chan bool)
}

func (m *mockRaft) LeaderWithID() (string, string) {
	args := m.MethodCalled("LeaderWithID")
	return args.Get(0).(string), args.Get(1).(string)
}

func (m *mockRaft) ChainConfiguration() *chain.Configuration {
	return m.MethodCalled("ChainConfiguration").Get(0).(*chain.Configuration)
}

func (m *mockRaft) ClusterStatus() (Status, error) {
	args := m.MethodCalled("ClusterStatus")
	return args.Get(0).(Status), args.Error(1)
}

func (m *mockRaft) Shutdown() error {
	args := m.MethodCalled("Shutdown")
	return args.Error(0)
}

type mockChainClient struct {
	mock.Mock
}

func (m *mockChainClient) Ping(ctx context.Context, address string, request *chainpb.PingRequest) (*chainpb.PingResponse, error) {
	args := m.MethodCalled("Ping", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*chainpb.PingResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockChainClient) UpdateConfiguration(ctx context.Context, address string, request *chainpb.UpdateConfigurationRequest) (*chainpb.UpdateConfigurationResponse, error) {
	args := m.MethodCalled("UpdateConfiguration", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*chainpb.UpdateConfigurationResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockChainClient) Close() error {
	args := m.MethodCalled("Close")
	return args.Error(0)
}

type mockCoordinatorClient struct {
	mock.Mock
}

func (m *mockCoordinatorClient) AddMember(ctx context.Context, address string, request *pb.AddMemberRequest) (*pb.AddMemberResponse, error) {
	args := m.MethodCalled("AddMember", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*pb.AddMemberResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockCoordinatorClient) RemoveMember(ctx context.Context, address string, request *pb.RemoveMemberRequest) (*pb.RemoveMemberResponse, error) {
	args := m.MethodCalled("RemoveMember", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*pb.RemoveMemberResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockCoordinatorClient) GetMembers(ctx context.Context, address string, request *pb.GetMembersRequest) (*pb.GetMembersResponse, error) {
	args := m.MethodCalled("GetMembers", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*pb.GetMembersResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockCoordinatorClient) JoinCluster(ctx context.Context, address string, request *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
	args := m.MethodCalled("JoinCluster", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*pb.JoinClusterResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockCoordinatorClient) RemoveFromCluster(ctx context.Context, address string, request *pb.RemoveFromClusterRequest) (*pb.RemoveFromClusterResponse, error) {
	args := m.MethodCalled("RemoveFromCluster", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*pb.RemoveFromClusterResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockCoordinatorClient) ClusterStatus(ctx context.Context, address string, request *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	args := m.MethodCalled("ClusterStatus", ctx, address, request)
	if resp := args.Get(0); resp != nil {
		return resp.(*pb.ClusterStatusResponse), nil
	}
	return nil, args.Error(1)
}

func (m *mockCoordinatorClient) Close() error {
	args := m.MethodCalled("Close")
	return args.Error(0)
}

type mockSnapshotReader struct {
	mock.Mock
}

func (m *mockSnapshotReader) Read(p []byte) (n int, err error) {
	args := m.MethodCalled("Read")
	data := args.Get(0).([]byte)
	return copy(p, data), args.Error(1)
}

func (m *mockSnapshotReader) Close() error {
	args := m.MethodCalled("Close")
	return args.Error(0)
}

type mockSnapshotSync struct {
	mock.Mock
}

func (m *mockSnapshotSync) ID() string {
	args := m.MethodCalled("ID")
	return args.String(0)
}

func (m *mockSnapshotSync) Write(p []byte) (n int, err error) {
	args := m.MethodCalled("Write", p)
	return args.Int(0), args.Error(1)
}

func (m *mockSnapshotSync) Close() error {
	args := m.MethodCalled("Close")
	return args.Error(0)
}

func (m *mockSnapshotSync) Cancel() error {
	args := m.MethodCalled("Cancel")
	return args.Error(0)
}
