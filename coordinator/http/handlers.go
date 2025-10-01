package http

import (
	"encoding/json"
	"net/http"

	"github.com/jmsadair/keychain/coordinator/node"
)

type JoinClusterRequest struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type RemoveFromClusterRequest struct {
	ID string `json:"id"`
}

type AddChainMemberRequest struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type RemoveChainMemberRequest struct {
	ID string `json:"id"`
}

func (s *Server) handleJoinCluster(w http.ResponseWriter, r *http.Request) {
	var req JoinClusterRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	coordReq := &node.JoinClusterRequest{ID: req.ID, Address: req.Address}
	var coordResp node.JoinClusterResponse
	err = s.Coordinator.JoinCluster(r.Context(), coordReq, &coordResp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) handleRemoveFromCluster(w http.ResponseWriter, r *http.Request) {
	var req RemoveFromClusterRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	coordReq := &node.RemoveFromClusterRequest{ID: req.ID}
	var coordResp node.RemoveFromClusterResponse
	err = s.Coordinator.RemoveFromCluster(r.Context(), coordReq, &coordResp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	coordReq := &node.ClusterStatusRequest{}
	var coordResp node.ClusterStatusResponse
	err := s.Coordinator.ClusterStatus(r.Context(), coordReq, &coordResp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = json.NewEncoder(w).Encode(coordResp.Status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleAddChainMember(w http.ResponseWriter, r *http.Request) {
	var req AddChainMemberRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	coordReq := &node.AddMemberRequest{ID: req.ID, Address: req.Address}
	var coordResp node.AddMemberResponse
	err = s.Coordinator.AddMember(r.Context(), coordReq, &coordResp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) handleRemoveChainMember(w http.ResponseWriter, r *http.Request) {
	var req RemoveChainMemberRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	coordReq := &node.RemoveMemberRequest{ID: req.ID}
	var coordResp node.RemoveMemberResponse
	err = s.Coordinator.RemoveMember(r.Context(), coordReq, &coordResp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) handleChainConfiguration(w http.ResponseWriter, r *http.Request) {
	coordReq := &node.ReadChainConfigurationRequest{}
	var coordResp node.ReadChainConfigurationResponse
	err := s.Coordinator.ReadMembershipConfiguration(r.Context(), coordReq, &coordResp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	members := coordResp.Configuration.Members()
	err = json.NewEncoder(w).Encode(members)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
