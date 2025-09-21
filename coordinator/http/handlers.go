package http

import (
	"encoding/json"
	"net/http"
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

	err = s.Coordinator.Raft.JoinCluster(r.Context(), req.ID, req.Address)
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

	err = s.Coordinator.Raft.RemoveFromCluster(r.Context(), req.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	status, err := s.Coordinator.Raft.ClusterStatus()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = json.NewEncoder(w).Encode(status)
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

	err = s.Coordinator.AddMember(r.Context(), req.ID, req.Address)
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

	err = s.Coordinator.RemoveMember(r.Context(), req.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) handleChainConfiguration(w http.ResponseWriter, r *http.Request) {
	config, err := s.Coordinator.ReadMembershipConfiguration(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	members := config.Members()
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
