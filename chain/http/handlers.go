package http

import (
	"encoding/json"
	"net/http"

	"github.com/jmsadair/keychain/chain/node"
)

type SetRequest struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type GetRequest struct {
	Key string `json:"key"`
}

func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	var req SetRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = s.Node.InitiateReplicatedWrite(r.Context(), req.Key, req.Value)
	if err == node.ErrNotHead {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")

	var req GetRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resp node.ReadResponse
	err = s.Node.Read(r.Context(), &node.ReadRequest{Key: req.Key}, &resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(resp.Value)
}
