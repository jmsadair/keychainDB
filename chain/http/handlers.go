package http

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/chain/storage"
)

var (
	ErrInvalidKey       = errors.New("key must be of length 1 or greater")
	ErrMethodNotAllowed = errors.New("method is not allowed")
)

func keyIsValid(key string) bool {
	return key != ""
}

type SetRequest struct {
	Key           string `json:"key"`
	Value         []byte `json:"value"`
	ConfigVersion uint64 `json:"config_version"`
}

type GetRequest struct {
	Key           string `json:"key"`
	ConfigVersion uint64 `json:"config_version"`
}

func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	var req SetRequest
	defer r.Body.Close()
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if !keyIsValid(req.Key) {
		http.Error(w, ErrInvalidKey.Error(), http.StatusBadRequest)
		return
	}

	err = s.Node.InitiateReplicatedWrite(r.Context(), req.Key, req.Value, req.ConfigVersion)
	switch err {
	case nil:
		w.WriteHeader(http.StatusNoContent)
	case node.ErrInvalidConfigVersion:
	case node.ErrNotHead:
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, ErrMethodNotAllowed.Error(), http.StatusMethodNotAllowed)
		return
	}

	var req GetRequest
	defer r.Body.Close()
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !keyIsValid(req.Key) {
		http.Error(w, ErrInvalidKey.Error(), http.StatusBadRequest)
		return
	}

	var resp node.ReadResponse
	err = s.Node.Read(r.Context(), &node.ReadRequest{Key: req.Key, ConfigVersion: req.ConfigVersion}, &resp)
	switch err {
	case nil:
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(resp.Value)
	case node.ErrInvalidConfigVersion:
		http.Error(w, err.Error(), http.StatusConflict)
	case storage.ErrKeyDoesNotExist:
		http.Error(w, err.Error(), http.StatusNotFound)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
