package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	chainnode "github.com/jmsadair/keychain/chain/node"
	proxynode "github.com/jmsadair/keychain/proxy/node"
)

type ErrorCode string

const (
	CodeInvalidKey             ErrorCode = "invalid_key"
	CodeMethodNotAllowed       ErrorCode = "method_not_allowed"
	CodeInvalidJSON            ErrorCode = "invalid_json"
	CodeInternalError          ErrorCode = "internal_error"
	CodeKeyNotCommitted        ErrorCode = "not_committed"
	CodeConfigConflict         ErrorCode = "config_conflict"
	CodeNodeNotReady           ErrorCode = "node_not_ready"
	CodeNoMembers              ErrorCode = "no_members"
	CodeCoordinatorUnavailable ErrorCode = "coordinator_unavailable"
)

type APIError struct {
	Status  int       `json:"-"`
	Code    ErrorCode `json:"error"`
	Message string    `json:"message,omitempty"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func writeAPIError(w http.ResponseWriter, err *APIError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(err.Status)
	json.NewEncoder(w).Encode(err)
}

func handleProxyError(w http.ResponseWriter, err error) {
	if err != nil {
		return
	}

	switch {
	case errors.Is(err, chainnode.ErrInvalidConfigVersion):
		writeAPIError(w, ErrConfigConflict)
	case errors.Is(err, chainnode.ErrSyncing):
		writeAPIError(w, ErrNodeNotReady)
	case errors.Is(err, chainnode.ErrNotCommitted):
		writeAPIError(w, ErrKeyNotCommitted)
	case errors.Is(err, proxynode.ErrConfigReadFailure):
		writeAPIError(w, ErrCoordinatorUnavailable)
	case errors.Is(err, proxynode.ErrNoMembers):
		writeAPIError(w, ErrNoMembers)
	default:
		writeAPIError(w, &APIError{
			Status:  http.StatusInternalServerError,
			Code:    "internal_error",
			Message: "An unexpected error occurred",
		})
	}
}

var (
	ErrInvalidJSON = &APIError{
		Status:  http.StatusBadRequest,
		Code:    CodeInvalidJSON,
		Message: "Malformed request body",
	}
	ErrInvalidKey = &APIError{
		Status:  http.StatusBadRequest,
		Code:    CodeInvalidKey,
		Message: "Key must be of length 1 or greater",
	}
	ErrMethodNotAllowed = &APIError{
		Status:  http.StatusMethodNotAllowed,
		Code:    CodeMethodNotAllowed,
		Message: "Method not allowed",
	}
	ErrKeyNotCommitted = &APIError{
		Status:  http.StatusConflict,
		Code:    CodeKeyNotCommitted,
		Message: "Attempted read of uncommitted key, try again",
	}
	ErrConfigConflict = &APIError{
		Status:  http.StatusConflict,
		Code:    CodeConfigConflict,
		Message: "Concurrent chain configuration modifcations, try again",
	}
	ErrNodeNotReady = &APIError{
		Status:  http.StatusServiceUnavailable,
		Code:    CodeNodeNotReady,
		Message: "Node is syncing and is unable to serve reads, try again",
	}
	ErrNoMembers = &APIError{
		Status:  http.StatusServiceUnavailable,
		Code:    CodeNoMembers,
		Message: "No nodes eligible to serve request",
	}
	ErrCoordinatorUnavailable = &APIError{
		Status:  http.StatusServiceUnavailable,
		Code:    CodeCoordinatorUnavailable,
		Message: "Unable to contact coordinator, try again",
	}
)

func keyIsValid(key string) bool {
	return key != ""
}

type SetRequest struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type GetRequest struct {
	Key string `json:"key"`
}

func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, ErrMethodNotAllowed)
		return
	}

	var req SetRequest
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, ErrInvalidJSON)
		return
	}

	if !keyIsValid(req.Key) {
		writeAPIError(w, ErrInvalidKey)
		return
	}

	err := s.Proxy.SetValue(r.Context(), req.Key, req.Value)
	if err != nil {
		handleProxyError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, ErrMethodNotAllowed)
		return
	}

	var req GetRequest
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, ErrInvalidJSON)
		return
	}

	if !keyIsValid(req.Key) {
		writeAPIError(w, ErrInvalidKey)
		return
	}

	value, err := s.Proxy.GetValue(r.Context(), req.Key)
	if err != nil {
		handleProxyError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(value)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
