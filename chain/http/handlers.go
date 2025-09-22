package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/jmsadair/keychain/chain/node"
	"github.com/jmsadair/keychain/chain/storage"
)

type ErrorCode string

const (
	CodeInvalidKey           ErrorCode = "invalid_key"
	CodeKeyNotFound          ErrorCode = "key_not_found"
	CodeMethodNotAllowed     ErrorCode = "method_not_allowed"
	CodeInvalidConfigVersion ErrorCode = "invalid_config_version"
	CodeInvalidJSON          ErrorCode = "invalid_json"
	CodeInternalError        ErrorCode = "internal_error"
	CodeNotChainMember       ErrorCode = "not_chain_member"
	CodeNotHeadNode          ErrorCode = "not_head_node"
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

var (
	ErrInvalidKey = &APIError{
		Status:  http.StatusBadRequest,
		Code:    CodeInvalidKey,
		Message: "Key must be of length 1 or greater",
	}
	ErrKeyNotFound = &APIError{
		Status:  http.StatusNotFound,
		Code:    CodeKeyNotFound,
		Message: "Key does not exist",
	}
	ErrMethodNotAllowed = &APIError{
		Status:  http.StatusMethodNotAllowed,
		Code:    CodeMethodNotAllowed,
		Message: "Method not allowed",
	}
	ErrInvalidConfigVersion = &APIError{
		Status:  http.StatusConflict,
		Code:    CodeInvalidConfigVersion,
		Message: "Invalid config version",
	}
	ErrNotChainMember = &APIError{
		Status:  http.StatusNotFound,
		Code:    CodeNotChainMember,
		Message: "Not member of chain",
	}
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
		writeAPIError(w, ErrMethodNotAllowed)
		return
	}

	var req SetRequest
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, &APIError{
			Status:  http.StatusBadRequest,
			Code:    "invalid_json",
			Message: "Malformed request body",
		})
		return
	}

	if !keyIsValid(req.Key) {
		writeAPIError(w, ErrInvalidKey)
		return
	}

	err := s.Node.InitiateReplicatedWrite(r.Context(), req.Key, req.Value, req.ConfigVersion)

	var errNotHead *node.ErrNotHead
	switch {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.As(err, &errNotHead):
		http.Redirect(w, r, fmt.Sprintf("http://%s/set", errNotHead.HeadAddr), http.StatusTemporaryRedirect)
	case errors.Is(err, node.ErrInvalidConfigVersion):
		writeAPIError(w, ErrInvalidConfigVersion)
	case errors.Is(err, node.ErrNotMemberOfChain):
		writeAPIError(w, ErrNotChainMember)
	default:
		writeAPIError(w, &APIError{
			Status:  http.StatusInternalServerError,
			Code:    "internal_error",
			Message: "An unexpected error occurred",
		})
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, ErrMethodNotAllowed)
		return
	}

	var req GetRequest
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, &APIError{
			Status:  http.StatusBadRequest,
			Code:    "invalid_json",
			Message: "Malformed request body",
		})
		return
	}

	if !keyIsValid(req.Key) {
		writeAPIError(w, ErrInvalidKey)
		return
	}

	var resp node.ReadResponse
	err := s.Node.Read(r.Context(), &node.ReadRequest{
		Key:           req.Key,
		ConfigVersion: req.ConfigVersion,
	}, &resp)

	switch {
	case err == nil:
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(resp.Value)
	case errors.Is(err, node.ErrInvalidConfigVersion):
		writeAPIError(w, ErrInvalidConfigVersion)
	case errors.Is(err, storage.ErrKeyDoesNotExist):
		writeAPIError(w, ErrKeyNotFound)
	case errors.Is(err, node.ErrNotMemberOfChain):
		writeAPIError(w, ErrNotChainMember)
	default:
		writeAPIError(w, &APIError{
			Status:  http.StatusInternalServerError,
			Code:    "internal_error",
			Message: "An unexpected error occurred",
		})
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
