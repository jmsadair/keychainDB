package http

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type ErrorCode string

const (
	CodeInvalidKey       ErrorCode = "invalid_key"
	CodeMethodNotAllowed ErrorCode = "method_not_allowed"
	CodeInvalidJSON      ErrorCode = "invalid_json"
	CodeInternalError    ErrorCode = "internal_error"
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
	ErrMethodNotAllowed = &APIError{
		Status:  http.StatusMethodNotAllowed,
		Code:    CodeMethodNotAllowed,
		Message: "Method not allowed",
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

	err := s.Proxy.SetValue(r.Context(), req.Key, req.Value)

	if err != nil {
		writeAPIError(w, &APIError{
			Status:  http.StatusInternalServerError,
			Code:    "internal_error",
			Message: "An unexpected error occurred",
		})
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

	value, err := s.Proxy.GetValue(r.Context(), req.Key)
	if err != nil {
		writeAPIError(w, &APIError{
			Status:  http.StatusInternalServerError,
			Code:    "internal_error",
			Message: "An unexpected error occurred",
		})
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(value)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
