package chain

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	chainhttp "github.com/jmsadair/keychain/chain/http"
	"github.com/jmsadair/keychain/chain/node"
	"github.com/stretchr/testify/require"
)

func makeServer(t *testing.T) *Server {
	id := "chain-node-1"
	httpAddr := ":8080"
	grpcAddr := ":8081"
	srv, err := NewServer(id, httpAddr, grpcAddr, t.TempDir())
	require.NoError(t, err)
	return srv
}

func startServer(t *testing.T, srv *Server) (*node.Configuration, func()) {
	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		require.NoError(t, srv.Run(ctx))
	}()

	waitForHealthy := func() bool {
		healthURL := fmt.Sprintf("http://%s/healthz", srv.HTTPServer.Address)
		resp, err := http.Get(healthURL)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}
	require.Eventually(t, waitForHealthy, 3*time.Second, 100*time.Millisecond)

	config := node.NewConfiguration([]*node.ChainMember{{Address: srv.Node.Address, ID: srv.Node.ID}}, 1)
	require.NoError(t, srv.Node.UpdateConfiguration(context.Background(), &node.UpdateConfigurationRequest{Configuration: config}, &node.UpdateConfigurationResponse{}))

	return config, func() {
		cancel()
		wg.Wait()
	}
}

func TestSetGet(t *testing.T) {
	srv := makeServer(t)
	config, cancel := startServer(t, srv)
	defer cancel()

	key := "key-1"
	value := []byte("value-1")
	setURL := fmt.Sprintf("http://%s/set", srv.HTTPServer.Address)
	setRequest := chainhttp.SetRequest{Key: key, Value: value, ConfigVersion: config.Version}
	b, err := json.Marshal(&setRequest)
	require.NoError(t, err)

	setResponse, err := http.Post(setURL, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	defer setResponse.Body.Close()
	require.Equal(t, http.StatusNoContent, setResponse.StatusCode)

	getURL := fmt.Sprintf("http://%s/get", srv.HTTPServer.Address)
	b, err = json.Marshal(&chainhttp.GetRequest{Key: key, ConfigVersion: config.Version})
	require.NoError(t, err)
	getRequest, err := http.NewRequest(http.MethodGet, getURL, bytes.NewReader(b))
	require.NoError(t, err)

	getResponse, err := http.DefaultClient.Do(getRequest)
	require.NoError(t, err)
	defer getResponse.Body.Close()
	readValue, err := io.ReadAll(getResponse.Body)
	require.NoError(t, err)
	require.Equal(t, value, readValue)
	require.Equal(t, http.StatusOK, getResponse.StatusCode)
}

func TestKeyDoesNotExist(t *testing.T) {
	srv := makeServer(t)
	config, cancel := startServer(t, srv)
	defer cancel()

	key := "key-1"
	getURL := fmt.Sprintf("http://%s/get", srv.HTTPServer.Address)
	b, err := json.Marshal(&chainhttp.GetRequest{Key: key, ConfigVersion: config.Version})
	require.NoError(t, err)
	getRequest, err := http.NewRequest(http.MethodGet, getURL, bytes.NewReader(b))
	require.NoError(t, err)

	getResponse, err := http.DefaultClient.Do(getRequest)
	require.NoError(t, err)
	defer getResponse.Body.Close()
	var apiErr chainhttp.APIError
	err = json.NewDecoder(getResponse.Body).Decode(&apiErr)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, getResponse.StatusCode)
	require.Equal(t, chainhttp.CodeKeyNotFound, apiErr.Code)
}

func TestSetGetInvalidKey(t *testing.T) {
	srv := makeServer(t)
	config, cancel := startServer(t, srv)
	defer cancel()

	key := ""
	value := []byte("value-1")
	setURL := fmt.Sprintf("http://%s/set", srv.HTTPServer.Address)
	setRequest := chainhttp.SetRequest{Key: key, Value: value, ConfigVersion: config.Version}
	b, err := json.Marshal(&setRequest)
	require.NoError(t, err)

	setResponse, err := http.Post(setURL, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	defer setResponse.Body.Close()
	var apiErr chainhttp.APIError
	err = json.NewDecoder(setResponse.Body).Decode(&apiErr)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, setResponse.StatusCode)
	require.Equal(t, chainhttp.CodeInvalidKey, apiErr.Code)

	getURL := fmt.Sprintf("http://%s/get", srv.HTTPServer.Address)
	b, err = json.Marshal(&chainhttp.GetRequest{ConfigVersion: config.Version})
	require.NoError(t, err)
	getRequest, err := http.NewRequest(http.MethodGet, getURL, bytes.NewReader(b))
	require.NoError(t, err)

	getResponse, err := http.DefaultClient.Do(getRequest)
	require.NoError(t, err)
	defer getResponse.Body.Close()
	apiErr = chainhttp.APIError{}
	err = json.NewDecoder(getResponse.Body).Decode(&apiErr)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, getResponse.StatusCode)
	require.Equal(t, chainhttp.CodeInvalidKey, apiErr.Code)

}

func TestSetGetInvalidConfigVersion(t *testing.T) {
	srv := makeServer(t)
	config, cancel := startServer(t, srv)
	defer cancel()

	key := "key-1"
	value := []byte("value-1")
	setURL := fmt.Sprintf("http://%s/set", srv.HTTPServer.Address)
	invalidSetRequest := chainhttp.SetRequest{Key: key, Value: value, ConfigVersion: config.Version + 1}
	b, err := json.Marshal(&invalidSetRequest)
	require.NoError(t, err)

	invalidSetResponse, err := http.Post(setURL, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	defer invalidSetResponse.Body.Close()
	var apiErr chainhttp.APIError
	err = json.NewDecoder(invalidSetResponse.Body).Decode(&apiErr)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, invalidSetResponse.StatusCode)
	require.Equal(t, chainhttp.CodeInvalidConfigVersion, apiErr.Code)

	validSetRequest := chainhttp.SetRequest{Key: key, Value: value, ConfigVersion: config.Version}
	b, err = json.Marshal(&validSetRequest)
	require.NoError(t, err)
	validSetResponse, err := http.Post(setURL, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	defer validSetResponse.Body.Close()

	getURL := fmt.Sprintf("http://%s/get", srv.HTTPServer.Address)
	b, err = json.Marshal(&chainhttp.GetRequest{Key: key, ConfigVersion: config.Version + 1})
	require.NoError(t, err)
	getRequest, err := http.NewRequest(http.MethodGet, getURL, bytes.NewReader(b))
	require.NoError(t, err)

	getResponse, err := http.DefaultClient.Do(getRequest)
	require.NoError(t, err)
	defer getResponse.Body.Close()
	apiErr = chainhttp.APIError{}
	err = json.NewDecoder(getResponse.Body).Decode(&apiErr)
	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, getResponse.StatusCode)
	require.Equal(t, chainhttp.CodeInvalidConfigVersion, apiErr.Code)
}
