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

func startServer(t *testing.T, srv *Server) func() {
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

	config := node.NewConfiguration([]*node.ChainMember{{Address: srv.Node.Address, ID: srv.Node.ID}}, 0)
	require.NoError(t, srv.Node.UpdateConfiguration(context.Background(), &node.UpdateConfigurationRequest{Configuration: config}, &node.UpdateConfigurationResponse{}))
	return func() {
		cancel()
		wg.Wait()
	}
}

func TestSetGet(t *testing.T) {
	srv := makeServer(t)
	cancel := startServer(t, srv)
	defer cancel()

	key := "key-1"
	value := []byte("value-1")
	setURL := fmt.Sprintf("http://%s/set", srv.HTTPServer.Address)
	getURL := fmt.Sprintf("http://%s/get?key=%s", srv.HTTPServer.Address, key)

	setRequest := chainhttp.SetRequest{Key: key, Value: value}
	b, err := json.Marshal(&setRequest)
	require.NoError(t, err)
	setResponse, err := http.Post(setURL, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	defer setResponse.Body.Close()
	require.Equal(t, http.StatusNoContent, setResponse.StatusCode)

	getResponse, err := http.Get(getURL)
	require.NoError(t, err)
	defer getResponse.Body.Close()
	readValue, err := io.ReadAll(getResponse.Body)
	require.NoError(t, err)
	require.Equal(t, value, readValue)
	require.Equal(t, http.StatusOK, getResponse.StatusCode)
}

func TestKeyDoesNotExist(t *testing.T) {
	srv := makeServer(t)
	cancel := startServer(t, srv)
	defer cancel()

	key := "key-1"
	getURL := fmt.Sprintf("http://%s/get?key=%s", srv.HTTPServer.Address, key)

	getResponse, err := http.Get(getURL)
	require.NoError(t, err)
	defer getResponse.Body.Close()
	require.Equal(t, http.StatusNotFound, getResponse.StatusCode)
}

func TestSetInvalidKey(t *testing.T) {
	srv := makeServer(t)
	cancel := startServer(t, srv)
	defer cancel()

	key := ""
	value := []byte("value-1")
	setURL := fmt.Sprintf("http://%s/set", srv.HTTPServer.Address)

	setRequest := chainhttp.SetRequest{Key: key, Value: value}
	b, err := json.Marshal(&setRequest)
	require.NoError(t, err)
	setResponse, err := http.Post(setURL, "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	defer setResponse.Body.Close()
	require.Equal(t, http.StatusBadRequest, setResponse.StatusCode)
}

func TestGetInvalidKey(t *testing.T) {
	srv := makeServer(t)
	cancel := startServer(t, srv)
	defer cancel()

	getURL := fmt.Sprintf("http://%s/get?key=", srv.HTTPServer.Address)

	getResponse, err := http.Get(getURL)
	require.NoError(t, err)
	defer getResponse.Body.Close()
	require.Equal(t, http.StatusBadRequest, getResponse.StatusCode)
}
