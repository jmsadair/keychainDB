package node

import (
	"errors"

	"github.com/hashicorp/raft"
)

var (
	ErrNodeExists     = errors.New("raft: node already exists with same ID or address")
	ErrLeadershipLost = errors.New("raft: node lost leadership")
	ErrNotLeader      = errors.New("raft: node is not the leader")
	ErrLeader         = errors.New("raft: node is the leader")
	ErrEnqueueTimeout = errors.New("raft: enqueue timed out")
)

func handleError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, raft.ErrNotLeader):
		return ErrNotLeader
	case errors.Is(err, raft.ErrLeadershipLost):
		return ErrLeadershipLost
	case errors.Is(err, raft.ErrLeader):
		return ErrLeader
	case errors.Is(err, raft.ErrEnqueueTimeout):
		return ErrEnqueueTimeout
	}

	return err
}
