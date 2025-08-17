package chain

// Status represents the operational status of a chain node.
type Status int

const (
	// Syncing indicates the node is synchronizing with the chain.
	Syncing Status = iota
	// Active indicates the node is actively participating in the chain.
	Active
	// Inactive indicates the node is not participating in any chain.
	Inactive
)

// State holds the complete state information for a chain node, including its configuration and status.
type State struct {
	config *ChainConfiguration
	status Status
}
