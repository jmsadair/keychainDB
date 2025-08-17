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

// State contains the membeship configuration and status of a chain node.
type State struct {
	// The membership configuration for a chain node.
	Config *ChainConfiguration
	// The operation status of a chain node.
	Status Status
}
