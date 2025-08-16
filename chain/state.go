package chain

type Status int

const (
	Syncing Status = iota
	Active
	Inactive
)

type State struct {
	config *ChainConfiguration
	status Status
}
