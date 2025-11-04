# KeychainDB 
KeychainDB is a distributed key-value storage system that provides fault tolerance, strong consistency, and high read throughput via chain replication with apportioned queries (CRAQ).

## Architecture
KeychaiDB consists of three main components:

### Chain Nodes
These nodes are responsible for storing key-value pairs. All writes are initated through the head of chain and are replicated across the entire chain. Reads may be directed to any node of the chain.

### Coordinator
The coordinator is a Raft cluster responsible for managing the chain membership configuration. Client requests for adding and removing chain nodes are executed through the coordinator.
The coordinator ensures that the chain nodes are alive through periodic heartbeats. If the coordinator is unable to contact a chain node, it will remove it from the chain.

### Proxies
Proxies are stateless client facing services that determine which chain nodes client requests should be routed to. It manages communication with the coordinator so that clients do not have to.
