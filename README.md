# KeychainDB
KeychainDB is a distributed key-value storage system that provides fault tolerance, strong consistency, and high read throughput through *Chain Replication with Apportioned Queries (CRAQ)*.

## Project Status
This project is currently under active development and is **not** yet ready for production use. Many features are still missing, and it is quite possible that there are bugs that have yet to be discovered.

## Key Features
### Consistency and Performance
- **Strong consistency:** All writes are serialized through the chain head to ensure a consistent state.
- **High read throughput:** Standard chain replication requires that reads are only served by the tail of the chain, which can become a bottleneck. This implementation uses CRAQ, which enables serving reads from any node in the chain.

### Resilience
- **Fault tolerance:** Replication across multiple nodes ensures data durability and availability. A chain can continue to serve reads and writes as long as at least one node remains available.
- **Dynamic chain membership:** Nodes can be added or removed without downtime through a Raft-based coordinator.

### Simplicity
- **Simple API:** Both an HTTP and an RPC interface are exposed for interacting with the coordinator cluster and performing operations against the chain. The proxy layer handles routing transparently, so clients do not need to be aware of the internal chain topology.

## Architecture
KeychainDB consists of three components.
```
                                                 +----------------------+
                                                 |      Clients         |
                                                 | (Read/Write Requests)|
                                                 +----------+-----------+
                                                            |
                                                            v
                                                 +----------------------+
                                                 |       Proxies        |
                                                 | (HTTP/RPC Gateway)   |
                                                 +----+-----------+-----+
                                                      |           |
                                                      |           |
                                      +---------------+           +----------------+
                                      |                                            |
                                      v                                            v
                            +---------------------+                      +--------------------+
                            |  Coordinator        |                      |     Chain Nodes    |
                            |  (Raft Cluster)     |                      | (Head → ... → Tail)|
                            |---------------------|                      +--------------------+
                            | - Manages chain     |                               ^
                            |   membership        |                               |
                            | - Heartbeats nodes  |                               |
                            | - Detects failures  |                               |
                            +---------+-----------+                               |
                                      |                                           |
                                      | Membership & Health Info                  |
                                      |                                           |
                                      +-------------------------------------------+
```
### Chain Nodes
Chain nodes are responsible for storing key-value pairs and are organized into a sequential chain. Writes are always initiated at the head node, which writes the key-value pair to disk and forwards the request to its successor. Each node repeats this process until the request reaches the tail, which finalizes the write by committing it and sending a commit acknowledgment back up the chain. Once the commit propagates to all nodes, the key-value pair becomes visible to readers.

### Coordinator Cluster
The coordinator is a Raft cluster that manages chain membership and health. Clients use the coordinator to add or remove nodes from a chain, and the coordinator periodically checks node liveness through heartbeats.  
If a node fails to respond within a configured timeout, the coordinator automatically reconfigures the chain to exclude it, ensuring the system can continue serving requests with minimal interruption.

### Proxies
Proxy nodes act as intermediaries between clients and the chain. When a client sends a request, the proxy determines the appropriate chain node to handle it and forwards the request accordingly.  
This allows clients to interact with the system through a single, simplified interface without needing to know the chain’s internal structure.

# Future Work
This a non-exhaustive list of things that need to be done in the near term.
- [ ] Leader forwarding. Coordinators should forward client requests to the leader.
- [ ] More testing. There needs to be more thorough integration and E2E tests. Especially for various failure scenarios.
- [ ] A `cmd` directory for with the logic for running the coordinator, chain nodes, and proxy nodes.
- [ ] Various package documentation.
