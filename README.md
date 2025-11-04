# KeychainDB 
keychainDB is a distributed key-value storage system that provides fault tolerance, strong consistency, and high read throughput through Chain Replication with Apportioned Queries (CRAQ).

## Project Status
This project is currently under active development and is **not** yet ready for production use. There are many features that are missing and it is quite possible there are bugs that have yet to be discovered.

## Key Features
- **Strong consistency:** All writes are serialized through the chain head to ensure consistent state.
- **Fault tolerance:** Replication across multiple nodes ensures data durability and availability.
- **High read throughput:** Standard chain replication requires that reads are only served by the tail of the chain, which can become a bottleneck. This implementation uses CRAQ, which enables serving read requests from any node in the chain.
- **Dynamic chain membership:** Nodes can be added or removed without downtime through the Raft-based coordinator.
