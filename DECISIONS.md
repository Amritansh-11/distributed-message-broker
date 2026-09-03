# Architectural Decision Records (ADR)

## ADR 001: Native Node.js TCP (`net`) Over HTTP / WebSockets
- **Status**: Accepted
- **Decision**: Use Node.js built-in `net` module to implement custom TCP sockets.

---

## ADR 002: Line-Delimited JSON (NDJSON) Framing Protocol
- **Status**: Accepted
- **Decision**: Use newline `\n` delimited JSON objects as the application wire protocol.

---

## ADR 003: Pure Domain Engine Decoupled From Network Sockets
- **Status**: Accepted
- **Decision**: Keep domain entities (`MessageBroker`, `TopicManager`, `Topic`, `Partition`, `ConsumerGroupManager`, `StorageEngine`, `ClusterManager`, `ReplicationManager`) pure.

---

## ADR 004: Explicit Topic Creation Requirement
- **Status**: Accepted
- **Decision**: Require explicit topic creation via `CREATE_TOPIC`. Return `TOPIC_NOT_FOUND` for unknown topics.

---

## ADR 005: Introduce Partitions Before Consumer Groups
- **Status**: Accepted
- **Decision**: Introduce partitions inside topics before consumer groups.

---

## ADR 006: Non-Destructive Monotonic Offsets & Consumer Groups
- **Status**: Accepted
- **Decision**: Store messages in an append-only log per partition with monotonically increasing offsets starting at 0.

---

## ADR 007: Persistent Storage Engine with Length-Prefixed Binary Log Segments
- **Status**: Accepted
- **Decision**: Implement `StorageEngine` managing partition log directories (`data/topics/<topic>/partition-<N>/`) and segment files (`000000000000.log`).

---

## ADR 008: Multi-Broker Cluster Topology & Inter-Broker TCP Communication
- **Status**: Accepted
- **Decision**: Implement `ClusterManager` and `BrokerNode` with static cluster configuration and inter-broker heartbeats.

---

## ADR 009: Partition Replication & Replica Synchronization
- **Status**: Accepted
- **Context**: Ensure messages are replicated across follower brokers to protect against single node data loss.
- **Decision**: Implement single-leader partition replication in `ReplicationManager`.
- **Deterministic Replica Assignment**: `Leader = brokers[p % N]`, `Replicas = brokers[(p + r) % N]`.
- **Leader Write Enforcement**: Produce requests sent to followers return `NOT_LEADER` with the current leader broker ID.
- **Offset Preservation**: Followers persist records at the exact leader-assigned offset via `enqueueWithOffset`.
- **High-Water Mark**: Compute $HWM$ as minimum offset across active caught-up replicas.
- **Catch-Up Synchronization**: Recovering followers send `REPLICA_SYNC` to stream missing log entries from leader.
- **Scope Limits**: Intentionally defer leader election and automatic failover to future milestones.
