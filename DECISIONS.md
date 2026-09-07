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
- **Decision**: Keep domain entities (`MessageBroker`, `TopicManager`, `Topic`, `Partition`, `ConsumerGroupManager`, `StorageEngine`, `ClusterManager`, `ReplicationManager`, `LeaderElectionManager`) pure.

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
- **Decision**: Implement single-leader partition replication in `ReplicationManager`.

---

## ADR 010: Failure Handling & Deterministic Leader Election
- **Status**: Accepted
- **Context**: Handle broker and partition leader failures by automatically electing a new leader among surviving replicas without data loss or split-brain inconsistencies.
- **Decision**: Implement `LeaderElectionManager` with deterministic ranking.
- **Candidate Ranking**:
  1. Primary: Highest replicated / local partition log offset.
  2. Secondary: Alphabetical `brokerId` ascending (`'broker-2'` < `'broker-3'`).
- **Leader Epoch**: Increment `leaderEpoch` on every election. Reject requests with stale epochs (`STALE_LEADER` / `NOT_LEADER`).
- **Partition Unavailable**: Set partition status to `NO_LEADER` if no candidates are `ALIVE`. Return `PARTITION_UNAVAILABLE`.
- **Old Leader Rejoin**: Returning broker rejoins as follower and catches up via `REPLICA_SYNC`.
- **Scope Limits**: Intentionally defer Raft/Paxos/ZooKeeper consensus to maintain deterministic, educational implementation.
