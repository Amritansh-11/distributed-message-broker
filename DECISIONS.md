# Architectural Decision Records (ADR)

## ADR 001: Native Node.js TCP (`net`) Over HTTP / WebSockets
- **Status**: Accepted
- **Context**: The message broker requires maximum throughput, minimal network overhead, precise control over framing, and zero third-party dependencies.
- **Decision**: Use Node.js built-in `net` module to implement custom TCP sockets.

---

## ADR 002: Line-Delimited JSON (NDJSON) Framing Protocol
- **Status**: Accepted
- **Context**: Need a human-readable, simple wire format that supports structured payloads over TCP.
- **Decision**: Use newline `\n` delimited JSON objects as the application wire protocol.

---

## ADR 003: Pure Domain Engine Decoupled From Network Sockets
- **Status**: Accepted
- **Context**: Network protocol logic should remain isolated from message broker queue operations.
- **Decision**: Keep `MessageBroker`, `TopicManager`, `Topic`, `Partition`, `ConsumerGroupManager`, and `StorageEngine` pure domain engines that process plain JS objects.

---

## ADR 004: Explicit Topic Creation Requirement
- **Status**: Accepted
- **Context**: Automatic topic creation can lead to resource leaks and silent typo bugs.
- **Decision**: Require explicit topic creation via `CREATE_TOPIC`. Return `TOPIC_NOT_FOUND` for unknown topics.

---

## ADR 005: Introduce Partitions Before Consumer Groups
- **Status**: Accepted
- **Context**: Need to establish partition data streams before building consumer group assignment algorithms.
- **Decision**: Introduce partitions inside topics before consumer groups.

---

## ADR 006: Non-Destructive Monotonic Offsets & Consumer Groups
- **Status**: Accepted
- **Context**: Destructive queues prevent multi-consumer group reads and historical replays.
- **Decision**: Store messages in an append-only array per partition with monotonically increasing offsets starting at 0.

---

## ADR 007: Persistent Storage Engine with Length-Prefixed Binary Log Segments
- **Status**: Accepted
- **Context**: Messages and committed offsets must survive broker process restarts and power loss crashes.
- **Decision**: Implement `StorageEngine` managing partition log directories (`data/topics/<topic>/partition-<N>/`) and segment files (`000000000000.log`).
- **Record Framing**: Binary length-prefixed records (`[4-byte BE Length][JSON Payload][\n]`) with CRC32 checksums.
- **Crash Recovery**: Automatically detect incomplete final records at EOF during startup recovery and truncate partial tails to preserve valid preceding data.
