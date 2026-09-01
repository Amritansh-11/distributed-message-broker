# Architectural Decision Records (ADR)

## ADR 001: Native Node.js TCP (`net`) Over HTTP / WebSockets
- **Status**: Accepted
- **Context**: The message broker requires maximum throughput, minimal network overhead, precise control over framing, and zero third-party dependencies.
- **Decision**: Use Node.js built-in `net` module to implement custom TCP sockets.
- **Consequences**: Enables low-latency byte stream handling; requires custom message framing.

---

## ADR 002: Line-Delimited JSON (NDJSON) Framing Protocol
- **Status**: Accepted
- **Context**: Need a human-readable, simple wire format that supports structured payloads over TCP.
- **Decision**: Use newline `\n` delimited JSON objects as the application wire protocol.
- **Consequences**: StreamFramer buffers TCP chunks and splits on `\n`. Messages must not contain unescaped raw newlines.

---

## ADR 003: Pure Domain Engine Decoupled From Network Sockets
- **Status**: Accepted
- **Context**: Network protocol logic should remain isolated from message broker queue operations.
- **Decision**: Keep `MessageBroker`, `TopicManager`, `Topic`, `Partition`, and `ConsumerGroupManager` pure domain engines that process plain JS objects.
- **Consequences**: Facilitates unit testing without running active TCP network sockets.

---

## ADR 004: Explicit Topic Creation Requirement
- **Status**: Accepted
- **Context**: Automatic topic creation can lead to resource leaks and silent typo bugs.
- **Decision**: Require explicit topic creation via `CREATE_TOPIC`. Return `TOPIC_NOT_FOUND` for unknown topics.
- **Consequences**: Producers and consumers must interact with existing topics, preventing garbage topics.

---

## ADR 005: Introduce Partitions Before Consumer Groups
- **Status**: Accepted
- **Context**: We need to determine whether to build consumer groups or partitions first.
- **Decision**: Introduce partitions inside topics before consumer groups.
- **Reason**: Partitions provide the fundamental underlying data structure for parallel message streams, isolated FIFO queues, and key-based routing. Consumer groups, offsets, and partition assignment protocols logically depend on the existence of partitions.
- **Consequences**: Topics contain `Map<number, Partition>` with default 3 partitions. Enables key-based and round-robin partition routing.

---

## ADR 006: Non-Destructive Monotonic Offsets & Consumer Groups
- **Status**: Accepted
- **Context**: Destructive queues where messages are deleted upon consumption prevent multi-consumer group reads and replayability.
- **Decision**: Store messages in an append-only array per partition with monotonically increasing offsets starting at 0. Maintain consumer groups and offsets in `ConsumerGroupManager`.
- **Reason**: Enables multiple independent consumer groups to read the same partition stream at their own pace without deleting data. Allows replaying historical data from offset 0.
- **Consequences**: Messages remain in memory; offset tracking is managed per consumer group via explicit `COMMIT_OFFSET` and auto-advancing read positions.
