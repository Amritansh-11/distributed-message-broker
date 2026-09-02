# Distributed Message Broker — Milestone 6: Persistent Message Storage

A custom, lightweight, TCP-based distributed message broker built from scratch in Node.js without third-party messaging libraries or HTTP frameworks.

---

## Milestone 6 Architecture: Persistent Storage Engine

Milestone 6 introduces **disk persistence** for broker message logs and committed consumer group offsets. Messages and offset metadata survive broker restarts and process crashes while keeping storage concerns strictly isolated behind a clean `StorageEngine` abstraction.

```
Broker
 ├── StorageEngine (dataDir: ./data)
 │    ├── topics/
 │    │    ├── orders/
 │    │    │    ├── partition-0/
 │    │    │    │    ├── 000000000000.log
 │    │    │    │    └── 000000001000.log (Segment Rollover)
 │    │    │    ├── partition-1/
 │    │    │    └── partition-2/
 │    │    └── payments/
 │    └── consumer-groups/
 │         ├── order-workers.json
 │         └── analytics-workers.json
 │
 ├── TopicManager (In-Memory Index & State)
 └── ConsumerGroupManager (In-Memory Membership & Offsets)
```

---

## Persistent Storage Features & Guarantees

### 1. Dedicated Storage Engine Abstraction
- All filesystem I/O operations are isolated within `StorageEngine` (`src/storage/storage-engine.js`), `LogSegment` (`src/storage/segment.js`), and `RecordFormat` (`src/storage/record-format.js`).
- `MessageBroker` and `BrokerServer` delegate storage persistence during `PRODUCE`, `CREATE_TOPIC`, `COMMIT_OFFSET`, and startup recovery.

### 2. Robust Length-Prefixed Binary Record Format
Each record written to a `.log` segment file has the format:
```
+-------------------+---------------------------------------------+-----+
| Length (4 Bytes)  | Payload JSON String                         | \n  |
| Big-Endian Uint32 | {"offset": 0, "message": "...", "crc": 123} | 1B  |
+-------------------+---------------------------------------------+-----+
```
- **Length**: 4-byte unsigned integer specifying exact byte length of JSON payload.
- **Payload**: JSON containing `offset`, `message`, and `crc` checksum.
- **`\n`**: Trailing newline byte delimiter for boundary verification.

### 3. Log Segment Rollover
- Partition logs are split into multiple segment files (e.g. `000000000000.log`, `000000001000.log`).
- Base filename represents the starting offset of the segment padded to 12 digits.
- Automatically rolls over to a new segment file when `maxMessagesPerSegment` limit is reached.

### 4. Crash Recovery & Tail Truncation
- On broker startup (`server.start()`), `StorageEngine.recoverAllState()` scans all topic partitions and log segments in numerical filename order.
- Reconstructs in-memory `Partition` message streams and offset counters.
- If an **incomplete final record** is detected at EOF (e.g. from abrupt power loss), the broker safely truncates the segment at the boundary of the last valid record and preserves all preceding records intact.
- Rejects startup if mid-file record corruption or invalid checksum is detected.

### 5. Consumer Group Offset Persistence
- Committed offsets for consumer groups are saved to `data/consumer-groups/<groupId>.json`.
- Restores committed offset checkpoints on restart so consumers resume processing from their exact committed position.

---

## Wire Protocol Summary

- `PRODUCE`: Returns `PRODUCE_ACK` with `topic`, `partition`, and assigned `offset` after persisting record to disk.
- `CONSUME`: Reads message by explicit offset or group position from in-memory stream without deleting persisted record.
- `JOIN_GROUP`: Registers consumer and returns partition assignments.
- `LEAVE_GROUP`: Unregisters consumer and triggers group partition rebalance.
- `COMMIT_OFFSET`: Saves committed offset checkpoint to memory and disk file.
- `GET_GROUP_INFO`: Returns group metadata, consumer assignments, and committed offsets.

---

## CLI & Test Execution

1. **Start Broker Server**:
   ```bash
   npm run broker
   ```

2. **Run Full Automated Test Suite**:
   ```bash
   npm test
   ```

3. **Run Milestone 6 Persistent Storage Tests**:
   ```bash
   npm run test:storage
   ```

---

## Core Components Architecture

- [storage-engine.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/storage/storage-engine.js) — `StorageEngine`: Root storage coordinator.
- [segment.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/storage/segment.js) — `LogSegment`: Appends records and manages rollover.
- [record-format.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/storage/record-format.js) — `RecordFormat`: Binary framing, CRC32, & truncation.
- [partition.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/broker/partition.js) — `Partition`: Pure domain in-memory log stream.
- [consumer-group-manager.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/broker/consumer-group-manager.js) — `ConsumerGroupManager`: Consumer group state manager.
- [server.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/broker/server.js) — `BrokerServer`: TCP socket server executing startup recovery.
