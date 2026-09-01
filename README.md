# Distributed Message Broker — Milestone 5: Offsets & Consumer Groups

A custom, lightweight, TCP-based distributed message broker built from scratch in Node.js without third-party messaging libraries or HTTP frameworks.

---

## Milestone 5 Architecture: Offsets & Consumer Groups

Milestone 5 introduces **Kafka-like offsets** and **Consumer Groups**. Messages are stored permanently in an append-only log per partition with monotonically increasing offsets. Consumers can read messages by offset or join isolated consumer groups with deterministic partition assignments and in-memory offset tracking.

```
Broker
 ├── orders
 │    ├── partition-0 [Log: (offset 0: A), (offset 1: D)]
 │    ├── partition-1 [Log: (offset 0: B), (offset 1: E)]
 │    └── partition-2 [Log: (offset 0: C)]
 │
 └── ConsumerGroupManager
      ├── "order-workers"
      │    ├── Members: ["consumer-1", "consumer-2"]
      │    ├── Partition Assignments: 
      │    │     consumer-1 -> [partition-0, partition-2]
      │    │     consumer-2 -> [partition-1]
      │    └── Committed Offsets:
      │          partition-0 -> offset 1
      │          partition-1 -> offset 0
      │          partition-2 -> offset 0
      │
      └── "analytics-workers" (Independent offsets & position)
```

---

## Key Concepts & Architecture

### 1. Monotonic Message Offsets
- Every produced message receives a unique integer `offset` starting at 0 and incrementing monotonically ($0, 1, 2, \dots$) within its partition.
- Offsets are unique per partition and independent between partitions.

### 2. Append-Only Partition Log
- Consuming a message **does not delete it** from the broker.
- Messages remain permanently stored in memory, allowing repeated offset reads by multiple consumers and independent groups.

### 3. Consumer Groups & Membership
- **`JOIN_GROUP`**: Registers a consumer (`consumerId`) into a consumer group (`groupId`).
- **`LEAVE_GROUP`**: Unregisters a consumer from a group.
- **Deterministic Rebalancing**: When members join or leave, partition assignments are recalculated round-robin across consumers ordered alphabetically by `consumerId`. Each partition is owned by at most 1 consumer per group.

### 4. Group Offsets & Position Tracking
- **`COMMIT_OFFSET`**: Persists a consumer group's committed offset for a specific topic/partition in memory.
- **Group-Based CONSUME**: Automatically tracks transient read position per consumer group, fetching un-consumed messages and advancing position without auto-committing.

---

## Wire Protocol Examples

#### 1. PRODUCE (With Offset Response)
- **Request**:
  ```json
  {
    "requestId": "req-101",
    "type": "PRODUCE",
    "payload": {
      "topic": "orders",
      "partition": 0,
      "message": "Order Created"
    }
  }
  ```
- **Response**:
  ```json
  {
    "requestId": "req-101",
    "type": "PRODUCE_ACK",
    "success": true,
    "payload": {
      "topic": "orders",
      "partition": 0,
      "offset": 0
    }
  }
  ```

#### 2. CONSUME (By Explicit Offset)
- **Request**:
  ```json
  {
    "requestId": "req-102",
    "type": "CONSUME",
    "payload": {
      "topic": "orders",
      "partition": 0,
      "offset": 0
    }
  }
  ```
- **Response**:
  ```json
  {
    "requestId": "req-102",
    "type": "MESSAGE",
    "success": true,
    "payload": {
      "topic": "orders",
      "partition": 0,
      "offset": 0,
      "message": "Order Created"
    }
  }
  ```

#### 3. JOIN_GROUP
- **Request**:
  ```json
  {
    "requestId": "req-103",
    "type": "JOIN_GROUP",
    "payload": {
      "groupId": "order-workers",
      "consumerId": "consumer-1",
      "topics": ["orders"]
    }
  }
  ```
- **Response**:
  ```json
  {
    "requestId": "req-103",
    "type": "JOIN_GROUP_ACK",
    "success": true,
    "payload": {
      "groupId": "order-workers",
      "consumerId": "consumer-1",
      "assignments": [
        { "topic": "orders", "partition": 0 },
        { "topic": "orders", "partition": 2 }
      ]
    }
  }
  ```

#### 4. GROUP-BASED CONSUME
- **Request**:
  ```json
  {
    "requestId": "req-104",
    "type": "CONSUME",
    "payload": {
      "groupId": "order-workers",
      "consumerId": "consumer-1",
      "topic": "orders"
    }
  }
  ```
- **Response**:
  ```json
  {
    "requestId": "req-104",
    "type": "MESSAGE",
    "success": true,
    "payload": {
      "topic": "orders",
      "partition": 0,
      "offset": 0,
      "message": "Order Created"
    }
  }
  ```

#### 5. COMMIT_OFFSET
- **Request**:
  ```json
  {
    "requestId": "req-105",
    "type": "COMMIT_OFFSET",
    "payload": {
      "groupId": "order-workers",
      "topic": "orders",
      "partition": 0,
      "offset": 0
    }
  }
  ```
- **Response**:
  ```json
  {
    "requestId": "req-105",
    "type": "COMMIT_OFFSET_ACK",
    "success": true,
    "payload": {
      "groupId": "order-workers",
      "topic": "orders",
      "partition": 0,
      "offset": 0
    }
  }
  ```

#### 6. GET_GROUP_INFO
- **Request**:
  ```json
  {
    "requestId": "req-106",
    "type": "GET_GROUP_INFO",
    "payload": {
      "groupId": "order-workers"
    }
  }
  ```
- **Response**:
  ```json
  {
    "requestId": "req-106",
    "type": "GROUP_INFO",
    "success": true,
    "payload": {
      "groupId": "order-workers",
      "consumers": ["consumer-1"],
      "assignments": {
        "consumer-1": [
          { "topic": "orders", "partition": 0 },
          { "topic": "orders", "partition": 1 },
          { "topic": "orders", "partition": 2 }
        ]
      },
      "committedOffsets": [
        { "topic": "orders", "partition": 0, "offset": 0 }
      ]
    }
  }
  ```

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

3. **Run Milestone 5 Offset & Consumer Group Tests**:
   ```bash
   npm run test:offset
   ```

---

## Core Components Architecture

- [partition.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/broker/partition.js) — `Partition`: Append-only log with monotonic offsets.
- [topic.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/broker/topic.js) — `Topic`: Topic entity managing partitions & offset reads.
- [consumer-group.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/broker/consumer-group.js) — `ConsumerGroup`: Group membership & offset state.
- [consumer-group-manager.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/broker/consumer-group-manager.js) — `ConsumerGroupManager`: Rebalancing algorithm & offset commits.
- [broker.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/broker/broker.js) — `MessageBroker`: Request routing coordinator.
