# Distributed Message Broker — Milestone 3

A custom, lightweight, TCP-based distributed message broker built from scratch in Node.js without third-party messaging libraries or HTTP frameworks.

## Milestone 3 Architecture: Topics & Message Routing

Milestone 3 introduces **Topics** into the message broker architecture. Rather than routing all published messages into a single global queue, messages are explicitly published to and consumed from dedicated, named logical channels called **topics**.

```
Producer (A)              Producer (B)
    ↓                         ↓
PRODUCE topic="orders"     PRODUCE topic="payments"
    ↓                         ↓
TCP Connection            TCP Connection
    ↓                         ↓
StreamFramer → Decoder → Validator → MessageBroker
                                           ↓
                                     TopicManager
                                     ├── Map {
                                     │     "orders"   => Topic Queue [FIFO]
                                     │     "payments" => Topic Queue [FIFO]
                                     │   }
                                           ↓
                                Consumer (per topic)
```

### What is a Topic?
A topic is a logical named stream of messages (e.g. `orders`, `payments`, `notifications`, `logs`). 
- **Logical Message Isolation**: Messages published to `orders` remain completely isolated from messages published to `payments`.
- **Topic-Specific FIFO Queues**: Each topic maintains its own independent in-memory FIFO queue.
- **Explicit Topic Creation**: Topics must be created explicitly via `CREATE_TOPIC` before producers can publish or consumers can consume. Attempting to access an unknown topic returns a structured `TOPIC_NOT_FOUND` error.

---

## Topic Name Rules
- Must be a string
- Must not be empty
- Maximum length: 100 characters
- Allowed characters: alphanumeric (`a-z`, `A-Z`, `0-9`), hyphen (`-`), underscore (`_`), dot (`.`)
- Valid examples: `orders`, `payments-v1`, `user_events`, `order.created`, `logs_2026`
- Invalid examples: `""`, `"topic with spaces"`, `"topic/with/slashes"`

---

## Wire Protocol Examples

#### 1. CREATE_TOPIC
- **Request**:
  ```json
  {
    "requestId": "req-101",
    "type": "CREATE_TOPIC",
    "payload": {
      "topic": "orders"
    }
  }
  ```
- **Response**:
  ```json
  {
    "requestId": "req-101",
    "type": "CREATE_TOPIC_ACK",
    "success": true,
    "payload": {
      "topic": "orders"
    }
  }
  ```

#### 2. LIST_TOPICS
- **Request**:
  ```json
  {
    "requestId": "req-102",
    "type": "LIST_TOPICS",
    "payload": {}
  }
  ```
- **Response**:
  ```json
  {
    "requestId": "req-102",
    "type": "TOPICS",
    "success": true,
    "payload": {
      "topics": ["orders", "payments"]
    }
  }
  ```

#### 3. PRODUCE
- **Request**:
  ```json
  {
    "requestId": "req-103",
    "type": "PRODUCE",
    "payload": {
      "topic": "orders",
      "message": "Order 1001 created"
    }
  }
  ```
- **Response**:
  ```json
  {
    "requestId": "req-103",
    "type": "PRODUCE_ACK",
    "success": true,
    "payload": {
      "topic": "orders"
    }
  }
  ```

#### 4. CONSUME
- **Request**:
  ```json
  {
    "requestId": "req-104",
    "type": "CONSUME",
    "payload": {
      "topic": "orders"
    }
  }
  ```
- **Response (Message Available)**:
  ```json
  {
    "requestId": "req-104",
    "type": "MESSAGE",
    "success": true,
    "payload": {
      "topic": "orders",
      "message": "Order 1001 created"
    }
  }
  ```
- **Response (Queue Empty)**:
  ```json
  {
    "requestId": "req-104",
    "type": "NO_MESSAGES",
    "success": true,
    "payload": {
      "topic": "orders"
    }
  }
  ```

#### 5. Unknown Topic Error
- **Response**:
  ```json
  {
    "requestId": "req-105",
    "type": "ERROR",
    "success": false,
    "error": {
      "code": "TOPIC_NOT_FOUND",
      "message": "Topic 'unknown' does not exist"
    }
  }
  ```

---

## CLI Usage & Commands

1. **Start the Broker Server**:
   ```bash
   npm run broker
   ```
   *Listens on TCP port 5000.*

2. **Create a Topic**:
   ```bash
   npm run topic:create -- orders
   npm run topic:create -- payments
   ```

3. **List Active Topics**:
   ```bash
   npm run topic:list
   ```

4. **Produce Message to a Topic**:
   ```bash
   npm run producer -- orders "Order 1001 Created"
   npm run producer -- payments "Payment 5001 Processed"
   ```

5. **Consume Message from a Topic**:
   ```bash
   npm run consumer -- orders
   npm run consumer -- payments
   ```

6. **Run Full Test Suite**:
   ```bash
   npm test
   ```

---

## Core Components

- [topic-manager.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/broker/topic-manager.js) — TopicManager: Pure domain manager for topic creation, validation, metadata, and isolated FIFO queues.
- [broker.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/broker/broker.js) — MessageBroker domain orchestrator delegating topic operations.
- [server.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/broker/server.js) — TCP Socket listener handling client connections.
- [framing.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/protocol/framing.js) — StreamFramer: TCP packet chunking and delimiter framing.
- [codec.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/protocol/codec.js) — ProtocolDecoder & ProtocolEncoder: Wire payload serialization.
- [validator.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/protocol/validator.js) — RequestValidator: Request schema and topic validation.
- [topic-create.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/cli/topic-create.js) — CLI helper for topic creation.
- [topic-list.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/cli/topic-list.js) — CLI helper for topic listing.

