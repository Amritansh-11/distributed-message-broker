# Distributed Message Broker — Milestone 2

A custom, lightweight, TCP-based distributed message broker built from scratch in Node.js without third-party messaging libraries or HTTP frameworks.

## Milestone 2 Architecture: Robust Message Protocol & TCP Framing

Milestone 2 enhances the TCP communication layer with a decoupled, multi-layer protocol processing pipeline:

```
Producer (TCP Client)
    ↓
TCP Connection
    ↓
Message Framing (StreamFramer)
    ↓
Protocol Encoder/Decoder (ProtocolCodec)
    ↓
Request Validation (RequestValidator)
    ↓
Broker (MessageBroker Core Engine)
    ↓
Protocol Response (ProtocolEncoder)
    ↓
Message Framing (StreamFramer)
    ↓
TCP Connection
    ↓
Consumer (TCP Client)
```

- **Producer**: Connects over TCP, encodes structured `PRODUCE` request via codec, receives `PRODUCE_ACK`.
- **Broker**: Decouples TCP handling, framing (`StreamFramer`), decoding (`ProtocolDecoder`), validation (`RequestValidator`), and domain logic (`MessageBroker`). Maintains an in-memory queue (`messages = []`).
- **Consumer**: Connects over TCP, encodes `CONSUME` request via codec, receives next available `MESSAGE` or `NO_MESSAGES` if queue is empty.

---

## TCP & Message Framing

TCP is a stream-oriented transport protocol (`OSI Layer 4`). It treats transmitted data as a continuous byte stream with no intrinsic record boundaries. One `socket.write()` from a client may arrive across multiple `data` events at the server, or multiple `socket.write()` calls may be coalesced into a single `data` chunk.

To delineate discrete application messages, this broker implements a **StreamFramer** with Line-Delimited JSON (NDJSON) and max-frame guardrails:
- Every wire message is formatted as a single JSON object terminated by a newline (`\n` or `\r\n`).
- Framing layer accumulates bytes in a connection buffer, splits chunks on newline boundaries, and enforces maximum frame limits (default 1 MB) to prevent buffer overflow attacks.

### Protocol Wire Examples

#### PING / PONG
- **Client sends**:
  ```json
  {"type":"PING"}
  ```
- **Broker responds**:
  ```json
  {"type":"PONG","success":true}
  ```

#### PRODUCE
- **Client sends**:
  ```json
  {"type":"PRODUCE","message":"Hello Distributed Systems"}
  ```
- **Broker responds**:
  ```json
  {"type":"PRODUCE_ACK","success":true}
  ```

#### CONSUME
- **Client sends**:
  ```json
  {"type":"CONSUME"}
  ```
- **Broker responds (when message available)**:
  ```json
  {"type":"MESSAGE","success":true,"message":"Hello Distributed Systems"}
  ```
- **Broker responds (when queue empty)**:
  ```json
  {"type":"NO_MESSAGES","success":true}
  ```

#### Malformed or Invalid Request
- **Broker responds**:
  ```json
  {"type":"ERROR","success":false,"error":"PRODUCE request must include a string \"message\""}
  ```

---

## Getting Started & Installation

### Prerequisites
- Node.js (v18 or higher recommended)

### Running the Components

1. **Start the Broker Server**:
   ```bash
   node src/index.js
   ```
   *Listens on TCP port 5000.*

2. **Publish a Message via Producer**:
   ```bash
   node src/producer/producer.js
   ```
   *Sends "Hello Distributed Systems" and prints PRODUCE_ACK.*

3. **Fetch a Message via Consumer**:
   ```bash
   node src/consumer/consumer.js
   ```
   *Retrieves "Hello Distributed Systems". Running it again yields NO_MESSAGES.*

4. **Run Automated Test Suite**:
   ```bash
   node tests/framing.test.js
   node tests/codec.test.js
   node tests/broker.test.js
   node tests/protocol.test.js
   ```

---

## Milestone 2 Components

- [framing.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/protocol/framing.js) — StreamFramer: TCP buffer accumulation, chunk splitting, and frame size safety enforcement.
- [codec.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/protocol/codec.js) — ProtocolDecoder & ProtocolEncoder: Wire payload serialization & deserialization.
- [validator.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/protocol/validator.js) — RequestValidator: Request schema and parameter boundary validation.
- [types.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/protocol/types.js) — Protocol constants and object builders.
- [broker.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/broker/broker.js) — MessageBroker domain engine.
- [server.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/broker/server.js) — BrokerServer TCP transport listener.
