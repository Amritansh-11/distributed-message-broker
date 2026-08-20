# Distributed Message Broker — Milestone 1

A custom, lightweight, TCP-based distributed message broker built from scratch in Node.js without third-party messaging libraries or HTTP frameworks.

## Architecture

```
Producer (TCP Client) ---> TCP (Port 5000) ---> Broker (TCP Server) ---> TCP (Port 5000) ---> Consumer (TCP Client)
                                                   |
                                           [ In-Memory Queue ]
```

- **Producer**: Connects over TCP, sends `PRODUCE` request containing payload string, receives `PRODUCE_ACK`.
- **Broker**: Maintains an in-memory queue (`messages = []`), processes incoming framing, routes commands, queues produced messages, and delivers them to consumers on request.
- **Consumer**: Connects over TCP, sends `CONSUME` request, receives next available `MESSAGE` or `NO_MESSAGES` if queue is empty.

## TCP & Message Framing (NDJSON)

TCP is a stream-oriented transport protocol (`Layer 4` OSI). It treats transmitted data as a continuous byte stream with no intrinsic record boundaries. One `socket.write()` from a client may arrive across multiple `data` events at the server, or multiple `socket.write()` calls may be coalesced into a single `data` chunk.

To delineate discrete application messages, this broker implements **Line-Delimited JSON (NDJSON)**:
- Every wire message is formatted as a single JSON object terminated by a newline (`\n` or `\r\n`).
- Framing layer accumulates bytes in a connection buffer, splits chunks on newline boundaries, and parses each line into JSON.

### Protocol Wire Examples

#### PING / PONG
- **Client sends**:
  ```json
  {"type":"PING"}
  ```
- **Broker responds**:
  ```json
  {"type":"PONG"}
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

#### Malformed or Unknown Request
- **Broker responds**:
  ```json
  {"type":"ERROR","success":false,"error":"Malformed JSON format"}
  ```

---

## Getting Started & Installation

### Prerequisites
- Node.js (v18 or higher recommended)

### Running the Components

1. **Start the Broker Server**:
   ```bash
   npm run broker
   ```
   *Listens on TCP port 5000.*

2. **Publish a Message via Producer**:
   ```bash
   npm run producer
   ```
   *Sends "Hello Distributed Systems" and prints PRODUCE_ACK.*

3. **Fetch a Message via Consumer**:
   ```bash
   npm run consumer
   ```
   *Retrieves "Hello Distributed Systems". Running it again yields NO_MESSAGES.*

4. **Run Automated Tests**:
   ```bash
   npm test
   ```

---

## Current Milestone 1 Limitations

- **In-Memory Storage**: Messages are stored in a JavaScript array (`messages = []`). Stopping or restarting the broker clears all unconsumed messages.
- **Single Node**: Operates on a single TCP server without replication or high availability.
- **Simple Queue Mechanics**: Messages are destroyed upon consumption (`messages.shift()`); consumer offsets and topics are not yet introduced.
- **Unauthenticated**: No authentication or TLS encryption on TCP sockets.
