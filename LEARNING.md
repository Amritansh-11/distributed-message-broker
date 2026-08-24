# Key System Architecture & TCP Learnings — Milestone 1 & 2

## 1. What is TCP?
Transmission Control Protocol (TCP) is a core transport layer protocol (OSI Layer 4) that provides reliable, ordered, and error-checked delivery of a stream of octets (bytes) between applications running on hosts communicating via an IP network.

Unlike UDP (User Datagram Protocol), which sends independent datagram packets, TCP establishes a logical connection between two endpoints before exchanging data.

---

## 2. What is a TCP Socket?
A TCP Socket is an endpoint abstraction provided by the operating system (combining an IP address and a Port number). In Node.js (`net.Socket`), a socket is an event-driven `Duplex Stream`. It allows bidirectional communication:
- Reading data from the remote peer (`socket.on('data', chunk)`)
- Writing data to the remote peer (`socket.write(data)`)

---

## 3. TCP Client vs Server
- **TCP Server (`net.createServer`)**: Listens on a designated local port (e.g. `5000`), binds to an interface (`127.0.0.1`), and accepts incoming TCP connection requests from clients.
- **TCP Client (`net.createConnection`)**: Initiates a 3-Way Handshake (`SYN` -> `SYN-ACK` -> `ACK`) with a remote listening server address and port to establish a socket connection.

---

## 4. Why is TCP a Byte Stream?
TCP operates at the byte level. It does **not** understand application level messages, JSON objects, or line boundaries. 

When an application calls `socket.write(data)`, TCP breaks the payload down into segments according to its Maximum Segment Size (MSS) and network conditions.
- **Packet Fragmentation**: A single large message sent by a producer may be split into multiple `data` events at the receiver.
- **Packet Coalescing (Nagle's Algorithm)**: Multiple small messages sent sequentially may be combined into a single TCP segment and arrive in a single `data` event at the receiver.

Therefore: **One `data` event in Node.js does NOT equal one application message.**

---

## 5. Why Message Framing is Necessary
Because TCP does not enforce message boundaries, the receiving application must implement **Message Framing** at OSI Layer 7 (Application Layer).

Without framing:
- Parsing `JSON.parse(chunk)` directly inside `socket.on('data')` will crash as soon as a partial JSON fragment or multiple JSON objects glued together arrive in `chunk`.

Message framing guarantees that the receiver can reliably reconstruct exact application messages from fragmented byte streams before attempting to process or parse them.

---

## 6. Why We Chose Line-Delimited Framing (NDJSON)
- **Simplicity**: Every payload is serialized as JSON followed by a newline delimiter (`\n`).
- **Human Readability & Debuggability**: Developers can test the server using standard terminal networking utilities like `netcat`, `telnet`, or PowerShell raw sockets.
- **Decoupled Architecture**: `StreamFramer` handles buffering and splitting. The core `MessageBroker` engine only receives fully parsed JavaScript objects.

---

## 7. Milestone 2: Multi-Layer Protocol Pipeline
In Milestone 2, we separated concerns into distinct pipeline layers:

1. **StreamFramer (`src/protocol/framing.js`)**: Buffers TCP chunks, extracts complete line frames, enforces frame size limits (1MB default), and clears buffers on un-delimited buffer overflow attacks.
2. **Protocol Codec (`src/protocol/codec.js`)**:
   - `ProtocolDecoder`: Deserializes raw wire frames into JavaScript objects and catches JSON syntax errors.
   - `ProtocolEncoder`: Serializes response/request objects into framed wire payload strings.
3. **Request Validator (`src/protocol/validator.js`)**: Enforces strict request schema validation rules (checks request object presence, valid type header, non-empty string payload for PRODUCE).
4. **MessageBroker Domain Engine (`src/broker/broker.js`)**: Pure domain logic operating on validated request objects and returning standard response objects without transport dependencies.

---

## 8. Buffer Safety & Security Boundaries
Without maximum frame size limits, a malicious client or broken network stream could send gigabytes of data without a newline delimiter, leading to heap out-of-memory crashes (`ERR_STRING_TOO_LONG` / OS OOM kill).

`StreamFramer` mitigates this by:
- Rejecting frames larger than `maxFrameSize` (default 1MB).
- Resetting the buffer when an un-delimited payload exceeds memory bounds.

---

## 9. Producer → Broker Communication Flow
1. Producer creates TCP socket connection to `127.0.0.1:5000`.
2. Producer encodes `ProtocolRequest.produce("Hello Distributed Systems")`.
3. Producer writes string to TCP socket.
4. Server pipeline:
   - `StreamFramer.feed(chunk)` -> extracts frame.
   - `ProtocolDecoder.decode(frame)` -> parses request object.
   - `RequestValidator.validate(request)` -> checks schema validity.
   - `MessageBroker.handleRequest(request)` -> pushes message to queue, returns `PRODUCE_ACK`.
   - `ProtocolEncoder.encode(response)` -> writes framed string to socket.
5. Producer receives `PRODUCE_ACK` and closes socket.

---

## 10. Consumer → Broker Communication Flow
1. Consumer creates TCP socket connection to `127.0.0.1:5000`.
2. Consumer encodes `ProtocolRequest.consume()`.
3. Consumer writes string to TCP socket.
4. Server pipeline processes request and returns `MESSAGE` or `NO_MESSAGES`.
5. Consumer receives response, prints payload, and closes socket.

---

## 12. Milestone 3: Topics & Message Routing Learnings

### 1. What is a Topic?
A topic is a named, logical channel or stream of messages within a message broker (e.g. `orders`, `payments`, `notifications`, `logs`). It provides a domain-specific namespace so that producers can categorize messages and consumers can subscribe to specific categories of interest.

### 2. Why Do Message Brokers Need Topics?
Without topics, all messages produced to a broker land in a single shared global queue. This creates several fundamental problems:
- **Coupling & Pollution**: A payment worker consuming from the global queue would receive order events, user signups, and log messages, forcing every client to filter out irrelevant data.
- **Lack of Multi-tenancy**: Different system components cannot operate independently on their own domain streams.
- **Inflexible Routing**: Brokers use topics as first-class routing keys to deliver published data strictly to interested applications.

### 3. Topic vs. Queue
- **Single Global Queue**: A monolithic buffer where all producers write and all consumers read. Messages are intermingled regardless of payload domain.
- **Topic**: A logical category containing an independent, isolated queue for each distinct topic name. Published messages land strictly in their target topic queue.

### 4. How a Message is Routed
1. **Producer Request**: Producer sends `PRODUCE` request containing `payload.topic` and `payload.message`.
2. **Validator**: `RequestValidator` validates topic name rules and message presence.
3. **Broker Domain**: `MessageBroker` queries `TopicManager.hasTopic(topic)`.
4. **Queue Enqueue**: If topic exists, `TopicManager` routes the message payload into `topics.get(topic).messages.push(message)`.
5. **Ack**: Broker returns `PRODUCE_ACK` confirming delivery to that specific topic.

### 5. How Topic Isolation Works
In `TopicManager`, topics are stored internally as a `Map<string, { name: string, messages: Array<any> }>`.
Because each topic name maps to its own separate JavaScript array buffer:
- `CONSUME orders` shift-pops exclusively from `topics.get("orders").messages`.
- `CONSUME payments` shift-pops exclusively from `topics.get("payments").messages`.
A consumer requesting `orders` can never receive or drain messages belonging to `payments`.

### 6. Why Explicit Topic Creation is Useful
In our broker, attempting to `PRODUCE` to or `CONSUME` from an uncreated topic returns a `TOPIC_NOT_FOUND` error rather than auto-creating the topic implicitly.
- **Prevents Typos & Accidental Topics**: A producer with a typo (`PRODUCE topic="oredrs"`) will fail immediately with an explicit error instead of silently creating garbage topics.
- **Resource Management & Governance**: Administrators have full control over allowed topics, preventing unintended memory allocation.

### 7. FIFO Behavior Inside a Topic
First-In, First-Out (FIFO) ordering is strictly preserved **within each individual topic**.
- Messages pushed to `orders` (e.g. Order 1, Order 2, Order 3) are appended to `orders.messages`.
- Consuming from `orders` retrieves Order 1 first, then Order 2, then Order 3 in exact publish sequence.
- Cross-topic arrival order does not impact intra-topic FIFO order.

### 8. Why Partitions Are NOT Implemented Yet
Partitions are a **scalability and parallelism mechanism** (splitting a single topic across multiple logs/nodes to allow parallel consumption). 
- Before scaling a message stream across partitions, we must establish the **logical message model** (topics and routing) first.
- Introducing topics in Milestone 3 establishes clean domain isolation and routing contracts before partitioning and offset tracking are added in subsequent milestones.

