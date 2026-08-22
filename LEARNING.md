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

## 11. Current Limitations (Deferred to Future Milestones)
1. **Volatility**: Data is lost when broker process stops (needs persistent disk log).
2. **Destructive Reads**: Consuming a message removes it from memory; multiple consumers cannot read the same stream independently (needs topics/offsets).
3. **Single Point of Failure**: No node replication or clustering.
