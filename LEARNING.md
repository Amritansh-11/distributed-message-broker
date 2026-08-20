# Key System Architecture & TCP Learnings — Milestone 1

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

## 6. Why We Chose NDJSON (Line-Delimited JSON)
For Milestone 1, we selected **NDJSON (Newline-Delimited JSON)**:
- **Simplicity**: Every payload is serialized as JSON followed by a newline delimiter (`\n`).
- **Human Readability & Debuggability**: Developers can test the server using standard terminal networking utilities like `netcat`, `telnet`, or PowerShell raw sockets.
- **Decoupled Architecture**: Our `NDJSONFramer` handles buffering and splitting. The core `BrokerServer` only receives fully parsed JavaScript objects. If we migrate to Length-Prefixed binary framing in future milestones, `BrokerServer` logic remains untouched!

---

## 7. Producer → Broker Communication Flow
1. Producer creates socket connection to `127.0.0.1:5000`.
2. Producer encodes `{ "type": "PRODUCE", "message": "Hello Distributed Systems" }` + `\n`.
3. Producer writes string to TCP socket.
4. Broker framing buffer receives chunk, detects `\n`, parses JSON payload, validates request, and pushes `"Hello Distributed Systems"` into in-memory array `this.messages`.
5. Broker responds with `{ "type": "PRODUCE_ACK", "success": true }` + `\n`.
6. Producer receives ACK and closes socket (`socket.end()`).

---

## 8. Consumer → Broker Communication Flow
1. Consumer creates socket connection to `127.0.0.1:5000`.
2. Consumer encodes `{ "type": "CONSUME" }` + `\n`.
3. Consumer writes string to TCP socket.
4. Broker processes request:
   - If `this.messages.length > 0`, it pops/shifts the next message and responds with `{ "type": "MESSAGE", "success": true, "message": "..." }`.
   - If queue is empty, it responds with `{ "type": "NO_MESSAGES", "success": true }`.
5. Consumer receives response, prints result, and closes socket.

---

## 9. In-Memory Message Storage
In Milestone 1, messages are temporarily stored in a standard JavaScript array (`messages = []`).
- `PRODUCE` calls `messages.push(msg)` (FIFO enqueue).
- `CONSUME` calls `messages.shift()` (FIFO dequeue).

This queue lives strictly in heap memory.

---

## 10. Connection Disconnects & Error Handling
- **Socket Disconnect (`close` event)**: Broker tracks active client connections in a `Set`. When a client closes the socket, the broker logs the event and removes the socket reference to prevent memory leaks.
- **Socket Errors (`error` event)**: Network dropouts or client crashes (e.g. `ECONNRESET`) trigger socket error handlers. Both broker and clients handle socket errors gracefully without terminating the main process unexpectedly.
- **Malformed Data**: Invalid JSON strings received over TCP are caught by `NDJSONFramer`, logged, and responded to with a `{ "type": "ERROR", ... }` frame.

---

## 11. Current Limitations to address in future milestones
1. **Volatility**: Data is lost when broker process stops (needs persistent disk log).
2. **Destructive Reads**: Consuming a message removes it from memory; multiple consumers cannot read the same stream independently (needs topics/offsets).
3. **Single Point of Failure**: No node replication or clustering.
