# Key System Architecture & Distributed Systems Learnings

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

## 7. Milestone 5: Offsets & Consumer Groups Learnings

### 1. What are Offsets?
An offset is a unique, monotonically increasing integer assigned to each message appended to a partition log.
- Offsets start at 0 within each partition.
- Offsets are independent between partitions.
- Offsets provide an absolute logical sequence identifier for every message in a partition stream.

### 2. Offset-Based Consumption vs. Destructive Queues
- **Traditional Queue (e.g. RabbitMQ)**: Consuming a message removes/deletes it from the queue buffer.
- **Append-Only Log (Kafka / Our Broker)**: Messages remain stored in the partition log indefinitely (or until retention cleanup). Consumers read messages by specifying offsets.
- **Key Advantage**: Multiple independent applications (e.g., real-time order processing, analytics, audit logging) can read the exact same message stream at their own pace without interfering with each other.

### 3. Why Messages Are No Longer Deleted
By keeping messages in an append-only log:
- New consumer groups can replay historical messages starting from offset 0.
- Failed consumer nodes can re-read previously uncommitted offsets during recovery.
- Message data becomes immutable and audit-friendly.

### 4. What is a Consumer Group?
A Consumer Group is a logical grouping of consumers that cooperate to consume messages from a set of topics.
- Partitions of the subscribed topics are divided among the members of the group.
- Each partition is assigned to **at most one consumer** in the group.
- Different consumer groups maintain completely isolated offset pointers.

### 5. Partition Assignment & Rebalancing
When a consumer joins (`JOIN_GROUP`) or leaves (`LEAVE_GROUP`) a group:
- The broker recalculates partition assignments across active consumers using a deterministic round-robin algorithm.
- Consumers in the group sorted alphabetically by ID receive partition allocations evenly.
- Committed offsets are preserved during rebalance so consumers pick up exactly where previous partition owners left off.

### 6. Committed Offsets vs Current Position
- **Current Position**: The transient in-memory offset index that a consumer is currently reading.
- **Committed Offset**: The explicitly acknowledged offset checkpoint saved by calling `COMMIT_OFFSET`.
- **Separation of Concerns**: Auto-advancing position allows high-speed stream reading, while explicit `COMMIT_OFFSET` guarantees processing semantics (at-least-once processing).

### 7. Current In-Memory Limitations & Future Persistence
In Milestone 5, partition logs and committed offsets are maintained in-memory (`Map` and JS arrays).
- **Current Limit**: Server restart clears logs and offsets.
- **Future Milestones**: Log segments will be persisted to disk files (`.log` and `.index`) for crash recovery and durability.
