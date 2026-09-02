# Key System Architecture & Distributed Systems Learnings

## 1. What is TCP?
Transmission Control Protocol (TCP) is a core transport layer protocol (OSI Layer 4) that provides reliable, ordered, and error-checked delivery of a stream of octets (bytes) between applications running on hosts communicating via an IP network.

---

## 2. Why Message Framing is Necessary
Because TCP does not enforce message boundaries, the receiving application must implement **Message Framing** at OSI Layer 7 (Application Layer).

Without framing, direct `JSON.parse(chunk)` calls crash due to packet fragmentation or packet coalescing.

---

## 3. Milestone 5: Offsets & Consumer Groups Learnings
- **Monotonic Offsets**: Unique integer `offset` assigned to each message appended to a partition log starting at 0.
- **Append-Only Log**: Messages remain in memory indefinitely; consumers read by offset without deleting records.
- **Consumer Group Rebalancing**: Deterministic round-robin partition assignment across sorted consumer IDs when members join or leave.

---

## 4. Milestone 6: Persistent Message Storage Learnings

### 1. Storage Engine Abstraction
Placing file system calls directly throughout broker socket handlers leads to tight coupling and untestable code. Decoupling storage into `StorageEngine`, `LogSegment`, and `RecordFormat` allows domain components to remain pure while maintaining persistent state.

### 2. Length-Prefixed Framing vs. Text Delimiters
Binary length-prefix framing (`[4-byte Length][Payload String][\n]`) solves record boundary detection on disk:
- The reader knows exactly how many payload bytes to read.
- Partial writes from power loss or abrupt crashes are easily detected when available bytes $< 4 + \text{Length} + 1$.

### 3. Checksum Verification & CRC32
Hardware faults or incomplete disk writes can corrupt data. Including a CRC32 checksum in every log record payload guarantees data integrity during recovery.

### 4. Crash Recovery & Tail Truncation
When recovering segment files on broker startup:
- Valid records preceding an incomplete tail record are preserved.
- The partial tail at EOF is truncated back to the last valid byte offset, allowing normal production to resume seamlessly without losing historical data.

### 5. Log Segmentation & Rollover
Single-file logs cause unbounded file size growth and slow recovery. Splitting partition logs into segment files named after base offsets (`000000000000.log`, `000000001000.log`) enables bounded file limits and efficient sequential reads.
