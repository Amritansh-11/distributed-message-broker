# Key System Architecture & Distributed Systems Learnings

## 1. What is TCP?
Transmission Control Protocol (TCP) is a core transport layer protocol (OSI Layer 4) that provides reliable, ordered, and error-checked delivery of a stream of octets (bytes) between applications running on hosts communicating via an IP network.

---

## 2. Why Message Framing is Necessary
Because TCP does not enforce message boundaries, the receiving application must implement **Message Framing** at OSI Layer 7 (Application Layer).

---

## 3. Milestone 5 & 6: Offsets, Consumer Groups & Persistence
- **Monotonic Offsets**: Unique integer `offset` assigned to each message appended to a partition log starting at 0.
- **Append-Only Log**: Messages remain in memory and disk log segment files indefinitely.
- **Length-Prefixed Binary Framing**: `[4-byte Length][Payload String][\n]` with CRC32 checksums for crash recovery and tail truncation.

---

## 4. Milestone 7: Multi-Broker Cluster Learnings
- **Separation of Client vs. Inter-Broker Protocol**: Internal messages (`BROKER_HELLO`, `BROKER_PING`) reuse framing while remaining isolated from client requests.
- **Static Cluster Topology Discovery**: Pre-configured broker list ensures reliable peer connectivity.
- **Storage Directory Isolation**: Enforcing `./data/<brokerId>` prevents file lock collisions.

---

## 5. Milestone 8: Partition Replication & Replica Synchronization Learnings

### 1. Leader-Based Data Replication
Single-leader replication simplifies consensus by having one authoritative leader broker assign offsets and handle produce writes, while followers store identical copies.

### 2. Leader Write Rule Enforcement
Enforcing that produce requests sent to non-leader brokers are rejected with `NOT_LEADER` error prevents split-brain and out-of-order writes.

### 3. Preserving Leader Offsets on Followers
Followers must never assign independent local offsets; they must preserve the leader's assigned offset to guarantee identical log sequences across replicas.

### 4. High-Water Mark ($HWM$)
The High-Water Mark tracks the minimum offset replicated across required active followers, serving as the threshold for committed records.

### 5. Catch-Up Recovery (`REPLICA_SYNC`)
When a follower recovers from failure, comparing local vs. leader log offsets allows streaming missing records in order without re-synchronizing the entire log.
