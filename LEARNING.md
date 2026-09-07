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

## 4. Milestone 7 & 8: Multi-Broker Clustering & Replication
- **Static Cluster Topology**: Pre-configured broker list ensures reliable peer connectivity.
- **Single-Leader Data Replication**: Partition leader receives writes and streams `REPLICATE_RECORD` to follower replicas over TCP.

---

## 5. Milestone 9: Failure Handling & Deterministic Leader Election Learnings

### 1. Deterministic Candidate Selection
Ranking candidates first by highest log offset minimizes data loss during failover. Using alphabetical broker ID as a secondary tie-breaker ensures all surviving brokers independently reach the exact same election decision without round-trip voting rounds.

### 2. Leader Epoch (`leaderEpoch`)
Monotonically increasing `leaderEpoch` associated with partition metadata prevents stale leaders from accepting writes after network partitions heal, eliminating split-brain state.

### 3. Asymmetric Rejoin Behavior
A returning leader must not automatically usurp leadership upon restart. Remaining a follower and catching up missing offsets via `REPLICA_SYNC` preserves cluster stability.

### 4. Partition Independence
Partition leadership is tracked independently. A single broker failure only triggers election for partitions led by that specific broker, keeping unaffected partitions healthy.
