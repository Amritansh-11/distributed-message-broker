# Distributed Message Broker — Milestone 8: Partition Replication & Replica Synchronization

A custom, lightweight, TCP-based distributed message broker built from scratch in Node.js without third-party messaging libraries or HTTP frameworks.

---

## Milestone 8 Architecture: Partition Replication & Replica Synchronization

Milestone 8 introduces **data replication** across multiple brokers in the cluster.

Each topic partition has:
- **One Leader**: Accepts PRODUCE writes from producers, appends records to its authoritative partition log & persistent storage, and replicates records to follower brokers.
- **One or More Followers (Replicas)**: Persist replicated records at exact leader-assigned offsets and maintain isolated partition log segment files.

```
                    Producer
                       |
                   (PRODUCE)
                       v
         +---------------------------+
         |     Partition Leader      |
         |        (broker-1)         |
         |  Append Local Log & Disk  |
         +---------------------------+
               /               \
       (REPLICATE_RECORD)  (REPLICATE_RECORD)
             /                   \
            v                     v
+-----------------------+   +-----------------------+
|   Follower Replica    |   |   Follower Replica    |
|      (broker-2)       |   |      (broker-3)       |
| Persist Offset & Disk |   | Persist Offset & Disk |
+-----------------------+   +-----------------------+
            \                     /
      (REPLICATE_ACK)       (REPLICATE_ACK)
             \                   /
              v                 v
         +---------------------------+
         |  Update High-Water Mark   |
         |    Return PRODUCE_ACK     |
         +---------------------------+
```

---

## Key Concepts & Algorithms

### 1. Replication Factor
Configurable option when creating topics (`replicationFactor`, default: 1).
- $1 \le \text{replicationFactor} \le \text{totalClusterBrokers}$.
- Invalid values return structured error code `INVALID_REPLICATION_FACTOR`.

### 2. Deterministic Replica Assignment
Given $N$ cluster brokers sorted by ID (`['broker-1', 'broker-2', 'broker-3']`), for partition $p$:
- **Leader**: `brokers[p % N]`
- **Replicas**: `[ brokers[(p + 0) % N], ..., brokers[(p + R - 1) % N] ]`

### 3. Leader Write Rule (`NOT_LEADER` Error)
Only the partition leader can accept `PRODUCE` writes. If a producer sends a write to a follower:
```json
{
  "type": "ERROR",
  "success": false,
  "error": {
    "code": "NOT_LEADER",
    "message": "Broker 'broker-2' is not the leader for topic 'orders' partition 0",
    "topic": "orders",
    "partition": 0,
    "leader": "broker-1"
  }
}
```

### 4. High-Water Mark ($HWM$)
The High-Water Mark tracks the minimum `lastReplicatedOffset` among all active caught-up replicas for a partition. It advances as followers acknowledge record replication.

### 5. Follower Recovery & Catch-up (`REPLICA_SYNC`)
When a follower broker restarts after failure:
1. Follower determines its highest local partition offset.
2. Sends `REPLICA_SYNC` request to the partition leader with `fromOffset`.
3. Leader fetches missing records and sends `REPLICA_SYNC_RESPONSE`.
4. Follower appends missing records in order to local storage while preserving leader offsets.
5. Follower state transitions back to `CAUGHT_UP`.

---

## Scope Limits: What Milestone 8 Supports & Does NOT Support

### Supported in Milestone 8
- Configurable replication factor and deterministic replica assignment
- Leader write rule enforcement (`NOT_LEADER` error)
- Inter-broker record replication (`REPLICATE_RECORD` / `REPLICATE_ACK`)
- Follower record persistence & exact offset preservation
- High-Water Mark calculation
- Follower recovery & catch-up sync (`REPLICA_SYNC` / `REPLICA_SYNC_RESPONSE`)
- Consumer reads from Leader

### Intentionally NOT Supported in Milestone 8
- Leader election & automatic failover
- Raft / Paxos consensus algorithm
- Follower reads
- Dynamic partition reassignment

---

## How to Run a 3-Broker Replicated Cluster Setup

1. **Start Broker 1 (Port 5000)**:
   ```bash
   npm run broker -- --id=broker-1 --port=5000
   ```
2. **Start Broker 2 (Port 5001)**:
   ```bash
   npm run broker -- --id=broker-2 --port=5001
   ```
3. **Start Broker 3 (Port 5002)**:
   ```bash
   npm run broker -- --id=broker-3 --port=5002
   ```

4. **Create a Replicated Topic**:
   ```json
   { "type": "CREATE_TOPIC", "payload": { "topic": "orders", "partitions": 3, "replicationFactor": 3 } }
   ```

---

## Test Execution

```bash
npm test
```
Or run replication test suite specifically:
```bash
npm run test:replication
```

---

## Core Components Architecture

- [replication-manager.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/cluster/replication-manager.js) — `ReplicationManager`: Replication coordinator, leader write checks, high-water mark, follower sync.
- [broker-node.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/cluster/broker-node.js) — `BrokerNode`: Node entity tracking broker status and socket handle.
- [cluster-manager.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/cluster/cluster-manager.js) — `ClusterManager`: Cluster topology manager and heartbeat loop.
- [partition.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/broker/partition.js) — `Partition`: Append-only log supporting `enqueueWithOffset`.
- [broker.js](file:///c:/Users/amrit/OneDrive/Desktop/distributed-message-broker/src/broker/broker.js) — `MessageBroker`: Core domain orchestrator enforcing `NOT_LEADER` rules.
