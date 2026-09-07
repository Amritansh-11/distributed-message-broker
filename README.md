# Distributed Message Broker — Complete Milestones 1–10 Architecture

A custom, lightweight, high-performance, containerized TCP-based distributed message broker built from scratch in Node.js without third-party messaging libraries or HTTP frameworks.

---

## Complete End-to-End System Architecture (M1–M10)

```
+-----------------------------------------------------------------------------------+
|                           HTTP Observability Layer                                |
|   ManagementServer (server/management-server.js) — GET /health, /metrics, /cluster|
+-----------------------------------------------------------------------------------+
                                         |
+-----------------------------------------------------------------------------------+
|                              TCP Server Layer                                     |
|   BrokerServer (broker/server.js) — Handles Client & Inter-Broker TCP Connections |
+-----------------------------------------------------------------------------------+
                                         |
+-----------------------------------------------------------------------------------+
|                          Protocol & Framing Layer                                 |
|   StreamFramer (framing.js) & ProtocolDecoder/Encoder (codec.js) (NDJSON wire)    |
+-----------------------------------------------------------------------------------+
                                         |
+-----------------------------------------------------------------------------------+
|                          MessageBroker Domain Core                                |
|   MessageBroker (broker/broker.js) — Integrates Topics, Partitions, Groups        |
+-----------------------------------------------------------------------------------+
     /                     |                     |                     \
    v                      v                     v                      v
TopicManager       ConsumerGroupManager   StorageEngine          ClusterManager
(Topic & Partitions) (Group Rebalancing)  (Binary Log Segments) (Heartbeats & Node States)
                                                 |                      |
                                                 v                      v
                                          LogSegments           ReplicationManager &
                                         (0000.log & CRC32)     LeaderElectionManager
```

---

## Summary of Completed Milestones (M1–M10)

- **M1: Basic TCP Broker**: Native Node.js `net.Server` with concurrent client connections.
- **M2: TCP Framing & Protocol**: Line-delimited NDJSON protocol (`StreamFramer`) handling fragmentation.
- **M3: Topics & Routing**: Explicit topic creation (`CREATE_TOPIC`), listing (`LIST_TOPICS`), and isolation.
- **M4: Partitions & Routing**: Key-based hashing (`hashKey(key) % P`), round-robin, explicit partition parameter, and per-partition FIFO ordering.
- **M5: Offsets & Consumer Groups**: 0-indexed monotonic offsets, consumer group membership (`JOIN_GROUP`/`LEAVE_GROUP`), partition rebalancing, and offset commits (`COMMIT_OFFSET`).
- **M6: Persistent Storage Engine**: Length-prefixed binary log segments (`000000000000.log`), CRC32 checksums, crash recovery (`recoverAllState()`).
- **M7: Multi-Broker Cluster**: Static cluster topology, inter-broker TCP handshakes (`BROKER_HELLO`), and heartbeats (`BROKER_PING`).
- **M8: Partition Replication**: Single-leader replication (`REPLICATE_RECORD`), High-Water Mark (`highWaterMark`), and follower catch-up sync (`REPLICA_SYNC`).
- **M9: Failure Handling & Leader Election**: Deterministic election candidate ranking (highest offset primary, alphabetical broker ID tie-breaker), leader epoch incrementing (`leaderEpoch += 1`), stale leader protection (`NOT_LEADER`/`STALE_LEADER`), and old leader rejoin as follower.
- **M10: Observability, Docker & Production Readiness**: Centralized structured JSON logger (`LOG_LEVEL`), in-memory metrics collector (`MetricsCollector`), HTTP management endpoints (`/metrics`, `/health`, `/cluster`), environment configuration (`ConfigLoader`), Dockerized 3-broker cluster compose setup with persistent volumes, graceful signal shutdown (`SIGINT`/`SIGTERM`), 10,000-message stress testing, and complete end-to-end integration.

---

## Environment Variables Configuration

| Variable | Description | Default |
|---|---|---|
| `BROKER_ID` | Unique identifier for local broker node | `broker-1` |
| `BROKER_HOST` | Host IP for TCP server binding | `127.0.0.1` |
| `BROKER_PORT` | Listening port for client & peer TCP sockets | `5000` |
| `HTTP_PORT` | Listening port for HTTP observability management server | `8000` |
| `BROKER_DATA_DIR` | Disk path for persistent log segments and checkpoints | `./data/broker-1` |
| `CLUSTER_BROKERS` | Comma-separated list of static cluster nodes (`id:host:port`) | `broker-1:127.0.0.1:5000...` |
| `REPLICATION_FACTOR` | Requested partition replica count | `1` |
| `HEARTBEAT_INTERVAL` | Peer heartbeat check interval in milliseconds | `1000` |
| `ELECTION_TIMEOUT` | Peer missed heartbeat timeout in milliseconds | `2000` |
| `LOG_LEVEL` | Logging threshold (`DEBUG`, `INFO`, `WARN`, `ERROR`) | `INFO` |

---

## How to Run Locally

1. **Install Dependencies**:
   ```bash
   npm install
   ```

2. **Run Single Local Broker**:
   ```bash
   npm run broker
   ```

3. **Run 3-Broker Cluster Locally**:
   ```bash
   npm run broker -- --id=broker-1 --port=5000 --http-port=8001
   npm run broker -- --id=broker-2 --port=5001 --http-port=8002
   npm run broker -- --id=broker-3 --port=5002 --http-port=8003
   ```

---

## How to Run with Docker Compose

1. **Start 3-Broker Docker Cluster**:
   ```bash
   docker compose up --build
   ```

2. **Verify Container Cluster Status**:
   ```bash
   docker compose ps
   ```

3. **Stop Docker Cluster & Retain Volumes**:
   ```bash
   docker compose down
   ```

4. **Stop Docker Cluster & Remove Volumes**:
   ```bash
   docker compose down -v
   ```

---

## HTTP Observability Endpoints

- **GET /health**:
  ```json
  {
    "status": "HEALTHY",
    "brokerId": "broker-1",
    "uptime": 124.5,
    "clusterSize": 3
  }
  ```
- **GET /metrics**:
  ```text
  connections_total 10
  active_connections 2
  messages_produced_total 10000
  messages_consumed_total 9500
  produce_errors_total 0
  consume_errors_total 0
  replication_success_total 10000
  replication_failure_total 0
  replica_sync_total 3
  leader_elections_total 1
  leader_changes_total 1
  produce_latency_avg_ms 1.15
  consume_latency_avg_ms 0.85
  replication_latency_avg_ms 2.05
  ```
- **GET /cluster**:
  ```json
  [
    { "id": "broker-1", "host": "broker-1", "port": 5000, "status": "alive" },
    { "id": "broker-2", "host": "broker-2", "port": 5001, "status": "alive" },
    { "id": "broker-3", "host": "broker-3", "port": 5002, "status": "alive" }
  ]
  ```

## Performance Benchmarking

The broker was benchmarked using k6 and custom TCP wire benchmarks. The benchmark scripts and methodology are included in the repository.

### k6 Load Test

- Virtual Users: **215**
- Total HTTP requests: **30,636**
- Test duration: **25.8 seconds**
- HTTP throughput: **1,185.90 requests/sec**
- Check success rate: **99.70%**

### TCP Wire Benchmark

| Operation | Throughput | Average Latency |
|---|---:|---:|
| TCP Produce | **681 msgs/sec** | **1.23 ms** |
| TCP Consume | **8,130 msgs/sec** | **0.04 ms** |

TCP Produce was tested with **10 concurrent TCP connections**.

### Test Reliability

- **15/15 unit and integration test suites passed**
- Benchmark completed with **zero exit errors**

---

## Testing Commands

- **Run All 15 Test Suites**:
  ```bash
  npm test
  ```
- **Run Observability & Docker Unit Tests**:
  ```bash
  npm run test:m10
  ```
- **Run 10,000-Message Stress & E2E Test**:
  ```bash
  npm run test:stress
  ```

---

## Simulating Failures & Recovery

1. **Simulate Follower Failure**: Stop `broker-3`. The leader continues accepting produce requests and tracks lag. When `broker-3` restarts, it issues `REPLICA_SYNC` to catch up missing log segments.
2. **Simulate Leader Failure**: Stop `broker-1` (the partition 0 leader). Remaining brokers detect node failure via heartbeats, run deterministic leader election, elect `broker-2` as the new leader, increment `leaderEpoch`, and broadcast `LEADER_ANNOUNCE`.
3. **Simulate Old Leader Rejoin**: Restart `broker-1`. It detects current leader & epoch, rejoins as a follower, issues `REPLICA_SYNC`, catches up, and does NOT usurp active leadership.

---

## Known Limitations

- Static cluster topology configuration (dynamic cluster membership is out of scope).
- Follower brokers reject consumer read requests with `NOT_LEADER` (all consumer reads route through partition leaders).
- Raft/Paxos/ZooKeeper consensus protocols are intentionally not used to maintain a clear, deterministic implementation.
