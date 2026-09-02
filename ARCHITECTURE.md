# Distributed Message Broker Architecture — Milestone 6

## Overview
A lightweight, high-performance distributed message broker implemented in Node.js using native TCP networking, custom line-delimited (NDJSON) framing, multi-partition topic streams, persistent append-only log segments, crash recovery, consumer groups with deterministic rebalancing, and pure domain isolation.

---

## High-Level Pipeline Diagram

```
+-------------------------------------------------------------------------+
|                              TCP Layer                                  |
|   net.Server (server.js) <---> net.Socket (Client / Producer / Consumer) |
+-------------------------------------------------------------------------+
                                   | (Raw Byte Stream)
                                   v
+-------------------------------------------------------------------------+
|                          StreamFramer Layer                             |
|   StreamFramer (framing.js) — Enforces 1MB max frame size & \n delim    |
+-------------------------------------------------------------------------+
                                   | (Extracted JSON Strings)
                                   v
+-------------------------------------------------------------------------+
|                        Protocol Codec Layer                             |
|   ProtocolDecoder / ProtocolEncoder (codec.js) — JSON Parse / Stringify |
+-------------------------------------------------------------------------+
                                   | (Parsed Object)
                                   v
+-------------------------------------------------------------------------+
|                      Request Validator Layer                            |
|   RequestValidator (validator.js) — Enforces schemas & partition range |
+-------------------------------------------------------------------------+
                                   | (Validated Command Object)
                                   v
+-------------------------------------------------------------------------+
|                     MessageBroker Orchestrator                          |
|   MessageBroker (broker.js) — Coordinates domain & storage              |
+-------------------------------------------------------------------------+
         /                                 |                              \
        v                                  v                               v
+-----------------------+     +------------------------+     +--------------------------+
|  TopicManager Domain  |     |     StorageEngine      |     | ConsumerGroupManager     |
| (topic-manager.js)    |     | (storage-engine.js)    |     | (consumer-group-manager) |
| - Topics & Partitions |     | - Disk recovery        |     | - Membership             |
| - In-memory Log Index |     | - LogSegment rollover  |     | - Rebalance              |
+-----------------------+     | - Group offsets disk   |     | - In-memory Offsets      |
                              +------------------------+     +--------------------------+
                                          |
                                          v
                              +------------------------+
                              |      LogSegment        |
                              |  (000000000000.log)    |
                              |  - RecordFormat binary |
                              |  - Crash truncation    |
                              +------------------------+
```

---

## Data Directory Layout

```
data/ (configurable dataDir)
 ├── topics/
 │    ├── orders/
 │    │    ├── partition-0/
 │    │    │    ├── 000000000000.log
 │    │    │    └── 000000001000.log
 │    │    ├── partition-1/
 │    │    │    └── 000000000000.log
 │    │    └── partition-2/
 │    │         └── 000000000000.log
 │    └── payments/
 │         └── partition-0/
 │              └── 000000000000.log
 └── consumer-groups/
      ├── order-workers.json
      └── analytics-workers.json
```
