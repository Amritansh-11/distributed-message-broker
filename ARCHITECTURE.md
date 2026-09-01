# Distributed Message Broker Architecture — Milestone 5

## Overview
A lightweight, high-performance distributed message broker implemented in Node.js using native TCP networking, custom line-delimited (NDJSON) framing, multi-partition topic streams, append-only offset logs, consumer groups with deterministic rebalancing, and pure domain isolation.

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
|   MessageBroker (broker.js) — Coordinates domain components              |
+-------------------------------------------------------------------------+
                  /                                       \
                 v                                         v
+-----------------------------------+   +----------------------------------+
|        TopicManager Domain        |   |    ConsumerGroupManager Domain   |
|  (topic-manager.js)               |   |  (consumer-group-manager.js)     |
|  - Topics & Partition Management  |   |  - Group membership (JOIN/LEAVE) |
|  - Append-only log offset reads   |   |  - Round-robin rebalancing       |
+-----------------------------------+   |  - In-memory offset commits      |
                 |                      |  - Group-based CONSUME           |
                 v                      +----------------------------------+
+-----------------------------------+                     |
|           Topic Domain            |                     v
|  (topic.js)                       |   +----------------------------------+
|  - Priority Partition Routing     |   |       ConsumerGroup Domain       |
|  - Map<number, Partition>         |   |  (consumer-group.js)             |
+-----------------------------------+   |  - Active consumers Set          |
                 |                      |  - Partition assignments Map     |
                 v                      |  - Committed offsets Map         |
+-----------------------------------+   |  - Transient positions Map       |
|         Partition Domain          |   +----------------------------------+
|  (partition.js)                   |
|  - Append-only log:               |
|    [{ offset: 0, message }, ...]  |
|  - Monotonic nextOffset counter   |
+-----------------------------------+
```

---

## Domain Hierarchy

```
Broker (MessageBroker)
 ├── TopicManager
 │    └── Topic ("orders")
 │         ├── Partition 0 [Log: (offset 0: Order A), (offset 1: Order D)]
 │         ├── Partition 1 [Log: (offset 0: Order B), (offset 1: Order E)]
 │         └── Partition 2 [Log: (offset 0: Order C)]
 │
 └── ConsumerGroupManager
      └── ConsumerGroup ("order-workers")
           ├── Members: ["consumer-1", "consumer-2"]
           ├── Assignments:
           │     consumer-1 -> [partition-0, partition-2]
           │     consumer-2 -> [partition-1]
           └── Committed Offsets:
                 "orders:0" -> 1
                 "orders:1" -> 0
                 "orders:2" -> 0
```
