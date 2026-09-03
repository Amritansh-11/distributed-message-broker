# Distributed Message Broker Architecture — Milestone 8

## Overview
A lightweight, high-performance distributed message broker implemented in Node.js using native TCP networking, custom line-delimited (NDJSON) framing, multi-partition topic streams, persistent append-only log segments, crash recovery, consumer groups with deterministic rebalancing, multi-broker cluster topology, inter-broker TCP heartbeats, partition replication across follower brokers (`REPLICATE_RECORD` / `REPLICATE_ACK`), leader write enforcement (`NOT_LEADER` error), High-Water Mark tracking, and follower catch-up sync (`REPLICA_SYNC`).

---

## Replication Flow Diagram

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

## High-Level Component Layout

```
+-------------------------------------------------------------------------+
|                              TCP Layer                                  |
|   net.Server (server.js) <---> net.Socket (Client / Remote Broker)      |
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
|                       Server Routing Switch                             |
+-------------------------------------------------------------------------+
           /                           |                           \
          v                            v                            v
(BROKER_HELLO/PING)            (REPLICATE/SYNC)              (Client Requests)
          |                            |                            |
          v                            v                            v
  ClusterManager              ReplicationManager              MessageBroker
(cluster-manager.js)       (replication-manager.js)           (broker.js)
```
