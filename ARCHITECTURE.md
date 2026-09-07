# Distributed Message Broker Architecture — Milestone 9

## Overview
A lightweight, high-performance distributed message broker implemented in Node.js using native TCP networking, custom line-delimited (NDJSON) framing, multi-partition topic streams, persistent append-only log segments, crash recovery, consumer groups with deterministic rebalancing, multi-broker cluster topology, inter-broker TCP heartbeats, partition replication across follower brokers (`REPLICATE_RECORD` / `REPLICATE_ACK`), leader failure detection, deterministic leader election (`LeaderElectionManager`), leader epoch tracking (`leaderEpoch`), stale leader rejection (`STALE_LEADER` / `NOT_LEADER`), and follower catch-up sync (`REPLICA_SYNC`).

---

## Failover & Leader Election Flow

```
                         Broker Failure (broker-1)
                                     |
                         Failure Detector (Heartbeat / Socket Close)
                                     |
                     Partition Marked as Requiring Election
                                     |
                        Evaluate ALIVE Replicas
                                     |
             Filter Candidates -> Rank by Highest Offset -> Tie-Break by Broker ID
                                     |
                         New Leader Elected (broker-2)
                                     |
                            Increment leaderEpoch
                                     |
                          Broadcast LEADER_ANNOUNCE
                                     |
               +-------------------------------------------+
               |                                           |
    New Leader (broker-2)                        Old Leader (broker-1)
  Accepts PRODUCE & CONSUME                      Rejoins as Follower &
                                                 Syncs via REPLICA_SYNC
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
     /                  |                     |                    \
    v                   v                     v                     v
(BROKER_HELLO/PING) (LEADER_ANNOUNCE)   (REPLICATE/SYNC)     (Client Requests)
    |                   |                     |                     |
    v                   v                     v                     v
ClusterManager  LeaderElectionManager  ReplicationManager     MessageBroker
```
