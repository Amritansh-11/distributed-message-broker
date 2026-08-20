# Distributed Message Broker - Architecture Document

## System Overview
A lightweight, high-performance distributed message broker designed for asynchronous event-driven messaging across microservices.

```
+------------------+          +------------------------+          +------------------+
|     Producer     |  ----->  | Distributed Broker Node|  ----->  |     Consumer     |
| (Publish Event)  |   TCP    |  (Topic/Partition/Log) |   TCP    | (Subscribe Event)|
+------------------+          +------------------------+          +------------------+
```

## Core Components (Planned Roadmap)
1. **Transport Layer**: High-concurrency TCP Server handling client connections and custom binary/text protocol frame parsing.
2. **Broker Core**: Topic & Partition management, Message Storage Engine (Append-Only Log + Index).
3. **Consumer Management**: Push/Pull delivery models, Consumer Groups, Offset Tracking.
4. **Distributed Consensus & Clustering**: Multi-node replication, Leader Election, High Availability.

## Current System Architecture (Phase 1: TCP Core & Protocol Parser)

```
                       +-----------------------------------+
                       |        TCP Socket Server          |
                       |    (Node.js 'net' on Port 4222)   |
                       +-----------------------------------+
                                         |
                                    (Raw Buffers)
                                         v
                       +-----------------------------------+
                       |      Protocol Stream Parser       |
                       | (Line Delimited + Length-Prefixed)|
                       +-----------------------------------+
                                         |
                                (Parsed Command Frame)
                                         v
                       +-----------------------------------+
                       |         Broker Core Engine        |
                       |  (Topic Registry & Client Sockets)|
                       +-----------------------------------+
```

### Module Structure
- `src/protocol/parser.js`: Accumulates raw TCP stream buffers, parses complete header lines (`\r\n`) and length-prefixed payload frames.
- `src/protocol/commands.js`: Defines commands (`PUB`, `SUB`, `UNSUB`, `ACK`, `MSG`, `ERR`, `OK`) and wire format formatters.
- `src/broker/broker.js`: Manages topic subscriptions, active consumer client sockets, and dispatches messages to subscribers.
- `src/broker/server.js`: Handles TCP connection lifecycle, socket read/write streams, and disconnect cleanup.
- `src/index.js`: Server entry point.

