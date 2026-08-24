# Distributed Message Broker — Architecture Document

## System Overview
A lightweight, high-performance distributed message broker designed for asynchronous event-driven messaging across microservices.

```
Producer (Publish Event)
    ↓
TCP Socket
    ↓
Broker Server (server.js)
    ↓
Protocol Layer (framing → codec → validator)
    ↓
MessageBroker (broker.js)
    ↓
TopicManager (topic-manager.js)
    ↓
Topic Entity
    ↓
Message Queue [FIFO]
    ↓
Consumer (Fetch Event)
```

## Milestone 3 Architecture: Topics & Message Routing

Milestone 3 decouples message queues into dedicated, isolated **Topic** queues managed by `TopicManager`.

```
                      +-----------------------------------+
                      |         TCP Connection            |
                      |   (Node.js 'net' Socket / Server) |
                      +-----------------------------------+
                                        | (Raw Byte Chunks)
                                        v
                      +-----------------------------------+
                      |      StreamFramer (Framing)       |
                      | (Delimiter Buffer & Boundary Split)|
                      +-----------------------------------+
                                        | (Wire Strings)
                                        v
                      +-----------------------------------+
                      |     ProtocolDecoder (Codec)       |
                      | (Wire String -> Request Object)   |
                      +-----------------------------------+
                                        | (Request Object)
                                        v
                      +-----------------------------------+
                      |  RequestValidator (Validation)    |
                      | (Schema & Field Constraint Checks)|
                      +-----------------------------------+
                                        | (Validated Request)
                                        v
                      +-----------------------------------+
                      |     MessageBroker Core Engine     |
                      | (Orchestrates Protocol Dispatch)  |
                      +-----------------------------------+
                                        |
                                        v
                      +-----------------------------------+
                      |     TopicManager Domain Entity    |
                      |   (Topic Lifecycle & Queues)      |
                      |   Map<string, TopicQueue>         |
                      +-----------------------------------+
                                   /    |    \
                        "orders"  /     |     \  "payments"
                                 v      v      v
                           [Queue]   [Queue]  [Queue]
```

### Module Structure

- `src/broker/topic-manager.js`: `TopicManager` class for managing topic lifecycle, topic validation, topic listing, deletion safeguards, and isolated FIFO message queues (`Map<string, Topic>`).
- `src/broker/broker.js`: `MessageBroker` domain orchestrator that receives validated protocol requests and delegates routing to `TopicManager`.
- `src/broker/server.js`: `BrokerServer` TCP socket transport listener pipeline.
- `src/protocol/types.js`: Defines request types (`PING`, `CREATE_TOPIC`, `LIST_TOPICS`, `GET_TOPIC_INFO`, `DELETE_TOPIC`, `PRODUCE`, `CONSUME`), response types (`PONG`, `CREATE_TOPIC_ACK`, `TOPICS`, `TOPIC_INFO`, `DELETE_TOPIC_ACK`, `PRODUCE_ACK`, `MESSAGE`, `NO_MESSAGES`, `ERROR`), and object builders.
- `src/protocol/framing.js`: `StreamFramer` class for stream byte accumulation, frame boundary extraction, partial frame handling, and max frame size limit enforcement.
- `src/protocol/codec.js`: `ProtocolDecoder` and `ProtocolEncoder` for JSON serialization/deserialization.
- `src/protocol/validator.js`: `RequestValidator` enforcing request schema boundaries, topic validation rules, and string message checks.
- `src/cli/topic-create.js`: CLI script for sending `CREATE_TOPIC` requests.
- `src/cli/topic-list.js`: CLI script for sending `LIST_TOPICS` requests.
- `src/producer/producer.js`: Producer CLI client sending topic-routed `PRODUCE` requests.
- `src/consumer/consumer.js`: Consumer CLI client sending topic-routed `CONSUME` requests.

