# Distributed Message Broker — Architecture Document

## System Overview
A lightweight, high-performance distributed message broker designed for asynchronous event-driven messaging across microservices.

```
+------------------+          +------------------------+          +------------------+
|     Producer     |  ----->  | Distributed Broker Node|  ----->  |     Consumer     |
| (Publish Event)  |   TCP    |  (In-Memory Store / Log)|   TCP    | (Consume Event)  |
+------------------+          +------------------------+          +------------------+
```

## Milestone 2 Layered Protocol Architecture

Milestone 2 establishes a structured, decoupled, multi-layer protocol pipeline for TCP communication:

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
                      | (Pure In-Memory Store Logic)      |
                      +-----------------------------------+
                                        | (Response Object)
                                        v
                      +-----------------------------------+
                      |     ProtocolEncoder (Codec)       |
                      | (Response Object -> Wire Frame)   |
                      +-----------------------------------+
                                        | (Framed String)
                                        v
                      +-----------------------------------+
                      |         TCP Connection            |
                      |     (socket.write Payload)        |
                      +-----------------------------------+
```

### Module Structure

- `src/protocol/types.js`: Defines request constants (`PING`, `PRODUCE`, `CONSUME`), response constants (`PONG`, `PRODUCE_ACK`, `MESSAGE`, `NO_MESSAGES`, `ERROR`), and request/response object creators.
- `src/protocol/framing.js`: `StreamFramer` class for stream byte accumulation, frame boundary extraction, partial frame handling, and max frame size limit enforcement.
- `src/protocol/codec.js`: `ProtocolDecoder` (deserializes wire strings into request objects) and `ProtocolEncoder` (serializes response/request objects into framed wire strings).
- `src/protocol/validator.js`: `RequestValidator` for enforcing request object structure, command type validation, and message string constraints before reaching broker logic.
- `src/broker/broker.js`: `MessageBroker` domain engine class containing pure message queue storage/retrieval logic.
- `src/broker/server.js`: `BrokerServer` TCP listener connecting TCP sockets to the multi-layer protocol pipeline.
- `src/producer/producer.js`: Producer client operating through the protocol framing and codec layer.
- `src/consumer/consumer.js`: Consumer client operating through the protocol framing and codec layer.
- `src/index.js`: Main server execution entry point.
