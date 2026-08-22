# Architecture Decision Records (ADR)

This file logs key architectural decisions, rationale, alternatives considered, and tradeoffs made during the project lifecycle.

---

## ADR-000: Adoption of Learning-First Development Methodology
- **Status**: Accepted
- **Context**: The project will be developed iteratively with explicit conceptual explanations, architectural options, code walkthroughs, testing commands, failure scenarios, and interview questions.
- **Decision**: Adhere strictly to the 9-step Learning-First process and maintain `ARCHITECTURE.md`, `LEARNING.md`, and `DECISIONS.md`.

---

## ADR-001: Technology Stack & TCP Wire Protocol Choice
- **Status**: Accepted
- **Context**: A message broker requires high-concurrency connection handling and ultra-low latency wire protocol parsing without HTTP header overhead.
- **Options Considered**:
  1. Node.js (Async Event Loop, `net` module) vs Go vs Python.
  2. Length-Prefixed Line-Delimited framing (NATS/Redis RESP style) vs JSON-over-TCP vs Binary protocol.
- **Decision**: 
  - **Runtime**: Node.js (JavaScript ES Modules) utilizing native `net` socket module.
  - **Protocol**: Length-prefixed line-delimited wire protocol (`PUB <topic> <bytes>\r\n<payload>\r\n`, `SUB <topic>\r\n`).
- **Tradeoffs**:
  - *Pros*: Human readable via `telnet`/`netcat`, zero HTTP overhead, streaming framing support, non-blocking I/O.
  - *Cons*: Slightly higher byte overhead compared to pure binary packed formats (Protobuf/FlatBuffers), single-threaded JS event loop requires clustering/worker threads for multi-core scaling.

---

## ADR-002: Modular Multi-Layer TCP Protocol Pipeline (Milestone 2)
- **Status**: Accepted
- **Context**: Milestone 1 coupled socket handling, buffer framing, request parsing, and array storage in monolithic classes. Milestone 2 requires a robust, reusable protocol layer beneath the broker.
- **Options Considered**:
  1. Monolithic socket handler with embedded JSON parsing logic.
  2. Multi-layer decoupled pipeline: TCP -> StreamFramer -> ProtocolDecoder -> RequestValidator -> MessageBroker -> ProtocolEncoder -> StreamFramer -> TCP.
- **Decision**: Adopt the multi-layer pipeline architecture.
  - `StreamFramer`: Pure byte-stream delimiter framing and max frame size security boundaries.
  - `ProtocolDecoder` & `ProtocolEncoder`: Wire format serialization/deserialization.
  - `RequestValidator`: Explicit schema and validation layer returning standard ERROR responses.
  - `MessageBroker`: Pure domain state engine completely isolated from transport concerns.
- **Tradeoffs**:
  - *Pros*: Extreme modularity, high testability (unit testing framing and validator independently without TCP servers), robust error handling and buffer security.
  - *Cons*: Additional object allocations per request step in JavaScript runtime.
