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

