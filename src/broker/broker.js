import net from 'net';
import { fileURLToPath } from 'url';
import { NDJSONFramer, ProtocolResponse } from '../protocol/message.js';

export class BrokerServer {
  constructor(port = 5000) {
    this.port = port;
    this.messages = []; // In-memory message store for Milestone 1
    this.server = null;
    this.connections = new Set();
  }

  /**
   * Starts the TCP Broker server.
   * @returns {Promise<void>}
   */
  start() {
    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => {
        this._handleConnection(socket);
      });

      this.server.on('error', (err) => {
        console.error(`[Broker Error] Server error: ${err.message}`);
        reject(err);
      });

      this.server.listen(this.port, () => {
        console.log(`[Broker] TCP Message Broker listening on port ${this.port}`);
        resolve();
      });
    });
  }

  /**
   * Gracefully shuts down the broker and closes all active sockets.
   * @returns {Promise<void>}
   */
  stop() {
    return new Promise((resolve) => {
      if (!this.server) return resolve();

      // Close active client sockets
      for (const socket of this.connections) {
        socket.destroy();
      }
      this.connections.clear();

      this.server.close(() => {
        console.log('[Broker] Broker server shut down gracefully');
        resolve();
      });
    });
  }

  /**
   * Handles individual TCP client connection lifecycle and framing.
   * @param {import('net').Socket} socket 
   */
  _handleConnection(socket) {
    const clientAddr = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`[Broker] Client connected: ${clientAddr}`);
    
    this.connections.add(socket);
    const framer = new NDJSONFramer();

    socket.on('data', (chunk) => {
      const frames = framer.feed(chunk);

      for (const frame of frames) {
        if (frame.error) {
          console.warn(`[Broker] Malformed JSON received from ${clientAddr}: "${frame.raw}"`);
          if (socket.writable) {
            socket.write(ProtocolResponse.error('Malformed JSON format'));
          }
          continue;
        }

        const responseStr = this._processRequest(frame.parsed, clientAddr);
        if (socket.writable) {
          socket.write(responseStr);
        }
      }
    });

    const cleanup = () => {
      if (this.connections.has(socket)) {
        console.log(`[Broker] Client disconnected: ${clientAddr}`);
        this.connections.delete(socket);
      }
    };

    socket.on('close', cleanup);
    socket.on('error', (err) => {
      console.error(`[Broker] Socket error on ${clientAddr}: ${err.message}`);
      cleanup();
    });
  }

  /**
   * Dispatcher for application protocol commands.
   * @param {object} request 
   * @param {string} clientAddr 
   * @returns {string} Encoded NDJSON response string
   */
  _processRequest(request, clientAddr) {
    if (!request || typeof request !== 'object' || !request.type) {
      return ProtocolResponse.error('Missing or invalid "type" field in request');
    }

    switch (request.type) {
      case 'PING':
        return ProtocolResponse.pong();

      case 'PRODUCE':
        if (typeof request.message !== 'string') {
          return ProtocolResponse.error('PRODUCE request must include a string "message"');
        }
        this.messages.push(request.message);
        console.log(`[Broker] Produced message from ${clientAddr}: "${request.message}" (Queue size: ${this.messages.length})`);
        return ProtocolResponse.produceAck();

      case 'CONSUME':
        if (this.messages.length > 0) {
          const msg = this.messages.shift();
          console.log(`[Broker] Consumed message for ${clientAddr}: "${msg}" (Remaining: ${this.messages.length})`);
          return ProtocolResponse.message(msg);
        } else {
          console.log(`[Broker] CONSUME request from ${clientAddr} (Queue empty)`);
          return ProtocolResponse.noMessages();
        }

      default:
        console.warn(`[Broker] Unknown request type "${request.type}" from ${clientAddr}`);
        return ProtocolResponse.error(`Unknown request type "${request.type}"`);
    }
  }
}

// Start broker if executed directly
const isDirectExecution = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];
if (isDirectExecution) {
  const broker = new BrokerServer(5000);
  broker.start().catch((err) => {
    console.error('Failed to start broker:', err);
    process.exit(1);
  });

  const shutdown = async () => {
    console.log('\n[Broker] Shutting down broker...');
    await broker.stop();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}
