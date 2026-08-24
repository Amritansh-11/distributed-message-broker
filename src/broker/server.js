import net from 'net';
import { fileURLToPath } from 'url';
import { StreamFramer } from '../protocol/framing.js';
import { ProtocolDecoder, ProtocolEncoder } from '../protocol/codec.js';
import { RequestValidator } from '../protocol/validator.js';
import { ProtocolResponse } from '../protocol/types.js';
import { MessageBroker } from './broker.js';

export class BrokerServer {
  /**
   * @param {number | object} [portOrOptions=5000] 
   * @param {string} [host='127.0.0.1'] 
   */
  constructor(portOrOptions = 5000, host = '127.0.0.1') {
    if (typeof portOrOptions === 'object' && portOrOptions !== null) {
      this.port = portOrOptions.port || 5000;
      this.host = portOrOptions.host || '127.0.0.1';
    } else {
      this.port = portOrOptions;
      this.host = host;
    }
    this.broker = new MessageBroker();
    this.server = null;
    this.connections = new Set();
  }

  /**
   * Starts the TCP Broker server and binds to port.
   * @returns {Promise<void>}
   */
  start() {
    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => {
        this._handleConnection(socket);
      });

      this.server.on('error', (err) => {
        console.error(`[Broker Server Error] ${err.message}`);
        reject(err);
      });

      this.server.listen(this.port, this.host, () => {
        console.log(`[Broker] TCP Message Broker listening on tcp://${this.host}:${this.port}`);
        resolve();
      });
    });
  }

  /**
   * Gracefully stops the broker server and destroys active client sockets.
   * @returns {Promise<void>}
   */
  stop() {
    return new Promise((resolve) => {
      if (!this.server) return resolve();

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
   * Handles TCP socket connection pipeline.
   * Pipeline: TCP -> Framing -> Decoder -> Validator -> Broker -> Encoder -> TCP
   * 
   * @param {net.Socket} socket 
   */
  _handleConnection(socket) {
    const clientAddr = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`[Broker] Client connected: ${clientAddr}`);

    this.connections.add(socket);
    const framer = new StreamFramer();

    socket.on('data', (chunk) => {
      const frames = framer.feed(chunk);

      for (const frame of frames) {
        // 1. Framing error (e.g. max frame size exceeded)
        if (frame.error) {
          console.warn(`[Broker] Framing error from ${clientAddr}: ${frame.error.message}`);
          this._sendResponse(socket, ProtocolResponse.error(frame.error.message));
          continue;
        }

        // 2. Decoder (Parse raw wire payload)
        const decoded = ProtocolDecoder.decode(frame.raw);
        if (decoded.error) {
          console.warn(`[Broker] Decoding error from ${clientAddr}: ${decoded.error.message}`);
          this._sendResponse(socket, ProtocolResponse.error(decoded.error.message));
          continue;
        }

        // 3. Request Validator (Check request structure and bounds)
        const validation = RequestValidator.validate(decoded.parsed);
        if (!validation.valid) {
          console.warn(`[Broker] Request validation failed from ${clientAddr}: ${validation.error}`);
          this._sendResponse(socket, ProtocolResponse.error(validation.error));
          continue;
        }

        // 4. Core Message Broker Engine
        const responseObj = this.broker.handleRequest(decoded.parsed, clientAddr);

        // 5. Encoder & Framing -> Write to TCP Socket
        this._sendResponse(socket, responseObj);
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
   * Encodes response object and sends to client socket.
   * @param {net.Socket} socket 
   * @param {object} responseObj 
   */
  _sendResponse(socket, responseObj) {
    if (socket.writable) {
      const wireData = ProtocolEncoder.encode(responseObj);
      socket.write(wireData);
    }
  }

  /**
   * Helper accessor to access topicManager directly in tests
   */
  get topicManager() {
    return this.broker.topicManager;
  }
}

// Start broker if executed directly
const isDirectExecution = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];
if (isDirectExecution) {
  const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 5000;
  const broker = new BrokerServer(PORT);
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

export { BrokerServer as Server };

