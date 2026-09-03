import net from 'net';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { StreamFramer } from '../protocol/framing.js';
import { ProtocolDecoder, ProtocolEncoder } from '../protocol/codec.js';
import { RequestValidator } from '../protocol/validator.js';
import { ProtocolResponse, REQUEST_TYPES } from '../protocol/types.js';
import { MessageBroker } from './broker.js';

export class BrokerServer {
  /**
   * @param {number | object} [portOrOptions=5000] 
   * @param {string} [host='127.0.0.1'] 
   * @param {object} [options={}]
   */
  constructor(portOrOptions = 5000, host = '127.0.0.1', options = {}) {
    let brokerOptions = {};
    if (typeof portOrOptions === 'object' && portOrOptions !== null) {
      this.port = portOrOptions.port || 5000;
      this.host = portOrOptions.host || '127.0.0.1';
      brokerOptions = portOrOptions;
    } else {
      this.port = portOrOptions;
      this.host = host;
      brokerOptions = { port: this.port, host: this.host, ...options };
    }

    this.brokerId = brokerOptions.brokerId || process.env.BROKER_ID || 'broker-1';
    brokerOptions.brokerId = this.brokerId;
    brokerOptions.port = this.port;
    brokerOptions.host = this.host;

    this.broker = new MessageBroker(brokerOptions);
    this.server = null;
    this.connections = new Set();
  }

  /**
   * Helper accessor to access ClusterManager.
   */
  get clusterManager() {
    return this.broker.clusterManager;
  }

  /**
   * Helper accessor to access ReplicationManager.
   */
  get replicationManager() {
    return this.broker.replicationManager;
  }

  /**
   * Starts the TCP Broker server after completing disk state recovery
   * and starting cluster manager inter-broker communication.
   * 
   * @returns {Promise<void>}
   */
  async start() {
    // Perform disk state recovery before listening for TCP connections
    try {
      this.broker.recover();
    } catch (err) {
      console.error(`[Broker Server Error] Disk state recovery failed: ${err.message}`);
      throw err;
    }

    // Start cluster manager inter-broker heartbeat loop & connections
    this.clusterManager.start();

    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => {
        this._handleConnection(socket);
      });

      this.server.on('error', (err) => {
        console.error(`[Broker Server Error] ${err.message}`);
        reject(err);
      });

      this.server.listen(this.port, this.host, () => {
        console.log(`[Broker ${this.brokerId}] TCP Message Broker listening on tcp://${this.host}:${this.port}`);
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
      this.clusterManager.stop();

      if (!this.server) return resolve();

      for (const socket of this.connections) {
        socket.destroy();
      }
      this.connections.clear();

      this.server.close(() => {
        console.log(`[Broker ${this.brokerId}] Broker server shut down gracefully`);
        resolve();
      });
    });
  }

  /**
   * Handles TCP socket connection pipeline.
   * Pipeline: TCP -> Framing -> Decoder -> Validator -> (Cluster / Broker Engine) -> Encoder -> TCP
   * 
   * @param {net.Socket} socket 
   */
  _handleConnection(socket) {
    const clientAddr = `${socket.remoteAddress}:${socket.remotePort}`;

    this.connections.add(socket);
    const framer = new StreamFramer();

    socket.on('data', async (chunk) => {
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

        const req = decoded.parsed;
        const uppercaseType = req.type.toUpperCase();

        // 4A. Inter-Broker Protocol Messages (BROKER_HELLO / BROKER_PING)
        if (uppercaseType === REQUEST_TYPES.BROKER_HELLO) {
          const { brokerId, host, port } = req.payload;
          const responseObj = this.clusterManager.handleIncomingHello(brokerId, host, port, socket);
          this._sendResponse(socket, responseObj);
          continue;
        }

        if (uppercaseType === REQUEST_TYPES.BROKER_PING) {
          const { brokerId } = req.payload;
          const responseObj = this.clusterManager.handleIncomingPing(brokerId);
          this._sendResponse(socket, responseObj);
          continue;
        }

        // 4B. Inter-Broker Replication Messages (REPLICATE_RECORD / REPLICA_SYNC)
        if (uppercaseType === REQUEST_TYPES.REPLICATE_RECORD) {
          const { topic, partition, offset, message } = req.payload;
          const responseObj = this.replicationManager.handleIncomingReplicateRecord(topic, partition, offset, message);
          this._sendResponse(socket, responseObj);
          continue;
        }

        if (uppercaseType === REQUEST_TYPES.REPLICA_SYNC) {
          const { brokerId, topic, partition, fromOffset } = req.payload;
          const responseObj = this.replicationManager.handleIncomingReplicaSync(brokerId, topic, partition, fromOffset);
          this._sendResponse(socket, responseObj);
          continue;
        }

        // 4C. Core Message Broker Domain Engine
        const responseObj = await this.broker.handleRequest(req, clientAddr);

        // 5. Encoder & Framing -> Write to TCP Socket
        this._sendResponse(socket, responseObj);
      }
    });

    const cleanup = () => {
      if (this.connections.has(socket)) {
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

// Start broker if executed directly via CLI
const isDirectExecution = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];
if (isDirectExecution) {
  // Parse CLI args e.g. --id=broker-1 --port=5000 --host=127.0.0.1 --dataDir=./data/broker-1 --config=cluster.json
  const args = process.argv.slice(2);
  let brokerId = process.env.BROKER_ID || 'broker-1';
  let port = process.env.BROKER_PORT ? parseInt(process.env.BROKER_PORT, 10) : 5000;
  let host = process.env.BROKER_HOST || '127.0.0.1';
  let dataDir = process.env.DATA_DIR;
  let clusterConfig = null;

  for (const arg of args) {
    if (arg.startsWith('--id=')) brokerId = arg.replace('--id=', '');
    if (arg.startsWith('--port=')) port = parseInt(arg.replace('--port=', ''), 10);
    if (arg.startsWith('--host=')) host = arg.replace('--host=', '');
    if (arg.startsWith('--dataDir=')) dataDir = arg.replace('--dataDir=', '');
    if (arg.startsWith('--config=')) {
      const configPath = arg.replace('--config=', '');
      if (fs.existsSync(configPath)) {
        clusterConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'));
      }
    }
  }

  const broker = new BrokerServer({ brokerId, port, host, dataDir, clusterConfig });
  broker.start().catch((err) => {
    console.error(`Failed to start broker ${brokerId}:`, err);
    process.exit(1);
  });

  const shutdown = async () => {
    console.log(`\n[Broker ${brokerId}] Shutting down broker...`);
    await broker.stop();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

export { BrokerServer as Server };
