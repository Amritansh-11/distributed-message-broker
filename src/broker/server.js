/**
 * BrokerServer — Production TCP Message Broker Server & Management Server
 * 
 * Manages TCP server socket lifecycle, inter-broker handshakes, structured logging,
 * metrics collection, HTTP management endpoints (/metrics, /health, /cluster),
 * and graceful SIGINT/SIGTERM shutdown sequence.
 */

import net from 'net';
import path from 'path';
import { fileURLToPath } from 'url';
import { StreamFramer } from '../protocol/framing.js';
import { ProtocolDecoder, ProtocolEncoder } from '../protocol/codec.js';
import { RequestValidator } from '../protocol/validator.js';
import { MessageBroker } from './broker.js';
import { ProtocolResponse, REQUEST_TYPES } from '../protocol/types.js';
import { ConfigLoader } from '../config/config.js';
import { logger } from '../utils/logger.js';
import { metricsCollector } from '../metrics/metrics-collector.js';
import { ManagementServer } from '../server/management-server.js';

export class BrokerServer {
  /**
   * @param {object|number} [options={}] - Options object or listening port number
   * @param {string} [hostArg='127.0.0.1'] - Listening host string if first arg is port number
   * @param {object} [optionsArg={}] - Additional options if first arg is port number
   */
  constructor(options = {}, hostArg, optionsArg) {
    let opts = options;
    if (typeof options === 'number') {
      const portNum = options;
      const hostStr = typeof hostArg === 'string' ? hostArg : '127.0.0.1';
      const extraOpts = typeof optionsArg === 'object' && optionsArg !== null ? optionsArg : {};
      opts = { port: portNum, host: hostStr, ...extraOpts };
    }

    const envConfig = ConfigLoader.loadConfig();
    this.port = Number(opts.port) || envConfig.port;
    this.host = opts.host || envConfig.host;
    this.httpPort = Number(opts.httpPort) || (this.port + 3000);
    this.brokerId = opts.brokerId || envConfig.brokerId;

    logger.setBrokerId(this.brokerId);
    if (opts.logLevel || envConfig.logLevel) {
      logger.setLevel(opts.logLevel || envConfig.logLevel);
    }

    this.broker = opts.broker || new MessageBroker({ ...envConfig, ...opts, port: this.port, host: this.host, brokerId: this.brokerId });
    this.server = null;
    this.connections = new Set();
    this.managementServer = opts.managementServer || new ManagementServer({
      port: this.httpPort,
      host: this.host === '127.0.0.1' ? '127.0.0.1' : '0.0.0.0',
      brokerId: this.brokerId,
      broker: this.broker
    });
    this.stopping = false;
  }

  /**
   * Exposes clusterManager from underlying broker instance.
   */
  get clusterManager() {
    return this.broker ? this.broker.clusterManager : null;
  }

  /**
   * Starts TCP broker server and HTTP management server.
   * @returns {Promise<void>}
   */
  start() {
    return new Promise((resolve, reject) => {
      // Recover persistent storage state on server startup
      if (this.broker && typeof this.broker.recover === 'function') {
        try {
          this.broker.recover();
        } catch (err) {
          logger.warn('Storage recovery warning', { error: err.message });
        }
      }

      this.server = net.createServer((socket) => this._handleConnection(socket));

      this.server.on('error', (err) => {
        logger.error('Broker TCP server error', { error: err.message });
        reject(err);
      });

      this.server.listen(this.port, this.host, async () => {
        logger.info('TCP Message Broker listening', { port: this.port, host: this.host });
        console.log(`[Broker ${this.brokerId}] TCP Message Broker listening on tcp://${this.host}:${this.port}`);

        // Start cluster heartbeats and inter-broker connections
        if (this.broker.clusterManager) {
          this.broker.clusterManager.start();
        }

        // Start HTTP management observability server
        try {
          await this.managementServer.start();
        } catch (err) {
          logger.warn('HTTP Management server failed to start', { error: err.message });
        }

        resolve();
      });
    });
  }

  /**
   * Handles individual TCP socket connection lifecycle.
   * @param {net.Socket} socket 
   */
  _handleConnection(socket) {
    if (this.stopping) {
      socket.destroy();
      return;
    }

    const clientAddr = `${socket.remoteAddress}:${socket.remotePort}`;
    this.connections.add(socket);
    metricsCollector.increment('connections_total');
    metricsCollector.increment('active_connections');

    const framer = new StreamFramer();

    socket.on('data', async (chunk) => {
      let frames;
      try {
        frames = framer.feed(chunk);
      } catch (err) {
        logger.warn('Framing error from client', { remoteBroker: clientAddr, error: err.message });
        const errResp = ProtocolEncoder.encode(ProtocolResponse.error(err.message));
        socket.write(errResp);
        socket.destroy();
        return;
      }

      for (const frame of frames) {
        if (frame.error) {
          logger.warn('Frame error from client', { remoteBroker: clientAddr, error: frame.error });
          const errResp = ProtocolEncoder.encode(ProtocolResponse.error(frame.error));
          socket.write(errResp);
          continue;
        }

        const decoded = ProtocolDecoder.decode(frame.raw);
        if (decoded.error) {
          logger.warn('Protocol decode error from client', { remoteBroker: clientAddr, error: decoded.error });
          const errResp = ProtocolEncoder.encode(ProtocolResponse.error(decoded.error));
          socket.write(errResp);
          continue;
        }

        const validation = RequestValidator.validate(decoded.parsed);
        if (!validation.valid) {
          logger.warn('Request validation error from client', { remoteBroker: clientAddr, error: validation.error });
          const errResp = ProtocolEncoder.encode(ProtocolResponse.error(validation.error));
          socket.write(errResp);
          continue;
        }

        const reqObj = decoded.parsed;
        const uppercaseType = reqObj.type.toUpperCase();

        // Handle inter-broker handshakes directly
        if (uppercaseType === REQUEST_TYPES.BROKER_HELLO && this.broker.clusterManager) {
          const { brokerId, host, port } = reqObj.payload;
          const res = this.broker.clusterManager.handleIncomingHello(brokerId, host, port, socket);
          socket.write(ProtocolEncoder.encode(res));
          continue;
        }

        if (uppercaseType === REQUEST_TYPES.BROKER_PING && this.broker.clusterManager) {
          const { brokerId } = reqObj.payload;
          const res = this.broker.clusterManager.handleIncomingPing(brokerId);
          socket.write(ProtocolEncoder.encode(res));
          continue;
        }

        if (uppercaseType === REQUEST_TYPES.REPLICATE_RECORD && this.broker.replicationManager) {
          const { topic, partition, offset, message } = reqObj.payload;
          const res = this.broker.replicationManager.handleIncomingReplicateRecord(topic, partition, offset, message);
          socket.write(ProtocolEncoder.encode(res));
          continue;
        }

        if (uppercaseType === REQUEST_TYPES.REPLICA_SYNC && this.broker.replicationManager) {
          const { brokerId, topic, partition, fromOffset } = reqObj.payload;
          const res = this.broker.replicationManager.handleIncomingReplicaSync(brokerId, topic, partition, fromOffset);
          socket.write(ProtocolEncoder.encode(res));
          continue;
        }

        if (uppercaseType === REQUEST_TYPES.LEADER_ANNOUNCE && this.broker.leaderElectionManager) {
          const { topic, partition, leader, leaderEpoch, replicas } = reqObj.payload;
          const res = this.broker.leaderElectionManager.handleLeaderAnnounce(topic, partition, leader, leaderEpoch, replicas);
          socket.write(ProtocolEncoder.encode(res));
          continue;
        }

        // Metrics tracking for PRODUCE and CONSUME
        const startTime = Date.now();
        const responseObj = await this.broker.handleRequest(reqObj, clientAddr);
        const durationMs = Date.now() - startTime;

        if (uppercaseType === REQUEST_TYPES.PRODUCE) {
          if (responseObj.success) {
            metricsCollector.increment('messages_produced_total');
            metricsCollector.recordLatency('produce', durationMs);
          } else {
            metricsCollector.increment('produce_errors_total');
          }
        } else if (uppercaseType === REQUEST_TYPES.CONSUME) {
          if (responseObj.success) {
            metricsCollector.increment('messages_consumed_total');
            metricsCollector.recordLatency('consume', durationMs);
          } else {
            metricsCollector.increment('consume_errors_total');
          }
        }

        const encodedResp = ProtocolEncoder.encode(responseObj);
        socket.write(encodedResp);
      }
    });

    const cleanup = () => {
      if (this.connections.has(socket)) {
        this.connections.delete(socket);
        metricsCollector.decrement('active_connections');
      }
    };

    socket.on('close', cleanup);
    socket.on('error', (err) => {
      cleanup();
    });
  }

  /**
   * Gracefully shuts down the TCP server, HTTP management server, flushes pending storage,
   * and closes all client connections.
   * @returns {Promise<void>}
   */
  stop() {
    if (this.stopping) return Promise.resolve();
    this.stopping = true;

    return new Promise((resolve) => {
      logger.info('Shutting down BrokerServer gracefully...', { brokerId: this.brokerId });

      // Stop management HTTP server
      if (this.managementServer) {
        this.managementServer.stop().catch(() => {});
      }

      // Flush pending storage log buffers to disk
      if (this.broker && this.broker.storageEngine) {
        try {
          this.broker.storageEngine.flushAll();
        } catch (err) {
          logger.warn('Error flushing storage on shutdown', { error: err.message });
        }
      }

      // Clear in-memory broker state and stop cluster heartbeats
      if (this.broker) {
        this.broker.clear();
      }

      // Destroy open client TCP sockets
      for (const socket of this.connections) {
        try {
          socket.destroy();
        } catch (err) {}
      }
      this.connections.clear();

      // Close TCP server
      if (this.server) {
        this.server.close(() => {
          logger.info('Broker server shut down gracefully', { brokerId: this.brokerId });
          console.log(`[Broker ${this.brokerId}] Broker server shut down gracefully`);
          resolve();
        });
      } else {
        resolve();
      }
    });
  }
}

// CLI Execution Entry Point
const currentFilePath = fileURLToPath(import.meta.url);
const entryFilePath = process.argv[1] ? path.resolve(process.argv[1]) : '';

if (entryFilePath && (currentFilePath === entryFilePath || entryFilePath.endsWith('server.js'))) {
  const cliArgs = {};
  for (const arg of process.argv.slice(2)) {
    if (arg.startsWith('--id=')) cliArgs.brokerId = arg.split('=')[1];
    if (arg.startsWith('--port=')) cliArgs.port = Number(arg.split('=')[1]);
    if (arg.startsWith('--host=')) cliArgs.host = arg.split('=')[1];
    if (arg.startsWith('--http-port=')) cliArgs.httpPort = Number(arg.split('=')[1]);
    if (arg.startsWith('--log-level=')) cliArgs.logLevel = arg.split('=')[1];
  }

  const server = new BrokerServer(cliArgs);
  
  const shutdown = async (signal) => {
    console.log(`\nReceived ${signal}. Initiating graceful shutdown...`);
    await server.stop();
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  server.start().catch((err) => {
    console.error(`Failed to start broker: ${err.message}`);
    process.exit(1);
  });
}
