/**
 * ClusterManager — Multi-Broker Cluster Topology & Health Manager
 * 
 * Manages static cluster topology configuration, identifies local vs remote
 * broker nodes, handles inter-broker TCP handshakes (BROKER_HELLO / BROKER_HELLO_ACK),
 * sends periodic heartbeats (BROKER_PING / BROKER_PONG), detects node failures ('down'),
 * handles node recovery ('alive'), and exposes cluster status via GET_CLUSTER_INFO.
 */

import net from 'net';
import { BrokerNode } from './broker-node.js';
import { StreamFramer } from '../protocol/framing.js';
import { ProtocolEncoder, ProtocolDecoder } from '../protocol/codec.js';
import { ProtocolRequest, ProtocolResponse, REQUEST_TYPES } from '../protocol/types.js';

export class ClusterManager {
  /**
   * @param {object} [options={}]
   * @param {string} [options.brokerId='broker-1'] - Unique local broker ID
   * @param {string} [options.host='127.0.0.1'] - Local host IP
   * @param {number} [options.port=5000] - Local port
   * @param {Array<{ id: string, host: string, port: number }>} [options.clusterConfig] - Static cluster topology list
   * @param {number} [options.heartbeatIntervalMs=1000] - Heartbeat check interval in ms
   * @param {number} [options.heartbeatTimeoutMs=2000] - Heartbeat timeout in ms
   * @param {import('../broker/broker.js').MessageBroker} [options.broker]
   */
  constructor(options = {}) {
    this.localBrokerId = options.brokerId || 'broker-1';
    this.localHost = options.host || '127.0.0.1';
    this.localPort = Number(options.port) || 5000;
    this.heartbeatIntervalMs = options.heartbeatIntervalMs || 1000;
    this.heartbeatTimeoutMs = options.heartbeatTimeoutMs || 2000;
    this.broker = options.broker || null;

    /** @type {Map<string, BrokerNode>} Cluster nodes indexed by brokerId */
    this.nodes = new Map();

    const clusterConfig = Array.isArray(options.clusterConfig) && options.clusterConfig.length > 0
      ? options.clusterConfig
      : [
          { id: this.localBrokerId, host: this.localHost, port: this.localPort }
        ];

    for (const nodeConfig of clusterConfig) {
      const isLocal = nodeConfig.id === this.localBrokerId;
      const node = new BrokerNode({
        id: nodeConfig.id,
        host: nodeConfig.host,
        port: nodeConfig.port,
        status: isLocal ? 'alive' : 'unknown'
      });

      if (isLocal) {
        node.markAlive();
      }

      this.nodes.set(nodeConfig.id, node);
    }

    this.active = false;
    this.timer = null;
  }

  /**
   * Sets parent broker instance reference for routing replication and election messages.
   * @param {any} broker 
   */
  setBroker(broker) {
    this.broker = broker;
  }

  /**
   * Starts periodic heartbeat check loop and inter-broker connection attempts.
   */
  start() {
    if (this.active) return;
    this.active = true;

    // Trigger immediate connection attempt to remote cluster nodes
    this._pingRemoteBrokers();

    // Schedule periodic heartbeat loop
    this.timer = setInterval(() => {
      if (this.active) {
        this._pingRemoteBrokers();
        this._checkDeadNodes();
      }
    }, this.heartbeatIntervalMs);
  }

  /**
   * Gracefully stops the cluster manager and closes open inter-broker TCP sockets.
   */
  stop() {
    this.active = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    for (const node of this.nodes.values()) {
      if (node.id !== this.localBrokerId) {
        node.markDown();
      }
    }
  }

  /**
   * Returns cluster node info array for GET_CLUSTER_INFO requests.
   * @returns {Array<{ id: string, host: string, port: number, status: string }>}
   */
  getClusterInfo() {
    const list = [];
    for (const node of this.nodes.values()) {
      list.push(node.toJSON());
    }
    // Sort deterministically by brokerId
    list.sort((a, b) => a.id.localeCompare(b.id));
    return list;
  }

  /**
   * Marks node alive and notifies parent broker.
   * @param {BrokerNode} node 
   */
  _markNodeAlive(node) {
    const prevStatus = node.status;
    node.markAlive();
    if (prevStatus !== 'alive' && this.broker && this.broker.leaderElectionManager) {
      console.log(`[ClusterManager] Node '${node.id}' transitioned to ALIVE`);
    }
  }

  /**
   * Marks node down and notifies parent broker.
   * @param {BrokerNode} node 
   */
  _markNodeDown(node) {
    const prevStatus = node.status;
    node.markDown();
    if (this.active && prevStatus === 'alive' && this.broker && this.broker.leaderElectionManager) {
      console.warn(`[ClusterManager] Node '${node.id}' transitioned to DOWN. Notifying LeaderElectionManager...`);
      this.broker.leaderElectionManager.handleBrokerFailure(node.id);
    }
  }

  /**
   * Connects or sends BROKER_PING to a remote broker node over TCP socket.
   * @param {BrokerNode} node 
   */
  _connectOrPingNode(node) {
    if (node.id === this.localBrokerId) return;

    // If socket is active and connected, send BROKER_PING heartbeat
    if (node.socket && !node.socket.destroyed && node.socket.writable) {
      try {
        const pingReq = ProtocolEncoder.encode(ProtocolRequest.brokerPing(this.localBrokerId));
        node.socket.write(pingReq);
      } catch (err) {
        this._markNodeDown(node);
      }
      return;
    }

    // Otherwise, initiate new TCP connection and execute BROKER_HELLO handshake
    const socket = net.createConnection({ port: node.port, host: node.host });
    const framer = new StreamFramer();
    socket.setKeepAlive(true);

    socket.on('connect', () => {
      const helloReq = ProtocolEncoder.encode(
        ProtocolRequest.brokerHello(this.localBrokerId, this.localHost, this.localPort)
      );
      socket.write(helloReq);
    });

    socket.on('data', (chunk) => {
      const frames = framer.feed(chunk);
      for (const frame of frames) {
        if (frame.error) continue;
        const decoded = ProtocolDecoder.decode(frame.raw);
        if (decoded.error) continue;

        const res = decoded.parsed;
        const uppercaseType = res.type ? res.type.toUpperCase() : '';

        if (uppercaseType === 'BROKER_HELLO_ACK') {
          node.socket = socket;
          this._markNodeAlive(node);
        } else if (uppercaseType === 'BROKER_PONG') {
          node.updateHeartbeat();
        } else if (uppercaseType === REQUEST_TYPES.REPLICATE_RECORD && this.broker) {
          const { topic, partition, offset, message } = res.payload;
          const resp = this.broker.replicationManager.handleIncomingReplicateRecord(topic, partition, offset, message);
          if (socket.writable) {
            socket.write(ProtocolEncoder.encode(resp));
          }
        } else if (uppercaseType === REQUEST_TYPES.REPLICA_SYNC && this.broker) {
          const { brokerId, topic, partition, fromOffset } = res.payload;
          const resp = this.broker.replicationManager.handleIncomingReplicaSync(brokerId, topic, partition, fromOffset);
          if (socket.writable) {
            socket.write(ProtocolEncoder.encode(resp));
          }
        } else if (uppercaseType === REQUEST_TYPES.LEADER_ANNOUNCE && this.broker && this.broker.leaderElectionManager) {
          const { topic, partition, leader, leaderEpoch, replicas } = res.payload;
          const resp = this.broker.leaderElectionManager.handleLeaderAnnounce(topic, partition, leader, leaderEpoch, replicas);
          if (socket.writable) {
            socket.write(ProtocolEncoder.encode(resp));
          }
        }
      }
    });

    const cleanup = () => {
      if (node.socket === socket) {
        this._markNodeDown(node);
      }
    };

    socket.on('error', cleanup);
    socket.on('close', cleanup);
  }

  /**
   * Pings all remote brokers in the cluster.
   */
  _pingRemoteBrokers() {
    for (const node of this.nodes.values()) {
      if (node.id !== this.localBrokerId) {
        this._connectOrPingNode(node);
      }
    }
  }

  /**
   * Checks for nodes that haven't responded within heartbeatTimeoutMs.
   */
  _checkDeadNodes() {
    const now = Date.now();
    for (const node of this.nodes.values()) {
      if (node.id === this.localBrokerId) continue;

      if (node.status === 'alive') {
        if (node.lastHeartbeat && (now - node.lastHeartbeat > this.heartbeatTimeoutMs)) {
          console.warn(`[ClusterManager] Broker ${node.id} missed heartbeats for ${now - node.lastHeartbeat}ms. Marking status DOWN.`);
          this._markNodeDown(node);
        }
      }
    }
  }

  /**
   * Handles incoming BROKER_HELLO handshake from another connecting broker.
   * 
   * @param {string} remoteBrokerId 
   * @param {string} remoteHost 
   * @param {number} remotePort 
   * @param {net.Socket} socket 
   * @returns {object} Response object (BROKER_HELLO_ACK or ERROR)
   */
  handleIncomingHello(remoteBrokerId, remoteHost, remotePort, socket) {
    if (!this.nodes.has(remoteBrokerId)) {
      console.warn(`[ClusterManager] Rejected unknown broker handshake: "${remoteBrokerId}"`);
      return ProtocolResponse.error({
        code: 'UNKNOWN_BROKER',
        message: `Broker "${remoteBrokerId}" is not in the static cluster configuration`
      });
    }

    const node = this.nodes.get(remoteBrokerId);
    node.socket = socket;
    this._markNodeAlive(node);

    const cleanup = () => {
      if (node.socket === socket) {
        this._markNodeDown(node);
      }
    };
    socket.on('close', cleanup);
    socket.on('error', cleanup);

    console.log(`[ClusterManager] Received BROKER_HELLO from "${remoteBrokerId}" (${remoteHost}:${remotePort}). Status: ALIVE.`);
    return ProtocolResponse.brokerHelloAck(this.localBrokerId, this.localHost, this.localPort);
  }

  /**
   * Handles incoming BROKER_PING heartbeat from another broker.
   * 
   * @param {string} remoteBrokerId 
   * @returns {object} Response object (BROKER_PONG or ERROR)
   */
  handleIncomingPing(remoteBrokerId) {
    if (this.nodes.has(remoteBrokerId)) {
      const node = this.nodes.get(remoteBrokerId);
      node.updateHeartbeat();
    }
    return ProtocolResponse.brokerPong(this.localBrokerId);
  }
}
