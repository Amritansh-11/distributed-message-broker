/**
 * ReplicationManager — Partition Data Replication & Replica Synchronization Manager
 * 
 * Manages deterministic leader/replica partition assignments, leader write rule
 * enforcement, inter-broker record replication (REPLICATE_RECORD / REPLICATE_ACK),
 * high-water mark calculations, replica state tracking, and follower catch-up sync.
 */

import { ProtocolRequest, ProtocolResponse } from '../protocol/types.js';
import { StreamFramer } from '../protocol/framing.js';
import { ProtocolEncoder, ProtocolDecoder } from '../protocol/codec.js';
import { Partition } from '../broker/partition.js';
import net from 'net';

export class ReplicationManager {
  /**
   * @param {object} options 
   * @param {import('./cluster-manager.js').ClusterManager} options.clusterManager 
   * @param {import('../broker/topic-manager.js').TopicManager} options.topicManager 
   * @param {import('../storage/storage-engine.js').StorageEngine} options.storageEngine 
   */
  constructor(options) {
    this.clusterManager = options.clusterManager;
    this.topicManager = options.topicManager;
    this.storageEngine = options.storageEngine;
    this.localBrokerId = this.clusterManager.localBrokerId;

    /** @type {Map<string, { leader: string, replicas: string[], replicationFactor: number }>} Key: "topic:partition" */
    this.partitionMetadata = new Map();

    /** @type {Map<string, Map<string, { status: string, lastReplicatedOffset: number }>>} Key: "topic:partition" -> Map<brokerId, state> */
    this.replicaStates = new Map();

    /** @type {Map<string, number>} Key: "topic:partition" -> High Water Mark offset */
    this.highWaterMarks = new Map();
  }

  /**
   * Validates replicationFactor against total cluster size.
   * @param {number} replicationFactor 
   * @returns {{ valid: boolean, error?: { code: string, message: string } }}
   */
  validateReplicationFactor(replicationFactor) {
    const totalBrokers = this.clusterManager.nodes.size;
    if (typeof replicationFactor !== 'number' || !Number.isInteger(replicationFactor) || replicationFactor < 1) {
      return {
        valid: false,
        error: {
          code: 'INVALID_REPLICATION_FACTOR',
          message: 'Replication factor must be an integer >= 1'
        }
      };
    }

    if (replicationFactor > totalBrokers) {
      return {
        valid: false,
        error: {
          code: 'INVALID_REPLICATION_FACTOR',
          message: `Replication factor ${replicationFactor} exceeds total cluster brokers count (${totalBrokers})`
        }
      };
    }

    return { valid: true };
  }

  /**
   * Computes deterministic partition leader and replica assignments across cluster brokers.
   * 
   * @param {string} topic 
   * @param {number} partitionCount 
   * @param {number} [replicationFactor=1] 
   */
  registerTopicReplication(topic, partitionCount, replicationFactor = 1) {
    const brokersList = Array.from(this.clusterManager.nodes.keys()).sort((a, b) => a.localeCompare(b));
    const N = brokersList.length;

    for (let p = 0; p < partitionCount; p++) {
      const pKey = `${topic}:${p}`;
      const leaderIdx = p % N;
      const leader = brokersList[leaderIdx];

      const replicas = [];
      for (let r = 0; r < replicationFactor; r++) {
        const repBroker = brokersList[(leaderIdx + r) % N];
        replicas.push(repBroker);
      }

      this.partitionMetadata.set(pKey, {
        leader,
        replicas,
        replicationFactor
      });

      const states = new Map();
      for (const repId of replicas) {
        states.set(repId, {
          status: repId === leader ? 'CAUGHT_UP' : 'UNKNOWN',
          lastReplicatedOffset: -1
        });
      }
      this.replicaStates.set(pKey, states);
      this.highWaterMarks.set(pKey, -1);
    }
  }

  /**
   * Gets partition metadata including leader, replicas, replicaStates, and highWaterMark.
   * 
   * @param {string} topic 
   * @param {number} partition 
   */
  getPartitionMetadata(topic, partition) {
    const pKey = `${topic}:${partition}`;
    const meta = this.partitionMetadata.get(pKey);
    if (!meta) return null;

    const states = this.replicaStates.get(pKey);
    const replicaStatesObj = {};
    if (states) {
      for (const [bId, st] of states.entries()) {
        replicaStatesObj[bId] = { ...st };
      }
    }

    const hwm = this.highWaterMarks.get(pKey) ?? -1;

    return {
      partition,
      leader: meta.leader,
      replicas: meta.replicas,
      replicationFactor: meta.replicationFactor,
      replicaStates: replicaStatesObj,
      highWaterMark: hwm
    };
  }

  /**
   * Checks if local broker is the leader for a partition.
   * 
   * @param {string} topic 
   * @param {number} partition 
   * @returns {boolean}
   */
  isLeader(topic, partition) {
    const meta = this.partitionMetadata.get(`${topic}:${partition}`);
    if (!meta) return true; // Default single broker fallback
    return meta.leader === this.localBrokerId;
  }

  /**
   * Gets current leader broker ID for a partition.
   * 
   * @param {string} topic 
   * @param {number} partition 
   * @returns {string}
   */
  getLeader(topic, partition) {
    const meta = this.partitionMetadata.get(`${topic}:${partition}`);
    return meta ? meta.leader : this.localBrokerId;
  }

  /**
   * Replicates a produced record from Leader to all follower replicas over TCP sockets.
   * Updates High-Water Mark upon receiving acknowledgments.
   * 
   * @param {string} topic 
   * @param {number} partition 
   * @param {number} offset 
   * @param {any} message 
   * @param {string|number} [key] 
   * @returns {Promise<boolean>}
   */
  async replicateToFollowers(topic, partition, offset, message, key) {
    const pKey = `${topic}:${partition}`;
    const meta = this.partitionMetadata.get(pKey);
    if (!meta || meta.replicas.length <= 1) {
      // Single broker or replicationFactor = 1
      this.highWaterMarks.set(pKey, offset);
      return true;
    }

    const states = this.replicaStates.get(pKey);
    // Leader's local offset
    if (states && states.has(this.localBrokerId)) {
      states.get(this.localBrokerId).lastReplicatedOffset = offset;
      states.get(this.localBrokerId).status = 'CAUGHT_UP';
    }

    const followers = meta.replicas.filter(id => id !== this.localBrokerId);

    const replicationPromises = followers.map(followerId => {
      return new Promise((resolve) => {
        const node = this.clusterManager.nodes.get(followerId);
        if (!node || node.status === 'down' || !node.socket || node.socket.destroyed) {
          if (states && states.has(followerId)) {
            states.get(followerId).status = 'DOWN';
          }
          return resolve(false);
        }

        const socket = node.socket;
        const framer = new StreamFramer();
        const reqObj = ProtocolRequest.replicateRecord(topic, partition, offset, message, key);
        const wireData = ProtocolEncoder.encode(reqObj);

        let timeoutTimer = null;

        const onData = (chunk) => {
          const frames = framer.feed(chunk);
          for (const frame of frames) {
            if (frame.error) continue;
            const decoded = ProtocolDecoder.decode(frame.raw);
            if (decoded.error) continue;

            if (decoded.parsed.type === 'REPLICATE_ACK') {
              if (timeoutTimer) clearTimeout(timeoutTimer);
              socket.removeListener('data', onData);
              if (states && states.has(followerId)) {
                states.get(followerId).lastReplicatedOffset = offset;
                states.get(followerId).status = 'CAUGHT_UP';
              }
              return resolve(true);
            }
          }
        };

        timeoutTimer = setTimeout(() => {
          socket.removeListener('data', onData);
          if (states && states.has(followerId)) {
            states.get(followerId).status = 'DOWN';
          }
          resolve(false);
        }, 3000);

        socket.on('data', onData);
        socket.write(wireData);
      });
    });

    await Promise.all(replicationPromises);

    // Calculate High-Water Mark (min offset among active caught-up replicas)
    this.updateHighWaterMark(topic, partition);
    return true;
  }

  /**
   * Recalculates High-Water Mark for a partition based on replica states.
   * @param {string} topic 
   * @param {number} partition 
   */
  updateHighWaterMark(topic, partition) {
    const pKey = `${topic}:${partition}`;
    const states = this.replicaStates.get(pKey);
    if (!states) return;

    let minOffset = Infinity;
    for (const [bId, st] of states.entries()) {
      if (st.status === 'CAUGHT_UP' || st.status === 'ALIVE') {
        minOffset = Math.min(minOffset, st.lastReplicatedOffset);
      }
    }

    if (minOffset !== Infinity && minOffset >= 0) {
      this.highWaterMarks.set(pKey, minOffset);
    }
  }

  /**
   * Handles incoming REPLICATE_RECORD on follower replica.
   * Persists record to follower's in-memory Partition and disk StorageEngine.
   * Auto-initializes local partition entity if not created yet.
   * 
   * @param {string} topic 
   * @param {number} partitionId 
   * @param {number} offset 
   * @param {any} message 
   * @returns {object} REPLICATE_ACK response
   */
  handleIncomingReplicateRecord(topic, partitionId, offset, message) {
    let topicEntity = this.topicManager.getTopic(topic);
    if (!topicEntity) {
      this.topicManager.createTopic(topic, Math.max(3, partitionId + 1));
      topicEntity = this.topicManager.getTopic(topic);
    } else if (!topicEntity.hasPartition(partitionId)) {
      topicEntity.partitions.set(partitionId, new Partition(partitionId));
    }
    this.storageEngine.ensurePartitionDir(topic, partitionId);

    const partition = topicEntity.partitions.get(partitionId);
    const writeResult = partition.enqueueWithOffset(offset, message);

    if (writeResult.error) {
      return ProtocolResponse.error({
        code: writeResult.error.code,
        message: writeResult.error.message
      });
    }

    // Persist follower replica record to disk
    try {
      this.storageEngine.appendMessage(topic, partitionId, offset, message);
    } catch (err) {
      console.error(`[Replica Storage Error] ${err.message}`);
    }

    return ProtocolResponse.replicateAck(this.localBrokerId, topic, partitionId, offset);
  }

  /**
   * Handles REPLICA_SYNC catch-up request from a recovering follower.
   * 
   * @param {string} followerBrokerId 
   * @param {string} topic 
   * @param {number} partitionId 
   * @param {number} fromOffset 
   * @returns {object} REPLICA_SYNC_RESPONSE response
   */
  handleIncomingReplicaSync(followerBrokerId, topic, partitionId, fromOffset) {
    const topicEntity = this.topicManager.getTopic(topic);
    if (!topicEntity) {
      return ProtocolResponse.replicaSyncResponse(topic, partitionId, []);
    }

    const partition = topicEntity.partitions.get(partitionId);
    if (!partition) {
      return ProtocolResponse.replicaSyncResponse(topic, partitionId, []);
    }

    const records = [];
    for (let off = fromOffset; off < partition.nextOffset; off++) {
      const readRes = partition.readOffset(off);
      if (readRes.success) {
        records.push({ offset: readRes.offset, message: readRes.message });
      }
    }

    // Update replica state for syncing follower
    const pKey = `${topic}:${partitionId}`;
    const states = this.replicaStates.get(pKey);
    if (states && states.has(followerBrokerId)) {
      states.get(followerBrokerId).status = 'SYNCING';
    }

    return ProtocolResponse.replicaSyncResponse(topic, partitionId, records);
  }

  /**
   * Triggers catchup synchronization for local follower partitions from Leader.
   * 
   * @param {string} topic 
   * @param {number} partitionId 
   */
  async syncFollowerPartition(topic, partitionId) {
    const leaderId = this.getLeader(topic, partitionId);
    if (leaderId === this.localBrokerId) return; // Local node is leader

    const leaderNode = this.clusterManager.nodes.get(leaderId);
    if (!leaderNode || leaderNode.status === 'down') return;

    let topicEntity = this.topicManager.getTopic(topic);
    if (!topicEntity) {
      this.topicManager.createTopic(topic, Math.max(3, partitionId + 1));
      topicEntity = this.topicManager.getTopic(topic);
    } else if (!topicEntity.hasPartition(partitionId)) {
      topicEntity.partitions.set(partitionId, new Partition(partitionId));
    }
    this.storageEngine.ensurePartitionDir(topic, partitionId);

    const partition = topicEntity.partitions.get(partitionId);
    const fromOffset = partition.nextOffset;

    return new Promise((resolve) => {
      const socket = net.createConnection({ port: leaderNode.port, host: leaderNode.host });
      const framer = new StreamFramer();

      socket.on('connect', () => {
        const syncReq = ProtocolRequest.replicaSync(this.localBrokerId, topic, partitionId, fromOffset);
        socket.write(ProtocolEncoder.encode(syncReq));
      });

      socket.on('data', (chunk) => {
        const frames = framer.feed(chunk);
        for (const frame of frames) {
          if (frame.error) continue;
          const decoded = ProtocolDecoder.decode(frame.raw);
          if (decoded.error) continue;

          if (decoded.parsed.type === 'REPLICA_SYNC_RESPONSE') {
            const records = decoded.parsed.payload.records || [];
            for (const rec of records) {
              partition.enqueueWithOffset(rec.offset, rec.message);
              this.storageEngine.appendMessage(topic, partitionId, rec.offset, rec.message);
            }
            socket.end();
            return resolve(true);
          }
        }
      });

      socket.on('error', () => resolve(false));
      socket.on('close', () => resolve(false));
    });
  }
}
