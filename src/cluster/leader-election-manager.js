/**
 * LeaderElectionManager — Deterministic Partition Leader Election & Failover Engine
 * 
 * Evaluates candidate replicas during broker failure, deterministically selects new leaders
 * based on highest replicated offset and broker ID tie-breaking, increments leader epoch,
 * enforces stale leader protection (STALE_LEADER / NOT_LEADER), and broadcasts LEADER_ANNOUNCE
 * across cluster nodes.
 */

import { ProtocolRequest, ProtocolResponse } from '../protocol/types.js';
import { ProtocolEncoder } from '../protocol/codec.js';

export class LeaderElectionManager {
  /**
   * @param {object} options 
   * @param {import('./cluster-manager.js').ClusterManager} options.clusterManager 
   * @param {import('./replication-manager.js').ReplicationManager} options.replicationManager 
   * @param {import('../broker/topic-manager.js').TopicManager} options.topicManager 
   */
  constructor(options) {
    this.clusterManager = options.clusterManager;
    this.replicationManager = options.replicationManager;
    this.topicManager = options.topicManager;
    this.localBrokerId = this.clusterManager.localBrokerId;
  }

  /**
   * Deterministically elects a new leader for a specific topic partition.
   * Ranking:
   * 1. Highest replicated / local partition offset.
   * 2. Alphabetical broker ID ascending (e.g. 'broker-2' < 'broker-3').
   * 
   * @param {string} topic 
   * @param {number} partitionId 
   * @returns {{ success: boolean, leader?: string, leaderEpoch?: number, code?: string, message?: string }}
   */
  electLeader(topic, partitionId) {
    const meta = this.replicationManager.getPartitionMetadata(topic, partitionId);
    if (!meta) {
      return {
        success: false,
        code: 'PARTITION_NOT_FOUND',
        message: `Partition ${partitionId} does not exist for topic '${topic}'`
      };
    }

    // Get local partition offset if available
    let localLastOffset = -1;
    const tEntity = this.topicManager.getTopic(topic);
    if (tEntity && tEntity.hasPartition(partitionId)) {
      const pEntity = tEntity.partitions.get(partitionId);
      localLastOffset = pEntity.nextOffset - 1;
    }

    // Identify candidate ALIVE replicas
    const candidates = [];
    for (const repId of meta.replicas) {
      const node = this.clusterManager.nodes.get(repId);
      if (node && node.status === 'alive') {
        let lastOffset = -1;
        if (repId === this.localBrokerId) {
          lastOffset = localLastOffset;
        } else {
          const repState = meta.replicaStates[repId];
          if (repState && typeof repState.lastReplicatedOffset === 'number' && repState.lastReplicatedOffset >= 0) {
            lastOffset = repState.lastReplicatedOffset;
          } else {
            lastOffset = localLastOffset;
          }
        }
        candidates.push({ brokerId: repId, lastOffset });
      }
    }

    if (candidates.length === 0) {
      console.warn(`[LeaderElection] Election failed for '${topic}' partition ${partitionId}: No ALIVE candidates available`);
      this.replicationManager.setPartitionStatus(topic, partitionId, 'NO_LEADER', null);
      return {
        success: false,
        code: 'PARTITION_UNAVAILABLE',
        message: `Partition '${topic}' partition ${partitionId} has no available leader`
      };
    }

    // Sort candidates deterministically:
    // 1. Highest lastOffset descending
    // 2. Alphabetical brokerId ascending
    candidates.sort((a, b) => {
      if (b.lastOffset !== a.lastOffset) {
        return b.lastOffset - a.lastOffset;
      }
      return a.brokerId.localeCompare(b.brokerId);
    });

    const winningCandidate = candidates[0];
    const newLeader = winningCandidate.brokerId;
    const newEpoch = (meta.leaderEpoch || 0) + 1;

    console.log(`[LeaderElection] Elected new leader '${newLeader}' for '${topic}' partition ${partitionId} (Epoch: ${newEpoch}, Offset: ${winningCandidate.lastOffset})`);

    // Update local partition replication metadata
    this.replicationManager.setPartitionLeaderAndEpoch(topic, partitionId, newLeader, newEpoch, 'HEALTHY');

    // Broadcast LEADER_ANNOUNCE to remote cluster peers
    this.broadcastLeaderAnnounce(topic, partitionId, newLeader, newEpoch, meta.replicas);

    return {
      success: true,
      leader: newLeader,
      leaderEpoch: newEpoch
    };
  }

  /**
   * Triggers failover leader election for all partitions led by a failed broker.
   * 
   * @param {string} failedBrokerId 
   */
  handleBrokerFailure(failedBrokerId) {
    console.warn(`[LeaderElection] Handling broker failure for '${failedBrokerId}'...`);
    const allMeta = this.replicationManager.getAllPartitionMetadata();

    for (const meta of allMeta) {
      if (meta.leader === failedBrokerId) {
        console.log(`[LeaderElection] Partition '${meta.topic}' partition ${meta.partition} lost leader '${failedBrokerId}'. Triggering election...`);
        this.electLeader(meta.topic, meta.partition);
      }
    }
  }

  /**
   * Handles incoming LEADER_ANNOUNCE request from another broker.
   * 
   * @param {string} topic 
   * @param {number} partitionId 
   * @param {string} newLeader 
   * @param {number} newLeaderEpoch 
   * @param {string[]} [replicas] 
   * @returns {object} Response object
   */
  handleLeaderAnnounce(topic, partitionId, newLeader, newLeaderEpoch, replicas) {
    const meta = this.replicationManager.getPartitionMetadata(topic, partitionId);
    if (meta && newLeaderEpoch < meta.leaderEpoch) {
      console.warn(`[LeaderElection] Rejected stale LEADER_ANNOUNCE for '${topic}' partition ${partitionId}: epoch ${newLeaderEpoch} < current ${meta.leaderEpoch}`);
      return ProtocolResponse.error({
        code: 'STALE_LEADER',
        message: `Stale leader epoch ${newLeaderEpoch}. Current epoch is ${meta.leaderEpoch}`,
        topic,
        partition: partitionId,
        leader: meta.leader,
        leaderEpoch: meta.leaderEpoch
      });
    }

    console.log(`[LeaderElection] Received LEADER_ANNOUNCE: '${topic}' partition ${partitionId} -> leader: '${newLeader}', epoch: ${newLeaderEpoch}`);
    this.replicationManager.setPartitionLeaderAndEpoch(topic, partitionId, newLeader, newLeaderEpoch, 'HEALTHY', replicas);

    // If local broker is a follower for this partition, initiate catch-up sync if returning
    if (newLeader !== this.localBrokerId) {
      this.replicationManager.syncFollowerPartition(topic, partitionId);
    }

    return ProtocolResponse.leaderAnnounceAck(topic, partitionId, newLeader, newLeaderEpoch);
  }

  /**
   * Broadcasts LEADER_ANNOUNCE to all active remote cluster nodes.
   * 
   * @param {string} topic 
   * @param {number} partitionId 
   * @param {string} leader 
   * @param {number} leaderEpoch 
   * @param {string[]} replicas 
   */
  broadcastLeaderAnnounce(topic, partitionId, leader, leaderEpoch, replicas) {
    const announceReq = ProtocolEncoder.encode(
      ProtocolRequest.leaderAnnounce(topic, partitionId, leader, leaderEpoch, replicas)
    );

    for (const [nodeId, node] of this.clusterManager.nodes.entries()) {
      if (nodeId !== this.localBrokerId && node.status === 'alive' && node.socket && !node.socket.destroyed) {
        try {
          node.socket.write(announceReq);
        } catch (err) {
          console.error(`[LeaderElection] Failed to send LEADER_ANNOUNCE to '${nodeId}': ${err.message}`);
        }
      }
    }
  }
}
