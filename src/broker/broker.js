import { REQUEST_TYPES, ProtocolResponse } from '../protocol/types.js';
import { TopicManager } from './topic-manager.js';
import { ConsumerGroupManager } from './consumer-group-manager.js';
import { StorageEngine } from '../storage/storage-engine.js';
import { ClusterManager } from '../cluster/cluster-manager.js';
import { ReplicationManager } from '../cluster/replication-manager.js';
import path from 'path';

export class MessageBroker {
  /**
   * @param {object} [options={}]
   * @param {StorageEngine} [options.storageEngine]
   * @param {ClusterManager} [options.clusterManager]
   * @param {ReplicationManager} [options.replicationManager]
   * @param {object} [options.storageConfig]
   * @param {string} [options.brokerId='broker-1']
   * @param {string} [options.dataDir]
   */
  constructor(options = {}) {
    /** @type {ClusterManager} Multi-broker cluster manager */
    this.clusterManager = options.clusterManager || new ClusterManager(options);
    this.brokerId = this.clusterManager.localBrokerId;
    this.clusterManager.setBroker(this);

    // Separate storage directory per brokerId if default dataDir is used
    let dataDir = options.dataDir;
    if (!dataDir || dataDir === './data') {
      dataDir = path.join('./data', this.brokerId);
    }
    const storageConfig = { ...(options.storageConfig || options), dataDir };

    /** @type {TopicManager} Domain manager for topic lifecycle, partitions, and message logs */
    this.topicManager = new TopicManager();
    /** @type {ConsumerGroupManager} Domain manager for consumer groups and offsets */
    this.consumerGroupManager = new ConsumerGroupManager(this.topicManager);
    /** @type {StorageEngine} Persistent Storage Engine */
    this.storageEngine = options.storageEngine || new StorageEngine(storageConfig);
    /** @type {ReplicationManager} Partition replication manager */
    this.replicationManager = options.replicationManager || new ReplicationManager({
      clusterManager: this.clusterManager,
      topicManager: this.topicManager,
      storageEngine: this.storageEngine
    });
  }

  /**
   * Recovers state from storage on startup and registers replication assignments.
   */
  recover() {
    this.storageEngine.recoverAllState(this.topicManager, this.consumerGroupManager);
    for (const tObj of this.topicManager.listTopics()) {
      this.replicationManager.registerTopicReplication(tObj.name, tObj.partitions, 1);
    }
  }

  /**
   * Processes a validated protocol request object and returns a response object.
   * 
   * @param {object} request - Validated request object
   * @param {string} [clientAddr='local'] - Client identifier for diagnostic logging
   * @returns {Promise<object>|object} Response object
   */
  async handleRequest(request, clientAddr = 'local') {
    const uppercaseType = request.type.toUpperCase();
    const requestId = request.requestId;

    const getField = (fieldName) => {
      if (request.payload && typeof request.payload[fieldName] !== 'undefined') {
        return request.payload[fieldName];
      }
      if (typeof request[fieldName] !== 'undefined') {
        return request[fieldName];
      }
      return undefined;
    };

    const topic = getField('topic');
    const message = getField('message');
    const partition = getField('partition');
    const offset = getField('offset');
    const groupId = getField('groupId');
    const consumerId = getField('consumerId');
    const topics = getField('topics');

    switch (uppercaseType) {
      case REQUEST_TYPES.PING:
        return ProtocolResponse.pong(requestId);

      case REQUEST_TYPES.GET_CLUSTER_INFO: {
        const brokersList = this.clusterManager.getClusterInfo();
        console.log(`[Broker] Cluster info requested by ${clientAddr}: ${brokersList.length} broker(s)`);
        return ProtocolResponse.clusterInfo(brokersList, requestId);
      }

      case REQUEST_TYPES.CREATE_TOPIC: {
        const partitionsCount = getField('partitions') ?? 3;
        const repFactor = getField('replicationFactor') ?? 1;

        // Validate replicationFactor
        const valRes = this.replicationManager.validateReplicationFactor(repFactor);
        if (!valRes.valid) {
          return ProtocolResponse.error(valRes.error, requestId);
        }

        const result = this.topicManager.createTopic(topic, partitionsCount);
        if (result.success) {
          // Register topic partition replication assignments
          this.replicationManager.registerTopicReplication(topic, result.partitions, repFactor);

          // Initialize storage directories for each partition
          for (let p = 0; p < result.partitions; p++) {
            this.storageEngine.ensurePartitionDir(topic, p);
          }
          console.log(`[Broker] Created topic "${topic}" with ${result.partitions} partition(s), replicationFactor=${repFactor} requested by ${clientAddr}`);
          return ProtocolResponse.createTopicAck(topic, result.partitions, repFactor, requestId);
        } else {
          console.warn(`[Broker] Failed to create topic "${topic}" for ${clientAddr}: ${result.message}`);
          return ProtocolResponse.error({ code: result.code, message: result.message }, requestId);
        }
      }

      case REQUEST_TYPES.LIST_TOPICS: {
        const topicsList = this.topicManager.listTopics();
        console.log(`[Broker] Listing ${topicsList.length} topic(s) for ${clientAddr}`);
        return ProtocolResponse.topics(topicsList, requestId);
      }

      case REQUEST_TYPES.GET_TOPIC_INFO: {
        const result = this.topicManager.getTopicInfo(topic);
        if (result.success) {
          if (Array.isArray(result.partitionInfo)) {
            for (const pInfo of result.partitionInfo) {
              const pMeta = this.replicationManager.getPartitionMetadata(topic, pInfo.partition);
              if (pMeta) {
                pInfo.leader = pMeta.leader;
                pInfo.replicas = pMeta.replicas;
                pInfo.highWaterMark = pMeta.highWaterMark;
              }
            }
          }
          console.log(`[Broker] Topic info for "${topic}" requested by ${clientAddr}: ${result.messageCount} total message(s) across ${result.partitions} partition(s)`);
          return ProtocolResponse.topicInfo(result, requestId);
        } else {
          return ProtocolResponse.error({ code: result.code, message: result.message }, requestId);
        }
      }

      case REQUEST_TYPES.GET_PARTITION_INFO: {
        const result = this.topicManager.getPartitionInfo(topic, partition);
        if (result.success) {
          const pMeta = this.replicationManager.getPartitionMetadata(topic, partition);
          if (pMeta) {
            result.leader = pMeta.leader;
            result.replicas = pMeta.replicas;
            result.highWaterMark = pMeta.highWaterMark;
          }
          console.log(`[Broker] Partition info for "${topic}" partition ${partition} requested by ${clientAddr}: ${result.messageCount} message(s), nextOffset: ${result.nextOffset}`);
          return ProtocolResponse.partitionInfo(topic, result.partition, result.messageCount, requestId);
        } else {
          return ProtocolResponse.error({ code: result.code, message: result.message }, requestId);
        }
      }

      case REQUEST_TYPES.DELETE_TOPIC: {
        const result = this.topicManager.deleteTopic(topic);
        if (result.success) {
          console.log(`[Broker] Deleted topic "${topic}" requested by ${clientAddr}`);
          return ProtocolResponse.deleteTopicAck(topic, requestId);
        } else {
          console.warn(`[Broker] Failed to delete topic "${topic}" for ${clientAddr}: ${result.message}`);
          return ProtocolResponse.error({ code: result.code, message: result.message }, requestId);
        }
      }

      case REQUEST_TYPES.PRODUCE: {
        if (!this.topicManager.hasTopic(topic)) {
          console.warn(`[Broker] PRODUCE failed: Topic "${topic}" does not exist`);
          return ProtocolResponse.error({
            code: 'TOPIC_NOT_FOUND',
            message: `Topic '${topic}' does not exist`
          }, requestId);
        }

        const key = getField('key');
        const topicEntity = this.topicManager.getTopic(topic);
        const selection = topicEntity.selectPartitionForProduce(partition, key);
        if (selection.error) {
          return ProtocolResponse.error({
            code: selection.error.code,
            message: selection.error.message
          }, requestId);
        }
        const targetPartition = selection.partitionId;

        // Leader Write Enforcement Check
        if (!this.replicationManager.isLeader(topic, targetPartition)) {
          const leaderId = this.replicationManager.getLeader(topic, targetPartition);
          console.warn(`[Broker] PRODUCE rejected: Local broker '${this.brokerId}' is NOT_LEADER for '${topic}' partition ${targetPartition} (Leader: '${leaderId}')`);
          return ProtocolResponse.error({
            code: 'NOT_LEADER',
            message: `Broker '${this.brokerId}' is not the leader for topic '${topic}' partition ${targetPartition}`,
            topic,
            partition: targetPartition,
            leader: leaderId
          }, requestId);
        }

        const enqueueResult = this.topicManager.enqueue(topic, message, targetPartition, key);
        if (!enqueueResult.success) {
          console.warn(`[Broker] PRODUCE failed on topic "${topic}": ${enqueueResult.message}`);
          return ProtocolResponse.error({
            code: enqueueResult.code,
            message: enqueueResult.message
          }, requestId);
        }

        // Persist message record to local storage log segment
        try {
          this.storageEngine.appendMessage(topic, enqueueResult.partitionId, enqueueResult.offset, message);
        } catch (err) {
          console.error(`[Broker Storage Error] Failed to persist message to disk: ${err.message}`);
          return ProtocolResponse.error({
            code: 'STORAGE_ERROR',
            message: `Failed to persist message to disk: ${err.message}`
          }, requestId);
        }

        // Replicate record to follower replicas over inter-broker TCP
        await this.replicationManager.replicateToFollowers(topic, enqueueResult.partitionId, enqueueResult.offset, message, key);

        console.log(`[Broker] Produced message to "${topic}" (partition: ${enqueueResult.partitionId}, offset: ${enqueueResult.offset}) from ${clientAddr}: "${message}"`);
        return ProtocolResponse.produceAck(topic, enqueueResult.partitionId, enqueueResult.offset, requestId);
      }

      case REQUEST_TYPES.CONSUME: {
        if (!this.topicManager.hasTopic(topic)) {
          console.warn(`[Broker] CONSUME failed: Topic "${topic}" does not exist`);
          return ProtocolResponse.error({
            code: 'TOPIC_NOT_FOUND',
            message: `Topic '${topic}' does not exist`
          }, requestId);
        }

        // Case 1: Consume by explicit offset
        if (offset !== undefined && offset !== null) {
          const targetPartition = partition ?? 0;
          const readRes = this.topicManager.readOffset(topic, targetPartition, offset);
          if (readRes.success) {
            console.log(`[Broker] Consumed message at explicit offset ${offset} from "${topic}" (partition: ${targetPartition}) for ${clientAddr}`);
            return ProtocolResponse.message(topic, readRes.message, targetPartition, readRes.offset, requestId);
          } else {
            console.warn(`[Broker] CONSUME by offset ${offset} failed on topic "${topic}": ${readRes.message}`);
            return ProtocolResponse.error({
              code: readRes.code,
              message: readRes.message
            }, requestId);
          }
        }

        // Case 2: Group-based CONSUME
        if (groupId !== undefined && consumerId !== undefined) {
          const groupRes = this.consumerGroupManager.consume(groupId, consumerId, topic);
          if (!groupRes.success) {
            return ProtocolResponse.error({
              code: groupRes.code,
              message: groupRes.message
            }, requestId);
          }

          if (groupRes.message !== null) {
            console.log(`[Broker] Group "${groupId}" (${consumerId}) consumed message from "${topic}" (partition: ${groupRes.partition}, offset: ${groupRes.offset}) for ${clientAddr}`);
            return ProtocolResponse.message(topic, groupRes.message, groupRes.partition, groupRes.offset, requestId);
          } else {
            return ProtocolResponse.noMessages(topic, groupRes.partition, requestId);
          }
        }

        // Case 3: Legacy un-grouped CONSUME
        const dequeueResult = this.topicManager.dequeue(topic, partition);
        if (!dequeueResult.success) {
          return ProtocolResponse.error({
            code: dequeueResult.code,
            message: dequeueResult.message
          }, requestId);
        }

        if (dequeueResult.message !== null) {
          console.log(`[Broker] Legacy consumed message from "${topic}" (partition: ${dequeueResult.partitionId}, offset: ${dequeueResult.offset}) for ${clientAddr}`);
          return ProtocolResponse.message(topic, dequeueResult.message, dequeueResult.partitionId, dequeueResult.offset, requestId);
        } else {
          return ProtocolResponse.noMessages(topic, dequeueResult.partitionId, requestId);
        }
      }

      case REQUEST_TYPES.JOIN_GROUP: {
        const result = this.consumerGroupManager.joinGroup(groupId, consumerId, topics);
        if (result.success) {
          console.log(`[Broker] Consumer "${consumerId}" joined group "${groupId}" from ${clientAddr}`);
          return ProtocolResponse.joinGroupAck(groupId, consumerId, result.assignments, requestId);
        } else {
          return ProtocolResponse.error({ code: result.code, message: result.message }, requestId);
        }
      }

      case REQUEST_TYPES.LEAVE_GROUP: {
        const result = this.consumerGroupManager.leaveGroup(groupId, consumerId);
        if (result.success) {
          console.log(`[Broker] Consumer "${consumerId}" left group "${groupId}" from ${clientAddr}`);
          return ProtocolResponse.leaveGroupAck(groupId, consumerId, requestId);
        } else {
          return ProtocolResponse.error({ code: result.code, message: result.message }, requestId);
        }
      }

      case REQUEST_TYPES.COMMIT_OFFSET: {
        const result = this.consumerGroupManager.commitOffset(groupId, topic, partition, offset);
        if (result.success) {
          // Persist group committed offsets checkpoint to storage
          try {
            const groupInfoRes = this.consumerGroupManager.getGroupInfo(groupId);
            this.storageEngine.saveConsumerGroupOffsets(groupId, groupInfoRes);
          } catch (err) {
            console.error(`[Broker Storage Error] Failed to persist group offset checkpoint: ${err.message}`);
          }

          console.log(`[Broker] Committed offset ${offset} for group "${groupId}" on "${topic}" partition ${partition} from ${clientAddr}`);
          return ProtocolResponse.commitOffsetAck(groupId, topic, partition, offset, requestId);
        } else {
          return ProtocolResponse.error({ code: result.code, message: result.message }, requestId);
        }
      }

      case REQUEST_TYPES.GET_GROUP_INFO: {
        const result = this.consumerGroupManager.getGroupInfo(groupId);
        if (result.success) {
          console.log(`[Broker] Group info for "${groupId}" requested by ${clientAddr}`);
          return ProtocolResponse.groupInfo(result, requestId);
        } else {
          return ProtocolResponse.error({ code: result.code, message: result.message }, requestId);
        }
      }

      case REQUEST_TYPES.REPLICATE_ACK:
        return ProtocolResponse.replicateAck(getField('brokerId'), topic, partition, offset, requestId);

      case REQUEST_TYPES.REPLICA_SYNC_RESPONSE:
        return ProtocolResponse.replicaSyncResponse(topic, partition, getField('records') || [], requestId);

      default:
        console.warn(`[Broker] Unhandled request type "${request.type}" from ${clientAddr}`);
        return ProtocolResponse.error({ code: 'UNHANDLED_REQUEST_TYPE', message: `Unhandled request type "${request.type}"` }, requestId);
    }
  }

  /**
   * Returns count of queued messages for a specific topic or across all topics.
   * @param {string} [topic]
   * @returns {number}
   */
  getQueueSize(topic) {
    if (topic) {
      const info = this.topicManager.getTopicInfo(topic);
      return info.success ? info.messageCount : 0;
    }
    let total = 0;
    for (const tObj of this.topicManager.listTopics()) {
      total += this.topicManager.getTopicInfo(tObj.name).messageCount;
    }
    return total;
  }

  /**
   * Resets all in-memory topics, queues, consumer groups, and cluster connections.
   */
  clear() {
    this.topicManager.clear();
    this.consumerGroupManager.clear();
    this.storageEngine.close();
    this.clusterManager.stop();
  }
}

// Export BrokerServer from server.js for backward compatibility
export { BrokerServer } from './server.js';
