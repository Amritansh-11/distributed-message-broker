/**
 * Protocol Types and Message Factory Definitions
 * 
 * Defines standard command types, response types, and object creators
 * for the Distributed Message Broker protocol.
 */

export const REQUEST_TYPES = {
  PING: 'PING',
  CREATE_TOPIC: 'CREATE_TOPIC',
  LIST_TOPICS: 'LIST_TOPICS',
  GET_TOPIC_INFO: 'GET_TOPIC_INFO',
  GET_PARTITION_INFO: 'GET_PARTITION_INFO',
  DELETE_TOPIC: 'DELETE_TOPIC',
  PRODUCE: 'PRODUCE',
  CONSUME: 'CONSUME',
  JOIN_GROUP: 'JOIN_GROUP',
  LEAVE_GROUP: 'LEAVE_GROUP',
  COMMIT_OFFSET: 'COMMIT_OFFSET',
  GET_GROUP_INFO: 'GET_GROUP_INFO',
  GET_CLUSTER_INFO: 'GET_CLUSTER_INFO',
  BROKER_HELLO: 'BROKER_HELLO',
  BROKER_PING: 'BROKER_PING',
  REPLICATE_RECORD: 'REPLICATE_RECORD',
  REPLICATE_ACK: 'REPLICATE_ACK',
  REPLICA_SYNC: 'REPLICA_SYNC',
  REPLICA_SYNC_RESPONSE: 'REPLICA_SYNC_RESPONSE',
  LEADER_ANNOUNCE: 'LEADER_ANNOUNCE'
};

export const RESPONSE_TYPES = {
  PONG: 'PONG',
  CREATE_TOPIC_ACK: 'CREATE_TOPIC_ACK',
  TOPICS: 'TOPICS',
  TOPIC_INFO: 'TOPIC_INFO',
  PARTITION_INFO: 'PARTITION_INFO',
  DELETE_TOPIC_ACK: 'DELETE_TOPIC_ACK',
  PRODUCE_ACK: 'PRODUCE_ACK',
  MESSAGE: 'MESSAGE',
  NO_MESSAGES: 'NO_MESSAGES',
  JOIN_GROUP_ACK: 'JOIN_GROUP_ACK',
  LEAVE_GROUP_ACK: 'LEAVE_GROUP_ACK',
  COMMIT_OFFSET_ACK: 'COMMIT_OFFSET_ACK',
  GROUP_INFO: 'GROUP_INFO',
  CLUSTER_INFO: 'CLUSTER_INFO',
  BROKER_HELLO_ACK: 'BROKER_HELLO_ACK',
  BROKER_PONG: 'BROKER_PONG',
  REPLICATE_ACK: 'REPLICATE_ACK',
  REPLICA_SYNC_RESPONSE: 'REPLICA_SYNC_RESPONSE',
  LEADER_ANNOUNCE_ACK: 'LEADER_ANNOUNCE_ACK',
  ERROR: 'ERROR'
};

export const ProtocolRequest = {
  ping: (requestId) => (requestId ? { requestId, type: REQUEST_TYPES.PING } : { type: REQUEST_TYPES.PING }),
  
  createTopic: (topic, partitions = 3, replicationFactor = 1, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.CREATE_TOPIC,
    payload: { topic, partitions, replicationFactor }
  }),

  listTopics: (requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.LIST_TOPICS,
    payload: {}
  }),

  getTopicInfo: (topic, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.GET_TOPIC_INFO,
    payload: { topic }
  }),

  getPartitionInfo: (topic, partition, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.GET_PARTITION_INFO,
    payload: { topic, partition }
  }),

  deleteTopic: (topic, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.DELETE_TOPIC,
    payload: { topic }
  }),

  produce: (topic, message, partition = undefined, key = undefined, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.PRODUCE,
    payload: {
      topic,
      message,
      ...(partition !== undefined && partition !== null && { partition }),
      ...(key !== undefined && key !== null && { key })
    }
  }),

  consume: (topic, partition = undefined, offset = undefined, groupId = undefined, consumerId = undefined, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.CONSUME,
    payload: {
      topic,
      ...(partition !== undefined && partition !== null && { partition }),
      ...(offset !== undefined && offset !== null && { offset }),
      ...(groupId !== undefined && groupId !== null && { groupId }),
      ...(consumerId !== undefined && consumerId !== null && { consumerId })
    }
  }),

  joinGroup: (groupId, consumerId, topics = undefined, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.JOIN_GROUP,
    payload: {
      groupId,
      consumerId,
      ...(topics !== undefined && { topics })
    }
  }),

  leaveGroup: (groupId, consumerId, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.LEAVE_GROUP,
    payload: {
      groupId,
      consumerId
    }
  }),

  commitOffset: (groupId, topic, partition, offset, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.COMMIT_OFFSET,
    payload: {
      groupId,
      topic,
      partition,
      offset
    }
  }),

  getGroupInfo: (groupId, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.GET_GROUP_INFO,
    payload: {
      groupId
    }
  }),

  getClusterInfo: (requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.GET_CLUSTER_INFO,
    payload: {}
  }),

  brokerHello: (brokerId, host, port, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.BROKER_HELLO,
    payload: {
      brokerId,
      host,
      port
    }
  }),

  brokerPing: (brokerId, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.BROKER_PING,
    payload: {
      brokerId
    }
  }),

  replicateRecord: (topic, partition, offset, message, key = undefined, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.REPLICATE_RECORD,
    payload: {
      topic,
      partition,
      offset,
      message,
      ...(key !== undefined && key !== null && { key })
    }
  }),

  replicateAck: (brokerId, topic, partition, offset, success = true, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.REPLICATE_ACK,
    success,
    payload: {
      brokerId,
      topic,
      partition,
      offset
    }
  }),

  replicaSync: (brokerId, topic, partition, fromOffset = 0, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.REPLICA_SYNC,
    payload: {
      brokerId,
      topic,
      partition,
      fromOffset
    }
  }),

  replicaSyncResponse: (topic, partition, records = [], requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.REPLICA_SYNC_RESPONSE,
    success: true,
    payload: {
      topic,
      partition,
      records
    }
  }),

  leaderAnnounce: (topic, partition, leader, leaderEpoch, replicas = [], requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.LEADER_ANNOUNCE,
    payload: {
      topic,
      partition,
      leader,
      leaderEpoch,
      replicas
    }
  })
};

export const ProtocolResponse = {
  pong: (requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.PONG,
    success: true
  }),

  createTopicAck: (topic, partitions = 3, replicationFactor = 1, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.CREATE_TOPIC_ACK,
    success: true,
    payload: { topic, partitions, replicationFactor }
  }),

  topics: (topicsList, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.TOPICS,
    success: true,
    payload: { topics: topicsList }
  }),

  topicInfo: (topicOrObj, partitions, messageCount, partitionInfo, requestId) => {
    if (typeof topicOrObj === 'object' && topicOrObj !== null) {
      const { topic, partitions: p, messageCount: m, partitionInfo: pi } = topicOrObj;
      return {
        ...(requestId && { requestId }),
        type: RESPONSE_TYPES.TOPIC_INFO,
        success: true,
        payload: { topic, partitions: p, messageCount: m, partitionInfo: pi }
      };
    }
    return {
      ...(requestId && { requestId }),
      type: RESPONSE_TYPES.TOPIC_INFO,
      success: true,
      payload: {
        topic: topicOrObj,
        partitions: partitions !== undefined ? partitions : 3,
        messageCount: messageCount !== undefined ? messageCount : 0,
        ...(partitionInfo && { partitionInfo })
      }
    };
  },

  partitionInfo: (topic, partition, messageCount, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.PARTITION_INFO,
    success: true,
    payload: { topic, partition, messageCount }
  }),

  deleteTopicAck: (topic, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.DELETE_TOPIC_ACK,
    success: true,
    payload: { topic }
  }),

  produceAck: (topic, partition, offset, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.PRODUCE_ACK,
    success: true,
    payload: {
      topic,
      ...(partition !== undefined && partition !== null && { partition }),
      ...(offset !== undefined && offset !== null && { offset })
    }
  }),

  message: (topic, msg, partition, offset, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.MESSAGE,
    success: true,
    payload: {
      topic,
      message: msg,
      ...(partition !== undefined && partition !== null && { partition }),
      ...(offset !== undefined && offset !== null && { offset })
    }
  }),

  noMessages: (topic, partition, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.NO_MESSAGES,
    success: true,
    payload: {
      topic,
      ...(partition !== undefined && partition !== null && { partition })
    }
  }),

  joinGroupAck: (groupId, consumerId, assignments, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.JOIN_GROUP_ACK,
    success: true,
    payload: {
      groupId,
      consumerId,
      ...(assignments !== undefined && { assignments })
    }
  }),

  leaveGroupAck: (groupId, consumerId, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.LEAVE_GROUP_ACK,
    success: true,
    payload: {
      groupId,
      consumerId
    }
  }),

  commitOffsetAck: (groupId, topic, partition, offset, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.COMMIT_OFFSET_ACK,
    success: true,
    payload: {
      groupId,
      topic,
      partition,
      offset
    }
  }),

  groupInfo: (groupInfoObj, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.GROUP_INFO,
    success: true,
    payload: typeof groupInfoObj === 'object' && groupInfoObj.groupId ? groupInfoObj : { groupId: groupInfoObj }
  }),

  clusterInfo: (brokersList, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.CLUSTER_INFO,
    success: true,
    payload: {
      brokers: brokersList
    }
  }),

  brokerHelloAck: (brokerId, host, port, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.BROKER_HELLO_ACK,
    success: true,
    payload: {
      brokerId,
      host,
      port
    }
  }),

  brokerPong: (brokerId, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.BROKER_PONG,
    success: true,
    payload: {
      brokerId
    }
  }),

  replicateAck: (brokerId, topic, partition, offset, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.REPLICATE_ACK,
    success: true,
    payload: {
      brokerId,
      topic,
      partition,
      offset
    }
  }),

  replicaSyncResponse: (topic, partition, records = [], requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.REPLICA_SYNC_RESPONSE,
    success: true,
    payload: {
      topic,
      partition,
      records
    }
  }),

  leaderAnnounceAck: (topic, partition, leader, leaderEpoch, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.LEADER_ANNOUNCE_ACK,
    success: true,
    payload: {
      topic,
      partition,
      leader,
      leaderEpoch
    }
  }),

  error: (errorPayload, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.ERROR,
    success: false,
    error: typeof errorPayload === 'string' ? { code: 'UNKNOWN_ERROR', message: errorPayload } : errorPayload
  })
};
