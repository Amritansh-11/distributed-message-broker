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
  DELETE_TOPIC: 'DELETE_TOPIC',
  PRODUCE: 'PRODUCE',
  CONSUME: 'CONSUME'
};

export const RESPONSE_TYPES = {
  PONG: 'PONG',
  CREATE_TOPIC_ACK: 'CREATE_TOPIC_ACK',
  TOPICS: 'TOPICS',
  TOPIC_INFO: 'TOPIC_INFO',
  DELETE_TOPIC_ACK: 'DELETE_TOPIC_ACK',
  PRODUCE_ACK: 'PRODUCE_ACK',
  MESSAGE: 'MESSAGE',
  NO_MESSAGES: 'NO_MESSAGES',
  ERROR: 'ERROR'
};

export const ProtocolRequest = {
  ping: (requestId) => (requestId ? { requestId, type: REQUEST_TYPES.PING } : { type: REQUEST_TYPES.PING }),
  createTopic: (topic, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.CREATE_TOPIC,
    payload: { topic }
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
  deleteTopic: (topic, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.DELETE_TOPIC,
    payload: { topic }
  }),
  produce: (topic, message, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.PRODUCE,
    payload: { topic, message }
  }),
  consume: (topic, requestId) => ({
    ...(requestId && { requestId }),
    type: REQUEST_TYPES.CONSUME,
    payload: { topic }
  })
};

export const ProtocolResponse = {
  pong: (requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.PONG,
    success: true
  }),
  createTopicAck: (topic, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.CREATE_TOPIC_ACK,
    success: true,
    payload: { topic }
  }),
  topics: (topicsList, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.TOPICS,
    success: true,
    payload: { topics: topicsList }
  }),
  topicInfo: (topic, messageCount, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.TOPIC_INFO,
    success: true,
    payload: { topic, messageCount }
  }),
  deleteTopicAck: (topic, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.DELETE_TOPIC_ACK,
    success: true,
    payload: { topic }
  }),
  produceAck: (topic, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.PRODUCE_ACK,
    success: true,
    payload: { topic }
  }),
  message: (topic, msg, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.MESSAGE,
    success: true,
    payload: { topic, message: msg }
  }),
  noMessages: (topic, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.NO_MESSAGES,
    success: true,
    payload: { topic }
  }),
  error: (errorPayload, requestId) => ({
    ...(requestId && { requestId }),
    type: RESPONSE_TYPES.ERROR,
    success: false,
    error: errorPayload
  })
};
