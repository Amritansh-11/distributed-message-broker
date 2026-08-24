import { REQUEST_TYPES, ProtocolResponse } from '../protocol/types.js';
import { TopicManager } from './topic-manager.js';

export class MessageBroker {
  constructor() {
    /** @type {TopicManager} Domain manager for topic lifecycle and message queues */
    this.topicManager = new TopicManager();
  }

  /**
   * Processes a validated protocol request object and returns a response object.
   * 
   * @param {object} request - Validated request object
   * @param {string} [clientAddr='local'] - Client identifier for diagnostic logging
   * @returns {object} Response object
   */
  handleRequest(request, clientAddr = 'local') {
    const uppercaseType = request.type.toUpperCase();
    const requestId = request.requestId;

    const topic = request.payload?.topic || request.topic;
    const message = request.payload?.message || request.message;

    switch (uppercaseType) {
      case REQUEST_TYPES.PING:
        return ProtocolResponse.pong(requestId);

      case REQUEST_TYPES.CREATE_TOPIC: {
        const result = this.topicManager.createTopic(topic);
        if (result.success) {
          console.log(`[Broker] Created topic "${topic}" requested by ${clientAddr}`);
          return ProtocolResponse.createTopicAck(topic, requestId);
        } else {
          console.warn(`[Broker] Failed to create topic "${topic}" for ${clientAddr}: ${result.message}`);
          return ProtocolResponse.error({ code: result.code, message: result.message }, requestId);
        }
      }

      case REQUEST_TYPES.LIST_TOPICS: {
        const topics = this.topicManager.listTopics();
        console.log(`[Broker] Listing ${topics.length} topic(s) for ${clientAddr}`);
        return ProtocolResponse.topics(topics, requestId);
      }

      case REQUEST_TYPES.GET_TOPIC_INFO: {
        const result = this.topicManager.getTopicInfo(topic);
        if (result.success) {
          console.log(`[Broker] Topic info for "${topic}" requested by ${clientAddr}: ${result.messageCount} message(s)`);
          return ProtocolResponse.topicInfo(topic, result.messageCount, requestId);
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

        this.topicManager.enqueue(topic, message);
        const info = this.topicManager.getTopicInfo(topic);
        console.log(`[Broker] Produced message to "${topic}" from ${clientAddr}: "${message}" (Queue size: ${info.messageCount})`);
        return ProtocolResponse.produceAck(topic, requestId);
      }

      case REQUEST_TYPES.CONSUME: {
        if (!this.topicManager.hasTopic(topic)) {
          console.warn(`[Broker] CONSUME failed: Topic "${topic}" does not exist`);
          return ProtocolResponse.error({
            code: 'TOPIC_NOT_FOUND',
            message: `Topic '${topic}' does not exist`
          }, requestId);
        }

        const dequeueResult = this.topicManager.dequeue(topic);
        if (dequeueResult.message !== null) {
          const info = this.topicManager.getTopicInfo(topic);
          console.log(`[Broker] Consumed message from "${topic}" for ${clientAddr}: "${dequeueResult.message}" (Remaining: ${info.messageCount})`);
          return ProtocolResponse.message(topic, dequeueResult.message, requestId);
        } else {
          console.log(`[Broker] CONSUME request from ${clientAddr} on topic "${topic}" (Queue empty)`);
          return ProtocolResponse.noMessages(topic, requestId);
        }
      }

      default:
        console.warn(`[Broker] Unhandled request type "${request.type}" from ${clientAddr}`);
        return ProtocolResponse.error(`Unhandled request type "${request.type}"`, requestId);
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
    for (const tName of this.topicManager.listTopics()) {
      total += this.topicManager.getTopicInfo(tName).messageCount;
    }
    return total;
  }

  /**
   * Resets all in-memory topics and queues.
   */
  clear() {
    this.topicManager.clear();
  }
}

// Export BrokerServer from server.js for backward compatibility
export { BrokerServer } from './server.js';

