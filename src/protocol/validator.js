/**
 * RequestValidator — Protocol Request Schema & Validation Layer
 * 
 * Enforces strict request schema constraints before requests are passed to
 * the core Message Broker domain engine.
 */

import { REQUEST_TYPES } from './types.js';
import { TopicManager } from '../broker/topic-manager.js';

export class RequestValidator {
  /**
   * Validates a decoded protocol request object.
   * 
   * @param {any} request - Decoded request object from ProtocolDecoder
   * @returns {{ valid: boolean, error?: string | object }} Validation outcome
   */
  static validate(request) {
    if (!request || typeof request !== 'object' || Array.isArray(request)) {
      return {
        valid: false,
        error: 'Invalid request format: Request must be a non-null JSON object'
      };
    }

    if (!request.type || typeof request.type !== 'string') {
      return {
        valid: false,
        error: 'Missing or invalid "type" field in request header'
      };
    }

    const uppercaseType = request.type.toUpperCase();
    const validTypes = Object.values(REQUEST_TYPES);

    if (!validTypes.includes(uppercaseType)) {
      return {
        valid: false,
        error: `Unknown request type "${request.type}"`
      };
    }

    // Helper to extract fields from payload or top-level
    const getField = (fieldName) => {
      if (request.payload && typeof request.payload[fieldName] !== 'undefined') {
        return request.payload[fieldName];
      }
      if (typeof request[fieldName] !== 'undefined') {
        return request[fieldName];
      }
      return undefined;
    };

    switch (uppercaseType) {
      case REQUEST_TYPES.PING:
      case REQUEST_TYPES.LIST_TOPICS:
        return { valid: true };

      case REQUEST_TYPES.CREATE_TOPIC: {
        const topic = getField('topic');
        if (!topic || typeof topic !== 'string') {
          return {
            valid: false,
            error: 'Request must include a string "topic" in payload'
          };
        }
        const topicValidation = TopicManager.validateTopicName(topic);
        if (!topicValidation.valid) {
          return {
            valid: false,
            error: topicValidation.error
          };
        }

        const partitions = getField('partitions');
        if (partitions !== undefined && partitions !== null) {
          const partitionValidation = TopicManager.validatePartitionCount(partitions);
          if (!partitionValidation.valid) {
            return {
              valid: false,
              error: partitionValidation.error
            };
          }
        }

        return { valid: true };
      }

      case REQUEST_TYPES.GET_TOPIC_INFO:
      case REQUEST_TYPES.DELETE_TOPIC: {
        const topic = getField('topic');
        if (!topic || typeof topic !== 'string') {
          return {
            valid: false,
            error: 'Request must include a string "topic" in payload'
          };
        }
        const topicValidation = TopicManager.validateTopicName(topic);
        if (!topicValidation.valid) {
          return {
            valid: false,
            error: topicValidation.error
          };
        }
        return { valid: true };
      }

      case REQUEST_TYPES.GET_PARTITION_INFO: {
        const topic = getField('topic');
        if (!topic || typeof topic !== 'string') {
          return {
            valid: false,
            error: 'GET_PARTITION_INFO request must include a string "topic" in payload'
          };
        }
        const topicValidation = TopicManager.validateTopicName(topic);
        if (!topicValidation.valid) {
          return {
            valid: false,
            error: topicValidation.error
          };
        }

        const partition = getField('partition');
        if (partition === undefined || partition === null) {
          return {
            valid: false,
            error: 'GET_PARTITION_INFO request must include an integer "partition" in payload'
          };
        }

        if (typeof partition !== 'number' || !Number.isInteger(partition) || partition < 0) {
          return {
            valid: false,
            error: 'Partition must be a non-negative integer'
          };
        }

        return { valid: true };
      }

      case REQUEST_TYPES.PRODUCE: {
        const topic = getField('topic');
        if (!topic || typeof topic !== 'string') {
          return {
            valid: false,
            error: 'PRODUCE request must include a string "topic" in payload'
          };
        }
        const topicValidation = TopicManager.validateTopicName(topic);
        if (!topicValidation.valid) {
          return {
            valid: false,
            error: topicValidation.error
          };
        }

        const msg = getField('message');
        if (typeof msg !== 'string') {
          return {
            valid: false,
            error: 'PRODUCE request must include a string "message"'
          };
        }
        if (msg.length === 0) {
          return {
            valid: false,
            error: 'PRODUCE request "message" cannot be empty'
          };
        }

        const partition = getField('partition');
        if (partition !== undefined && partition !== null) {
          if (typeof partition !== 'number' || !Number.isInteger(partition) || partition < 0) {
            return {
              valid: false,
              error: 'Partition must be a non-negative integer'
            };
          }
        }

        return { valid: true };
      }

      case REQUEST_TYPES.CONSUME: {
        const topic = getField('topic');
        if (!topic || typeof topic !== 'string') {
          return {
            valid: false,
            error: 'CONSUME request must include a string "topic" in payload'
          };
        }
        const topicValidation = TopicManager.validateTopicName(topic);
        if (!topicValidation.valid) {
          return {
            valid: false,
            error: topicValidation.error
          };
        }

        const partition = getField('partition');
        if (partition !== undefined && partition !== null) {
          if (typeof partition !== 'number' || !Number.isInteger(partition) || partition < 0) {
            return {
              valid: false,
              error: 'Partition must be a non-negative integer'
            };
          }
        }

        const offset = getField('offset');
        if (offset !== undefined && offset !== null) {
          if (typeof offset !== 'number' || !Number.isInteger(offset) || offset < 0) {
            return {
              valid: false,
              error: {
                code: 'OFFSET_OUT_OF_RANGE',
                message: `Offset ${offset} is out of range`
              }
            };
          }
        }

        return { valid: true };
      }

      case REQUEST_TYPES.JOIN_GROUP: {
        const groupId = getField('groupId');
        const consumerId = getField('consumerId');
        if (!groupId || typeof groupId !== 'string') {
          return { valid: false, error: 'JOIN_GROUP request must include a string "groupId"' };
        }
        if (!consumerId || typeof consumerId !== 'string') {
          return { valid: false, error: 'JOIN_GROUP request must include a string "consumerId"' };
        }
        return { valid: true };
      }

      case REQUEST_TYPES.LEAVE_GROUP: {
        const groupId = getField('groupId');
        const consumerId = getField('consumerId');
        if (!groupId || typeof groupId !== 'string') {
          return { valid: false, error: 'LEAVE_GROUP request must include a string "groupId"' };
        }
        if (!consumerId || typeof consumerId !== 'string') {
          return { valid: false, error: 'LEAVE_GROUP request must include a string "consumerId"' };
        }
        return { valid: true };
      }

      case REQUEST_TYPES.COMMIT_OFFSET: {
        const groupId = getField('groupId');
        const topic = getField('topic');
        const partition = getField('partition');
        const offset = getField('offset');

        if (!groupId || typeof groupId !== 'string') {
          return { valid: false, error: 'COMMIT_OFFSET request must include a string "groupId"' };
        }
        if (!topic || typeof topic !== 'string') {
          return { valid: false, error: 'COMMIT_OFFSET request must include a string "topic"' };
        }
        if (partition === undefined || typeof partition !== 'number' || !Number.isInteger(partition) || partition < 0) {
          return { valid: false, error: 'COMMIT_OFFSET request must include a non-negative integer "partition"' };
        }
        if (offset === undefined || typeof offset !== 'number' || !Number.isInteger(offset) || offset < 0) {
          return { valid: false, error: { code: 'OFFSET_OUT_OF_RANGE', message: `Offset ${offset} is out of range` } };
        }

        return { valid: true };
      }

      case REQUEST_TYPES.GET_GROUP_INFO: {
        const groupId = getField('groupId');
        if (!groupId || typeof groupId !== 'string') {
          return { valid: false, error: 'GET_GROUP_INFO request must include a string "groupId"' };
        }
        return { valid: true };
      }

      default:
        return {
          valid: false,
          error: `Unhandled request type "${request.type}"`
        };
    }
  }
}
