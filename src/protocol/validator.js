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
      case REQUEST_TYPES.GET_CLUSTER_INFO:
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

        const repFactor = getField('replicationFactor');
        if (repFactor !== undefined && repFactor !== null) {
          if (typeof repFactor !== 'number' || !Number.isInteger(repFactor) || repFactor < 1) {
            return {
              valid: false,
              error: {
                code: 'INVALID_REPLICATION_FACTOR',
                message: 'Replication factor must be an integer >= 1'
              }
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

      case REQUEST_TYPES.BROKER_HELLO: {
        const brokerId = getField('brokerId');
        if (!brokerId || typeof brokerId !== 'string') {
          return { valid: false, error: 'BROKER_HELLO request must include a string "brokerId"' };
        }
        return { valid: true };
      }

      case REQUEST_TYPES.BROKER_PING: {
        const brokerId = getField('brokerId');
        if (!brokerId || typeof brokerId !== 'string') {
          return { valid: false, error: 'BROKER_PING request must include a string "brokerId"' };
        }
        return { valid: true };
      }

      case REQUEST_TYPES.REPLICATE_RECORD: {
        const topic = getField('topic');
        const partition = getField('partition');
        const offset = getField('offset');
        const message = getField('message');

        if (!topic || typeof topic !== 'string') return { valid: false, error: 'REPLICATE_RECORD must include "topic"' };
        if (partition === undefined || typeof partition !== 'number' || partition < 0) return { valid: false, error: 'REPLICATE_RECORD must include valid "partition"' };
        if (offset === undefined || typeof offset !== 'number' || offset < 0) return { valid: false, error: 'REPLICATE_RECORD must include valid "offset"' };
        if (typeof message !== 'string') return { valid: false, error: 'REPLICATE_RECORD must include string "message"' };
        return { valid: true };
      }

      case REQUEST_TYPES.REPLICATE_ACK: {
        const brokerId = getField('brokerId');
        const topic = getField('topic');
        const partition = getField('partition');
        const offset = getField('offset');

        if (!brokerId || typeof brokerId !== 'string') return { valid: false, error: 'REPLICATE_ACK must include "brokerId"' };
        if (!topic || typeof topic !== 'string') return { valid: false, error: 'REPLICATE_ACK must include "topic"' };
        if (partition === undefined || typeof partition !== 'number') return { valid: false, error: 'REPLICATE_ACK must include "partition"' };
        if (offset === undefined || typeof offset !== 'number') return { valid: false, error: 'REPLICATE_ACK must include "offset"' };
        return { valid: true };
      }

      case REQUEST_TYPES.REPLICA_SYNC: {
        const brokerId = getField('brokerId');
        const topic = getField('topic');
        const partition = getField('partition');
        const fromOffset = getField('fromOffset');

        if (!brokerId || typeof brokerId !== 'string') return { valid: false, error: 'REPLICA_SYNC must include "brokerId"' };
        if (!topic || typeof topic !== 'string') return { valid: false, error: 'REPLICA_SYNC must include "topic"' };
        if (partition === undefined || typeof partition !== 'number') return { valid: false, error: 'REPLICA_SYNC must include "partition"' };
        if (fromOffset === undefined || typeof fromOffset !== 'number' || fromOffset < 0) return { valid: false, error: 'REPLICA_SYNC must include non-negative "fromOffset"' };
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
