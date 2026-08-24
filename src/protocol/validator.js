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

    // Helper to extract topic string from request payload or top-level
    const getTopic = () => {
      if (request.payload && typeof request.payload.topic === 'string') {
        return request.payload.topic;
      }
      if (typeof request.topic === 'string') {
        return request.topic;
      }
      return null;
    };

    // Helper to extract produce message from payload or top-level
    const getMessage = () => {
      if (request.payload && typeof request.payload.message !== 'undefined') {
        return request.payload.message;
      }
      if (typeof request.message !== 'undefined') {
        return request.message;
      }
      return undefined;
    };

    switch (uppercaseType) {
      case REQUEST_TYPES.PING:
      case REQUEST_TYPES.LIST_TOPICS:
        return { valid: true };

      case REQUEST_TYPES.CREATE_TOPIC:
      case REQUEST_TYPES.GET_TOPIC_INFO:
      case REQUEST_TYPES.DELETE_TOPIC: {
        const topic = getTopic();
        if (!topic) {
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

      case REQUEST_TYPES.PRODUCE: {
        const topic = getTopic();
        if (!topic) {
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

        const msg = getMessage();
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
        return { valid: true };
      }

      case REQUEST_TYPES.CONSUME: {
        const topic = getTopic();
        if (!topic) {
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
