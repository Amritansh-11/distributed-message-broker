/**
 * RequestValidator — Protocol Request Schema & Validation Layer
 * 
 * Enforces strict request schema constraints before requests are passed to
 * the core Message Broker domain engine.
 */

import { REQUEST_TYPES } from './types.js';

export class RequestValidator {
  /**
   * Validates a decoded protocol request object.
   * 
   * @param {any} request - Decoded request object from ProtocolDecoder
   * @returns {{ valid: boolean, error?: string }} Validation outcome
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

    switch (uppercaseType) {
      case REQUEST_TYPES.PING:
        return { valid: true };

      case REQUEST_TYPES.PRODUCE:
        if (typeof request.message !== 'string') {
          return {
            valid: false,
            error: 'PRODUCE request must include a string "message"'
          };
        }
        if (request.message.length === 0) {
          return {
            valid: false,
            error: 'PRODUCE request "message" cannot be empty'
          };
        }
        return { valid: true };

      case REQUEST_TYPES.CONSUME:
        return { valid: true };

      default:
        return {
          valid: false,
          error: `Unhandled request type "${request.type}"`
        };
    }
  }
}
