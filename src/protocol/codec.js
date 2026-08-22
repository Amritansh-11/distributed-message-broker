/**
 * Protocol Codec — Encoder and Decoder for Wire Protocol Messages
 * 
 * Handles serialization and deserialization between raw frame strings/buffers
 * and structured JavaScript request/response objects.
 */

import { StreamFramer } from './framing.js';

export class ProtocolDecoder {
  /**
   * Decodes a raw frame string into a structured protocol object.
   * 
   * @param {string} rawFrame - Raw un-delimited frame string
   * @returns {{ parsed?: object, error?: Error, raw: string }} Decoded object or error details
   */
  static decode(rawFrame) {
    if (typeof rawFrame !== 'string') {
      return {
        error: new TypeError('Decoder expects rawFrame to be a string'),
        raw: String(rawFrame)
      };
    }

    try {
      const parsed = JSON.parse(rawFrame);
      if (!parsed || typeof parsed !== 'object') {
        return {
          error: new Error('Decoded frame payload must be a JSON object'),
          raw: rawFrame
        };
      }
      return { parsed, raw: rawFrame };
    } catch (err) {
      return {
        error: new Error(`Malformed JSON wire payload: ${err.message}`),
        raw: rawFrame
      };
    }
  }
}

export class ProtocolEncoder {
  /**
   * Encodes a protocol payload object into a wire-ready framed string.
   * 
   * @param {object} payload - JavaScript object (Request or Response)
   * @returns {string} Framed string with trailing delimiter
   */
  static encode(payload) {
    if (payload === undefined || payload === null) {
      throw new TypeError('Cannot encode undefined or null payload');
    }

    const jsonStr = JSON.stringify(payload);
    return StreamFramer.format(jsonStr);
  }
}
