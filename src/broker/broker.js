/**
 * Core MessageBroker Domain Engine
 * 
 * Pure domain logic for in-memory message queue storage and retrieval.
 * Completely decoupled from TCP transport, socket handling, framing, and codecs.
 */

import { REQUEST_TYPES, ProtocolResponse } from '../protocol/types.js';

export class MessageBroker {
  constructor() {
    /** @type {string[]} In-memory FIFO message queue */
    this.messages = [];
  }

  /**
   * Processes a validated protocol request object and returns a response object.
   * 
   * @param {{ type: string, message?: string }} request - Validated request object
   * @param {string} [clientAddr='local'] - Client identifier for diagnostic logging
   * @returns {object} Response object (PONG, PRODUCE_ACK, MESSAGE, NO_MESSAGES, or ERROR)
   */
  handleRequest(request, clientAddr = 'local') {
    const uppercaseType = request.type.toUpperCase();

    switch (uppercaseType) {
      case REQUEST_TYPES.PING:
        return ProtocolResponse.pong();

      case REQUEST_TYPES.PRODUCE: {
        this.messages.push(request.message);
        console.log(`[Broker] Produced message from ${clientAddr}: "${request.message}" (Queue size: ${this.messages.length})`);
        return ProtocolResponse.produceAck();
      }

      case REQUEST_TYPES.CONSUME: {
        if (this.messages.length > 0) {
          const msg = this.messages.shift();
          console.log(`[Broker] Consumed message for ${clientAddr}: "${msg}" (Remaining: ${this.messages.length})`);
          return ProtocolResponse.message(msg);
        } else {
          console.log(`[Broker] CONSUME request from ${clientAddr} (Queue empty)`);
          return ProtocolResponse.noMessages();
        }
      }

      default:
        console.warn(`[Broker] Unhandled request type "${request.type}" from ${clientAddr}`);
        return ProtocolResponse.error(`Unhandled request type "${request.type}"`);
    }
  }

  /**
   * Returns current count of queued messages.
   * @returns {number}
   */
  getQueueSize() {
    return this.messages.length;
  }

  /**
   * Resets in-memory queue.
   */
  clear() {
    this.messages = [];
  }
}

// Export BrokerServer from server.js for backward compatibility
export { BrokerServer } from './server.js';
