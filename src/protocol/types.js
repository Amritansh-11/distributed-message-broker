/**
 * Protocol Types and Message Factory Definitions
 * 
 * Defines standard command types, response types, and object creators
 * for the Distributed Message Broker protocol.
 */

export const REQUEST_TYPES = {
  PING: 'PING',
  PRODUCE: 'PRODUCE',
  CONSUME: 'CONSUME'
};

export const RESPONSE_TYPES = {
  PONG: 'PONG',
  PRODUCE_ACK: 'PRODUCE_ACK',
  MESSAGE: 'MESSAGE',
  NO_MESSAGES: 'NO_MESSAGES',
  ERROR: 'ERROR'
};

export const ProtocolRequest = {
  ping: () => ({ type: REQUEST_TYPES.PING }),
  produce: (message) => ({ type: REQUEST_TYPES.PRODUCE, message }),
  consume: () => ({ type: REQUEST_TYPES.CONSUME })
};

export const ProtocolResponse = {
  pong: () => ({ type: RESPONSE_TYPES.PONG, success: true }),
  produceAck: () => ({ type: RESPONSE_TYPES.PRODUCE_ACK, success: true }),
  message: (msg) => ({ type: RESPONSE_TYPES.MESSAGE, success: true, message: msg }),
  noMessages: () => ({ type: RESPONSE_TYPES.NO_MESSAGES, success: true }),
  error: (errorMessage) => ({ type: RESPONSE_TYPES.ERROR, success: false, error: errorMessage })
};
