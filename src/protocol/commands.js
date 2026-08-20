/**
 * Protocol Command Definitions and Formatters
 * 
 * Wire Protocol Grammar:
 * 
 * 1. Publish Message:
 *    PUB <topic> <payload_size_in_bytes>\r\n
 *    <payload_bytes>\r\n
 * 
 * 2. Subscribe to Topic:
 *    SUB <topic>\r\n
 * 
 * 3. Unsubscribe from Topic:
 *    UNSUB <topic>\r\n
 * 
 * 4. Message Delivered to Consumer:
 *    MSG <topic> <message_id> <payload_size_in_bytes>\r\n
 *    <payload_bytes>\r\n
 * 
 * 5. Responses:
 *    +OK\r\n
 *    -ERR <error_message>\r\n
 */

export const COMMAND_TYPES = {
  PUB: 'PUB',
  SUB: 'SUB',
  UNSUB: 'UNSUB',
  MSG: 'MSG',
  OK: 'OK',
  ERR: 'ERR'
};

export class CommandFormatter {
  static ok() {
    return Buffer.from('+OK\r\n');
  }

  static error(message) {
    return Buffer.from(`-ERR ${message}\r\n`);
  }

  static message(topic, messageId, payload) {
    const payloadBuf = Buffer.isBuffer(payload) ? payload : Buffer.from(payload, 'utf-8');
    const header = `MSG ${topic} ${messageId} ${payloadBuf.length}\r\n`;
    return Buffer.concat([
      Buffer.from(header, 'utf-8'),
      payloadBuf,
      Buffer.from('\r\n', 'utf-8')
    ]);
  }
}
