import { EventEmitter } from 'events';
import { COMMAND_TYPES } from './commands.js';

/**
 * ProtocolParser handles stream framing over raw TCP buffers.
 * Emits 'command' events when a complete protocol message has been assembled.
 */
export class ProtocolParser extends EventEmitter {
  constructor() {
    super();
    this.buffer = Buffer.alloc(0);
    this.state = 'HEADER'; // 'HEADER' | 'PAYLOAD'
    this.pendingPub = null;
  }

  /**
   * Appends incoming socket chunk to buffer and processes complete frames.
   * @param {Buffer} chunk 
   */
  feed(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    this._process();
  }

  _process() {
    let processing = true;

    while (processing) {
      if (this.state === 'HEADER') {
        const delimiterIndex = this.buffer.indexOf('\r\n');
        if (delimiterIndex === -1) {
          // Not enough data for header yet
          processing = false;
          break;
        }

        const line = this.buffer.subarray(0, delimiterIndex).toString('utf-8').trim();
        this.buffer = this.buffer.subarray(delimiterIndex + 2);

        if (!line) continue; // Skip empty lines

        const parts = line.split(' ');
        const commandType = parts[0].toUpperCase();

        switch (commandType) {
          case COMMAND_TYPES.SUB: {
            if (parts.length < 2) {
              this.emit('error', new Error('Malformed SUB command: missing topic'));
              break;
            }
            this.emit('command', { type: COMMAND_TYPES.SUB, topic: parts[1] });
            break;
          }

          case COMMAND_TYPES.UNSUB: {
            if (parts.length < 2) {
              this.emit('error', new Error('Malformed UNSUB command: missing topic'));
              break;
            }
            this.emit('command', { type: COMMAND_TYPES.UNSUB, topic: parts[1] });
            break;
          }

          case COMMAND_TYPES.PUB: {
            if (parts.length < 3) {
              this.emit('error', new Error('Malformed PUB command: syntax is PUB <topic> <bytes>'));
              break;
            }
            const topic = parts[1];
            const bytes = parseInt(parts[2], 10);

            if (isNaN(bytes) || bytes < 0) {
              this.emit('error', new Error('Malformed PUB command: invalid payload byte count'));
              break;
            }

            this.pendingPub = { type: COMMAND_TYPES.PUB, topic, bytes };
            this.state = 'PAYLOAD';
            break;
          }

          case COMMAND_TYPES.MSG: {
            if (parts.length < 4) {
              this.emit('error', new Error('Malformed MSG command: syntax is MSG <topic> <id> <bytes>'));
              break;
            }
            const topic = parts[1];
            const messageId = parts[2];
            const bytes = parseInt(parts[3], 10);

            if (isNaN(bytes) || bytes < 0) {
              this.emit('error', new Error('Malformed MSG command: invalid payload byte count'));
              break;
            }

            this.pendingPub = { type: COMMAND_TYPES.MSG, topic, messageId, bytes };
            this.state = 'PAYLOAD';
            break;
          }

          case '+OK':
          case 'OK': {
            this.emit('command', { type: COMMAND_TYPES.OK });
            break;
          }

          case '-ERR':
          case 'ERR': {
            const errMsg = parts.slice(1).join(' ');
            this.emit('command', { type: COMMAND_TYPES.ERR, message: errMsg });
            break;
          }

          default:
            this.emit('error', new Error(`Unknown command: ${commandType}`));
            break;
        }
      }

      if (this.state === 'PAYLOAD') {
        const requiredLength = this.pendingPub.bytes + 2; // payload + trailing \r\n
        if (this.buffer.length < requiredLength) {
          // Wait for more payload data
          processing = false;
          break;
        }

        const payload = this.buffer.subarray(0, this.pendingPub.bytes);
        // Slice out payload and trailing \r\n
        this.buffer = this.buffer.subarray(requiredLength);

        const command = {
          type: this.pendingPub.type || COMMAND_TYPES.PUB,
          topic: this.pendingPub.topic,
          messageId: this.pendingPub.messageId,
          payload
        };

        this.pendingPub = null;
        this.state = 'HEADER';
        this.emit('command', command);
      }
    }
  }
}
