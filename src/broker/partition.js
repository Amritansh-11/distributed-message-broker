/**
 * Partition — Pure Domain Entity for an Append-Only Offset Log
 * 
 * Manages an individual partition's ID, its monotonically increasing offsets,
 * and an append-only log of messages.
 */
export class Partition {
  /**
   * @param {number} id - Numeric identifier for this partition (0-indexed)
   */
  constructor(id) {
    this.id = id;
    /** @type {Array<{ offset: number, message: any }>} Append-only log */
    this.messages = [];
    /** @type {number} Monotonically increasing offset counter */
    this.nextOffset = 0;
    /** @type {number} Pointer for legacy un-grouped consumption */
    this.legacyReadHead = 0;
  }

  /**
   * Appends a message to this partition log and assigns a unique offset.
   * @param {any} message 
   * @returns {{ offset: number, message: any }}
   */
  enqueue(message) {
    const offset = this.nextOffset++;
    const entry = { offset, message };
    this.messages.push(entry);
    return entry;
  }

  /**
   * Enqueues a message at an explicit leader-assigned offset (for follower replica writes).
   * Validates against offset payload conflicts.
   * 
   * @param {number} offset 
   * @param {any} message 
   * @returns {{ offset?: number, message?: any, error?: { code: string, message: string } }}
   */
  enqueueWithOffset(offset, message) {
    if (offset < this.nextOffset) {
      const existing = this.messages[offset];
      if (existing) {
        const exMsgStr = typeof existing.message === 'string' ? existing.message : JSON.stringify(existing.message);
        const newMsgStr = typeof message === 'string' ? message : JSON.stringify(message);
        if (exMsgStr === newMsgStr) {
          return { offset, message };
        }
        return {
          error: {
            code: 'OFFSET_CONFLICT',
            message: `Conflicting message at offset ${offset} for partition ${this.id}`
          }
        };
      }
    }

    const entry = { offset, message };
    this.messages[offset] = entry;
    this.nextOffset = Math.max(this.nextOffset, offset + 1);
    return entry;
  }

  /**
   * Reads a message at a specific offset without removing it from the log.
   * 
   * @param {number} offset 
   * @returns {{ success: boolean, offset?: number, message?: any, code?: string, message?: string }}
   */
  readOffset(offset) {
    if (typeof offset !== 'number' || !Number.isInteger(offset) || offset < 0 || offset >= this.nextOffset) {
      return {
        success: false,
        code: 'OFFSET_OUT_OF_RANGE',
        message: `Offset ${offset} is out of range for partition ${this.id}`
      };
    }

    const entry = this.messages[offset];
    if (!entry) {
      return {
        success: false,
        code: 'OFFSET_OUT_OF_RANGE',
        message: `Offset ${offset} is out of range for partition ${this.id}`
      };
    }

    return {
      success: true,
      offset: entry.offset,
      message: entry.message
    };
  }

  /**
   * Legacy dequeue helper for un-grouped / offset-less consumption.
   * Advances an internal un-grouped read pointer without removing the message.
   * 
   * @returns {any|null} Returns message payload or null if partition log has no unread legacy messages.
   */
  dequeue() {
    if (this.legacyReadHead >= this.messages.length) {
      return null;
    }

    const entry = this.messages[this.legacyReadHead++];
    return entry.message;
  }

  /**
   * Returns current total message count in this partition log.
   * @returns {number}
   */
  getMessageCount() {
    return this.messages.length;
  }

  /**
   * Returns the next offset to be assigned.
   * @returns {number}
   */
  getNextOffset() {
    return this.nextOffset;
  }

  /**
   * Resets partition log and offset counters.
   */
  clear() {
    this.messages = [];
    this.nextOffset = 0;
    this.legacyReadHead = 0;
  }
}
