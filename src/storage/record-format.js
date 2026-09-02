/**
 * RecordFormat — Persistent Record Serialization & Integrity Layer
 * 
 * Handles length-prefixed binary framing, CRC32 checksum calculation,
 * record parsing, and incomplete/corrupted record detection.
 */

export class RecordFormat {
  /**
   * Computes a deterministic 32-bit CRC checksum for a string payload.
   * @param {string} str 
   * @returns {number} Unsigned 32-bit integer CRC
   */
  static computeCrc32(str) {
    let crc = 0xFFFFFFFF;
    for (let i = 0; i < str.length; i++) {
      const byte = str.charCodeAt(i);
      crc = crc ^ byte;
      for (let j = 0; j < 8; j++) {
        crc = (crc >>> 1) ^ ((crc & 1) ? 0xEDB88320 : 0);
      }
    }
    return (crc ^ 0xFFFFFFFF) >>> 0;
  }

  /**
   * Encodes a message and its offset into a length-prefixed binary record buffer.
   * 
   * Format: [4 Bytes Big-Endian Length] [UTF-8 JSON String] [\n Delimiter]
   * 
   * @param {number} offset 
   * @param {any} message 
   * @returns {Buffer}
   */
  static encode(offset, message) {
    const msgStr = typeof message === 'string' ? message : JSON.stringify(message);
    const crc = RecordFormat.computeCrc32(msgStr);
    
    const obj = { offset, message, crc };
    const payloadStr = JSON.stringify(obj);
    const payloadBuf = Buffer.from(payloadStr, 'utf8');

    const lenBuf = Buffer.alloc(4);
    lenBuf.writeUInt32BE(payloadBuf.length, 0);

    const delimBuf = Buffer.from('\n', 'utf8');

    return Buffer.concat([lenBuf, payloadBuf, delimBuf]);
  }

  /**
   * Decodes a record from a buffer starting at a specific byte offset.
   * 
   * @param {Buffer} buffer 
   * @param {number} [startOffset=0] 
   * @returns {{ status: 'VALID' | 'INCOMPLETE' | 'CORRUPTED', record?: { offset: number, message: any }, bytesRead: number, error?: string }}
   */
  static decodeRecord(buffer, startOffset = 0) {
    const remaining = buffer.length - startOffset;

    // Need at least 4 bytes to read record length
    if (remaining < 4) {
      return { status: 'INCOMPLETE', bytesRead: 0 };
    }

    const payloadLength = buffer.readUInt32BE(startOffset);
    const totalRecordLength = 4 + payloadLength + 1; // 4 len + payload + 1 newline

    // Check if entire record payload + newline is available in buffer
    if (remaining < totalRecordLength) {
      return { status: 'INCOMPLETE', bytesRead: 0 };
    }

    // Verify trailing newline delimiter (0x0A)
    const delimiter = buffer[startOffset + 4 + payloadLength];
    if (delimiter !== 0x0A) {
      return { status: 'CORRUPTED', bytesRead: 0, error: 'Record missing valid newline delimiter' };
    }

    // Extract payload JSON string
    const payloadBuf = buffer.subarray(startOffset + 4, startOffset + 4 + payloadLength);
    let parsed;
    try {
      parsed = JSON.parse(payloadBuf.toString('utf8'));
    } catch (err) {
      return { status: 'CORRUPTED', bytesRead: 0, error: `Malformed JSON record payload: ${err.message}` };
    }

    if (parsed === null || typeof parsed !== 'object' || typeof parsed.offset !== 'number') {
      return { status: 'CORRUPTED', bytesRead: 0, error: 'Invalid record schema in payload' };
    }

    // Verify checksum integrity
    const msgStr = typeof parsed.message === 'string' ? parsed.message : JSON.stringify(parsed.message);
    const computedCrc = RecordFormat.computeCrc32(msgStr);
    if (parsed.crc !== undefined && parsed.crc !== computedCrc) {
      return { status: 'CORRUPTED', bytesRead: 0, error: `CRC mismatch: expected ${parsed.crc}, computed ${computedCrc}` };
    }

    return {
      status: 'VALID',
      record: {
        offset: parsed.offset,
        message: parsed.message
      },
      bytesRead: totalRecordLength
    };
  }

  /**
   * Reads all records from a segment buffer, handling partial records at EOF or corruption.
   * 
   * @param {Buffer} buffer 
   * @returns {{ records: Array<{ offset: number, message: any }>, validByteLength: number, hasIncompleteTail: boolean, corrupted: boolean, error?: string }}
   */
  static readAllRecords(buffer) {
    const records = [];
    let cursor = 0;
    let hasIncompleteTail = false;

    while (cursor < buffer.length) {
      const result = RecordFormat.decodeRecord(buffer, cursor);

      if (result.status === 'VALID') {
        records.push(result.record);
        cursor += result.bytesRead;
      } else if (result.status === 'INCOMPLETE') {
        hasIncompleteTail = true;
        break; // Stop reading at incomplete tail
      } else {
        // CORRUPTED record mid-stream
        return {
          records,
          validByteLength: cursor,
          hasIncompleteTail: true,
          corrupted: true,
          error: result.error
        };
      }
    }

    return {
      records,
      validByteLength: cursor,
      hasIncompleteTail,
      corrupted: false
    };
  }
}
