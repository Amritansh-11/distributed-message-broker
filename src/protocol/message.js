/**
 * NDJSON (Newline-Delimited JSON) Framing Handler
 * 
 * Decouples TCP stream framing from broker domain logic.
 * TCP delivers a stream of bytes with arbitrary chunk boundaries.
 * This class buffers incoming data, splits on newlines (\n or \r\n),
 * and yields fully parsed application-level JSON objects.
 */
export class NDJSONFramer {
  constructor() {
    this.buffer = '';
  }

  /**
   * Feeds raw TCP byte chunk into the frame buffer and extracts discrete messages.
   * @param {Buffer | string} chunk - Raw data from TCP socket
   * @returns {Array<{ parsed?: any, raw: string, error?: Error }>} Array of extracted frame results
   */
  feed(chunk) {
    this.buffer += chunk.toString('utf-8');
    const results = [];
    let newlineIndex;

    while ((newlineIndex = this.buffer.indexOf('\n')) !== -1) {
      let line = this.buffer.slice(0, newlineIndex);
      this.buffer = this.buffer.slice(newlineIndex + 1);

      // Handle trailing \r if sender uses CRLF (\r\n)
      if (line.endsWith('\r')) {
        line = line.slice(0, -1);
      }

      // Ignore empty lines
      if (line.trim() === '') {
        continue;
      }

      try {
        const parsed = JSON.parse(line);
        results.push({ parsed, raw: line });
      } catch (err) {
        results.push({ error: err, raw: line });
      }
    }

    return results;
  }

  /**
   * Encodes a JavaScript object into a line-delimited JSON string suitable for TCP transmission.
   * @param {object} payload - Message object to serialize
   * @returns {string} JSON payload formatted with a trailing newline
   */
  static encode(payload) {
    return JSON.stringify(payload) + '\n';
  }
}

/**
 * Helper builders for standardized protocol responses
 */
export const ProtocolResponse = {
  pong: () => NDJSONFramer.encode({ type: 'PONG' }),
  produceAck: () => NDJSONFramer.encode({ type: 'PRODUCE_ACK', success: true }),
  message: (msg) => NDJSONFramer.encode({ type: 'MESSAGE', success: true, message: msg }),
  noMessages: () => NDJSONFramer.encode({ type: 'NO_MESSAGES', success: true }),
  error: (errorMessage) => NDJSONFramer.encode({ type: 'ERROR', success: false, error: errorMessage })
};
