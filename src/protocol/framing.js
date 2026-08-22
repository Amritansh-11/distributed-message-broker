/**
 * StreamFramer — Stream-oriented TCP Message Framing Handler
 * 
 * Handles stream framing and buffer management at OSI Layer 7.
 * Delineates discrete application message frames from TCP stream chunks,
 * managing partial packets, multi-message chunks, and frame size safety bounds.
 */

export const DEFAULT_MAX_FRAME_SIZE = 1024 * 1024; // 1 MB limit per frame

export class StreamFramer {
  /**
   * @param {object} [options]
   * @param {number} [options.maxFrameSize=1048576] - Maximum allowed byte length for a single frame
   */
  constructor(options = {}) {
    this.maxFrameSize = options.maxFrameSize || DEFAULT_MAX_FRAME_SIZE;
    this.buffer = '';
  }

  /**
   * Feeds a raw TCP chunk into the stream buffer and extracts complete wire frames.
   * 
   * @param {Buffer | string} chunk - Raw data chunk from net.Socket
   * @returns {Array<{ raw: string, error?: Error }>} Array of extracted raw frame objects
   */
  feed(chunk) {
    const chunkStr = typeof chunk === 'string' ? chunk : chunk.toString('utf-8');
    this.buffer += chunkStr;

    const results = [];
    let newlineIndex;

    while ((newlineIndex = this.buffer.indexOf('\n')) !== -1) {
      let rawLine = this.buffer.slice(0, newlineIndex);
      this.buffer = this.buffer.slice(newlineIndex + 1);

      // Handle CRLF (\r\n) trailing carriage return
      if (rawLine.endsWith('\r')) {
        rawLine = rawLine.slice(0, -1);
      }

      // Ignore empty lines
      if (rawLine.trim() === '') {
        continue;
      }

      // Check max frame size safety bounds
      if (Buffer.byteLength(rawLine, 'utf-8') > this.maxFrameSize) {
        results.push({
          error: new Error(`Frame size exceeds maximum allowed limit of ${this.maxFrameSize} bytes`),
          raw: rawLine.slice(0, 100) + '...'
        });
        continue;
      }

      results.push({ raw: rawLine });
    }

    // Guard against un-delimited buffer overflow attack
    if (Buffer.byteLength(this.buffer, 'utf-8') > this.maxFrameSize) {
      const overflowRaw = this.buffer.slice(0, 100) + '...';
      this.buffer = ''; // Reset buffer after overflow violation
      results.push({
        error: new Error(`Frame buffer overflow: un-delimited frame exceeds limit of ${this.maxFrameSize} bytes`),
        raw: overflowRaw
      });
    }

    return results;
  }

  /**
   * Formats a raw payload string into a properly framed wire payload (with trailing newline).
   * 
   * @param {string} payload 
   * @returns {string} Framed wire payload
   */
  static format(payload) {
    return payload.endsWith('\n') ? payload : payload + '\n';
  }

  /**
   * Resets the internal stream buffer.
   */
  reset() {
    this.buffer = '';
  }
}
