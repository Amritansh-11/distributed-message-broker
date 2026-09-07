/**
 * Structured Logger — Centralized Logging System
 * 
 * Provides structured JSON logging with severity levels (DEBUG, INFO, WARN, ERROR)
 * and rich metadata fields (brokerId, topic, partition, offset, leaderEpoch, remoteBroker, error).
 */

const LOG_LEVELS = {
  DEBUG: 0,
  INFO: 1,
  WARN: 2,
  ERROR: 3
};

export class Logger {
  /**
   * @param {object} [options={}]
   * @param {string} [options.level='INFO'] - Log level threshold
   * @param {string} [options.brokerId='broker-1'] - Default local broker ID
   * @param {boolean} [options.json=true] - Format output as JSON strings
   */
  constructor(options = {}) {
    this.levelStr = (options.level || process.env.LOG_LEVEL || 'INFO').toUpperCase();
    this.level = LOG_LEVELS[this.levelStr] !== undefined ? LOG_LEVELS[this.levelStr] : LOG_LEVELS.INFO;
    this.brokerId = options.brokerId || process.env.BROKER_ID || 'broker-1';
    this.json = options.json !== undefined ? options.json : true;
    this.logs = []; // In-memory buffer for testing inspection
    this.maxMemoryLogs = 500;
  }

  setBrokerId(brokerId) {
    this.brokerId = brokerId;
  }

  setLevel(levelStr) {
    const uppercase = String(levelStr).toUpperCase();
    if (LOG_LEVELS[uppercase] !== undefined) {
      this.levelStr = uppercase;
      this.level = LOG_LEVELS[uppercase];
    }
  }

  _format(level, eventOrMessage, meta = {}) {
    const timestamp = new Date().toISOString();
    const isObjectMeta = typeof meta === 'object' && meta !== null;

    const logEntry = {
      timestamp,
      level,
      brokerId: meta.brokerId || this.brokerId
    };

    if (typeof eventOrMessage === 'string') {
      logEntry.event = eventOrMessage;
    }

    if (isObjectMeta) {
      if (meta.message) logEntry.message = meta.message;
      if (meta.topic !== undefined) logEntry.topic = meta.topic;
      if (meta.partition !== undefined) logEntry.partition = meta.partition;
      if (meta.offset !== undefined) logEntry.offset = meta.offset;
      if (meta.leaderEpoch !== undefined) logEntry.leaderEpoch = meta.leaderEpoch;
      if (meta.remoteBroker !== undefined) logEntry.remoteBroker = meta.remoteBroker;
      if (meta.error !== undefined) {
        logEntry.error = meta.error instanceof Error ? meta.error.message : String(meta.error);
      }
      // Copy any remaining extra fields
      for (const [k, v] of Object.entries(meta)) {
        if (!logEntry.hasOwnProperty(k) && k !== 'brokerId') {
          logEntry[k] = v;
        }
      }
    }

    return logEntry;
  }

  _output(logEntry) {
    // Keep in memory buffer
    this.logs.push(logEntry);
    if (this.logs.length > this.maxMemoryLogs) {
      this.logs.shift();
    }

    const outputStr = this.json
      ? JSON.stringify(logEntry)
      : `[${logEntry.timestamp}] [${logEntry.level}] [${logEntry.brokerId}] ${logEntry.event || ''} ${logEntry.message || ''}`;

    if (logEntry.level === 'ERROR') {
      console.error(outputStr);
    } else if (logEntry.level === 'WARN') {
      console.warn(outputStr);
    } else {
      console.log(outputStr);
    }
  }

  debug(eventOrMessage, meta = {}) {
    if (this.level <= LOG_LEVELS.DEBUG) {
      this._output(this._format('DEBUG', eventOrMessage, meta));
    }
  }

  info(eventOrMessage, meta = {}) {
    if (this.level <= LOG_LEVELS.INFO) {
      this._output(this._format('INFO', eventOrMessage, meta));
    }
  }

  warn(eventOrMessage, meta = {}) {
    if (this.level <= LOG_LEVELS.WARN) {
      this._output(this._format('WARN', eventOrMessage, meta));
    }
  }

  error(eventOrMessage, meta = {}) {
    if (this.level <= LOG_LEVELS.ERROR) {
      this._output(this._format('ERROR', eventOrMessage, meta));
    }
  }

  clear() {
    this.logs = [];
  }
}

// Export singleton instance
export const logger = new Logger();
