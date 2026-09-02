/**
 * LogSegment — Represents an Individual Append-Only Segment File
 * 
 * Manages segment file paths, appending encoded records, rollover limits,
 * reading records during recovery, and truncating partial tail records.
 */

import fs from 'fs';
import path from 'path';
import { RecordFormat } from './record-format.js';

export class LogSegment {
  /**
   * @param {string} dirPath - Partition directory path
   * @param {number} baseOffset - Starting offset for this segment
   * @param {number} [maxMessages=1000] - Max messages per segment before rollover
   */
  constructor(dirPath, baseOffset, maxMessages = 1000) {
    this.dirPath = dirPath;
    this.baseOffset = baseOffset;
    this.maxMessages = maxMessages;
    
    // Padded 12-digit filename e.g. 000000000000.log
    const fileName = `${String(baseOffset).padStart(12, '0')}.log`;
    this.filePath = path.join(dirPath, fileName);
    this.messageCount = 0;
  }

  /**
   * Formats a base offset integer into a 12-digit padded segment filename.
   * @param {number} offset 
   * @returns {string} e.g. "000000000000.log"
   */
  static formatSegmentFileName(offset) {
    return `${String(offset).padStart(12, '0')}.log`;
  }

  /**
   * Parses the base offset integer from a segment filename.
   * @param {string} fileName e.g. "000000000000.log"
   * @returns {number|null}
   */
  static parseBaseOffset(fileName) {
    if (!fileName.endsWith('.log')) return null;
    const baseStr = fileName.slice(0, -4);
    const parsed = parseInt(baseStr, 10);
    return isNaN(parsed) ? null : parsed;
  }

  /**
   * Appends a message record to this segment file.
   * 
   * @param {number} offset 
   * @param {any} message 
   * @returns {Promise<{ offset: number, message: any, bytesWritten: number }>}
   */
  async append(offset, message) {
    const recordBuffer = RecordFormat.encode(offset, message);
    await fs.promises.appendFile(this.filePath, recordBuffer);
    this.messageCount++;
    return { offset, message, bytesWritten: recordBuffer.length };
  }

  /**
   * Synchronously appends a message record to this segment file.
   * 
   * @param {number} offset 
   * @param {any} message 
   * @returns {{ offset: number, message: any, bytesWritten: number }}
   */
  appendSync(offset, message) {
    const recordBuffer = RecordFormat.encode(offset, message);
    fs.appendFileSync(this.filePath, recordBuffer);
    this.messageCount++;
    return { offset, message, bytesWritten: recordBuffer.length };
  }

  /**
   * Checks if this segment has reached its message capacity limit.
   * @returns {boolean}
   */
  isFull() {
    return this.messageCount >= this.maxMessages;
  }

  /**
   * Reads all records from this segment file, handling incomplete tail records.
   * Automatically truncates partial tail records if detected.
   * 
   * @returns {Promise<{ records: Array<{ offset: number, message: any }>, corrupted: boolean, error?: string }>}
   */
  async readRecords() {
    if (!fs.existsSync(this.filePath)) {
      this.messageCount = 0;
      return { records: [], corrupted: false };
    }

    const buffer = await fs.promises.readFile(this.filePath);
    const result = RecordFormat.readAllRecords(buffer);

    // If incomplete record at tail or corruption at end, truncate file at valid byte length
    if (result.hasIncompleteTail && result.validByteLength < buffer.length) {
      console.warn(`[LogSegment] Truncating incomplete tail record in ${this.filePath} at byte ${result.validByteLength} (original size: ${buffer.length})`);
      await fs.promises.truncate(this.filePath, result.validByteLength);
    }

    this.messageCount = result.records.length;

    return {
      records: result.records,
      corrupted: result.corrupted,
      error: result.error
    };
  }

  /**
   * Synchronously reads all records from this segment file.
   * 
   * @returns {{ records: Array<{ offset: number, message: any }>, corrupted: boolean, error?: string }}
   */
  readRecordsSync() {
    if (!fs.existsSync(this.filePath)) {
      this.messageCount = 0;
      return { records: [], corrupted: false };
    }

    const buffer = fs.readFileSync(this.filePath);
    const result = RecordFormat.readAllRecords(buffer);

    if (result.hasIncompleteTail && result.validByteLength < buffer.length) {
      console.warn(`[LogSegment] Truncating incomplete tail record in ${this.filePath} at byte ${result.validByteLength} (original size: ${buffer.length})`);
      fs.truncateSync(this.filePath, result.validByteLength);
    }

    this.messageCount = result.records.length;

    return {
      records: result.records,
      corrupted: result.corrupted,
      error: result.error
    };
  }
}
