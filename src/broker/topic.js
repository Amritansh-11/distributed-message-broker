/**
 * Topic — Pure Domain Entity representing a Topic and its Partitions
 * 
 * Manages partition creation, partition routing (explicit, key-based, round-robin),
 * offset-based message logs per partition, and partition-level metadata.
 */

import { Partition } from './partition.js';

export class Topic {
  /**
   * @param {string} name - Unique topic name
   * @param {number} [partitionCount=3] - Number of partitions (default: 3)
   */
  constructor(name, partitionCount = 3) {
    this.name = name;
    /** @type {Map<number, Partition>} */
    this.partitions = new Map();
    this.nextProduceRoundRobin = 0;
    this.nextConsumeRoundRobin = 0;

    const count = typeof partitionCount === 'number' ? partitionCount : 3;
    for (let i = 0; i < count; i++) {
      this.partitions.set(i, new Partition(i));
    }
  }

  /**
   * Returns total number of partitions in this topic.
   * @returns {number}
   */
  get partitionCount() {
    return this.partitions.size;
  }

  /**
   * Checks if partition ID exists within topic.
   * @param {number} partitionId 
   * @returns {boolean}
   */
  hasPartition(partitionId) {
    return typeof partitionId === 'number' && Number.isInteger(partitionId) && this.partitions.has(partitionId);
  }

  /**
   * Deterministic hash function for key-based routing.
   * Uses djb2 hash algorithm modulo partitionCount.
   * 
   * @param {string|number} key 
   * @param {number} partitionCount 
   * @returns {number} Valid partition index [0, partitionCount - 1]
   */
  static hashKey(key, partitionCount) {
    const str = String(key);
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      hash = (hash * 31 + str.charCodeAt(i)) >>> 0;
    }
    return hash % partitionCount;
  }

  /**
   * Selects target partition ID following selection priority:
   * 1. Explicit partition (if provided)
   * 2. Key-based partition hash (if key provided)
   * 3. Round-robin partition assignment
   * 
   * @param {number} [explicitPartition] 
   * @param {string|number} [key] 
   * @returns {{ partitionId?: number, error?: { code: string, message: string } }}
   */
  selectPartitionForProduce(explicitPartition, key) {
    if (explicitPartition !== undefined && explicitPartition !== null) {
      if (!this.hasPartition(explicitPartition)) {
        return {
          error: {
            code: 'PARTITION_NOT_FOUND',
            message: `Partition ${explicitPartition} does not exist for topic '${this.name}'`
          }
        };
      }
      return { partitionId: explicitPartition };
    }

    if (key !== undefined && key !== null && String(key).length > 0) {
      const pId = Topic.hashKey(key, this.partitionCount);
      return { partitionId: pId };
    }

    // Round-robin assignment
    const pId = this.nextProduceRoundRobin % this.partitionCount;
    this.nextProduceRoundRobin = (this.nextProduceRoundRobin + 1) % this.partitionCount;
    return { partitionId: pId };
  }

  /**
   * Enqueues message into designated partition and assigns offset.
   * 
   * @param {any} message 
   * @param {number} [explicitPartition] 
   * @param {string|number} [key] 
   * @returns {{ success: boolean, partitionId?: number, offset?: number, code?: string, message?: string }}
   */
  enqueue(message, explicitPartition, key) {
    const selection = this.selectPartitionForProduce(explicitPartition, key);
    if (selection.error) {
      return {
        success: false,
        code: selection.error.code,
        message: selection.error.message
      };
    }

    const partition = this.partitions.get(selection.partitionId);
    const entry = partition.enqueue(message);
    return {
      success: true,
      partitionId: selection.partitionId,
      offset: entry.offset
    };
  }

  /**
   * Reads a message at an explicit partition and offset.
   * 
   * @param {number} partitionId 
   * @param {number} offset 
   * @returns {{ success: boolean, partitionId?: number, offset?: number, message?: any, code?: string, message?: string }}
   */
  readOffset(partitionId, offset) {
    if (!this.hasPartition(partitionId)) {
      return {
        success: false,
        code: 'PARTITION_NOT_FOUND',
        message: `Partition ${partitionId} does not exist for topic '${this.name}'`
      };
    }

    const partition = this.partitions.get(partitionId);
    const result = partition.readOffset(offset);
    if (!result.success) {
      return {
        success: false,
        code: result.code,
        message: result.message
      };
    }

    return {
      success: true,
      partitionId,
      offset: result.offset,
      message: result.message
    };
  }

  /**
   * Dequeues message from explicit partition or via round-robin (legacy un-grouped consumption).
   * 
   * @param {number} [explicitPartition] 
   * @returns {{ success: boolean, message?: any, partitionId?: number, offset?: number, code?: string, message?: string }}
   */
  dequeue(explicitPartition) {
    if (explicitPartition !== undefined && explicitPartition !== null) {
      if (!this.hasPartition(explicitPartition)) {
        return {
          success: false,
          code: 'PARTITION_NOT_FOUND',
          message: `Partition ${explicitPartition} does not exist for topic '${this.name}'`
        };
      }
      const partition = this.partitions.get(explicitPartition);
      const headOffset = partition.legacyReadHead;
      const msg = partition.dequeue();
      return {
        success: true,
        message: msg,
        offset: msg !== null ? headOffset : undefined,
        partitionId: explicitPartition
      };
    }

    // Round-robin search across partitions for un-read legacy message
    const count = this.partitionCount;
    for (let i = 0; i < count; i++) {
      const pId = (this.nextConsumeRoundRobin + i) % count;
      const partition = this.partitions.get(pId);
      if (partition.legacyReadHead < partition.getMessageCount()) {
        const headOffset = partition.legacyReadHead;
        const msg = partition.dequeue();
        this.nextConsumeRoundRobin = (pId + 1) % count;
        return {
          success: true,
          message: msg,
          offset: headOffset,
          partitionId: pId
        };
      }
    }

    // All partitions empty for legacy consumption
    const defaultPId = this.nextConsumeRoundRobin % count;
    this.nextConsumeRoundRobin = (this.nextConsumeRoundRobin + 1) % count;
    return {
      success: true,
      message: null,
      partitionId: defaultPId
    };
  }

  /**
   * Calculates total message count across all partitions in this topic.
   * @returns {number}
   */
  getTotalMessageCount() {
    let sum = 0;
    for (const partition of this.partitions.values()) {
      sum += partition.getMessageCount();
    }
    return sum;
  }

  /**
   * Returns comprehensive topic and partition metadata.
   */
  getInfo() {
    const partitionInfo = [];
    for (const [id, partition] of this.partitions.entries()) {
      partitionInfo.push({
        partition: id,
        messageCount: partition.getMessageCount(),
        nextOffset: partition.getNextOffset()
      });
    }
    return {
      topic: this.name,
      partitions: this.partitionCount,
      messageCount: this.getTotalMessageCount(),
      partitionInfo
    };
  }

  /**
   * Returns metadata for a single specific partition.
   * @param {number} partitionId 
   */
  getPartitionInfo(partitionId) {
    if (!this.hasPartition(partitionId)) {
      return {
        success: false,
        code: 'PARTITION_NOT_FOUND',
        message: `Partition ${partitionId} does not exist for topic '${this.name}'`
      };
    }

    const partition = this.partitions.get(partitionId);
    return {
      success: true,
      topic: this.name,
      partition: partitionId,
      messageCount: partition.getMessageCount(),
      nextOffset: partition.getNextOffset()
    };
  }

  /**
   * Clears all messages across all partitions.
   */
  clear() {
    for (const partition of this.partitions.values()) {
      partition.clear();
    }
  }
}
