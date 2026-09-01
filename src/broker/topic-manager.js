/**
 * TopicManager — Pure Domain Entity for Broker Topic & Partition Lifecycle
 * 
 * Responsible for creating, listing, deleting, validating, and managing
 * topics, partitions, and offset-based message logs.
 */

import { Topic } from './topic.js';

export class TopicManager {
  constructor() {
    /** @type {Map<string, Topic>} */
    this.topics = new Map();
  }

  /**
   * Validates topic name against defined rules:
   * - Must be a string
   * - Must not be empty
   * - Max length: 100 characters
   * - Allowed characters: letters (a-z, A-Z), numbers (0-9), hyphen (-), underscore (_), dot (.)
   * 
   * @param {string} name 
   * @returns {{ valid: boolean, error?: string }}
   */
  static validateTopicName(name) {
    if (typeof name !== 'string') {
      return { valid: false, error: 'Topic name must be a string' };
    }
    if (name.trim().length === 0) {
      return { valid: false, error: 'Topic name cannot be empty' };
    }
    if (name.length > 100) {
      return { valid: false, error: 'Topic name cannot exceed 100 characters' };
    }
    const topicNameRegex = /^[a-zA-Z0-9._-]+$/;
    if (!topicNameRegex.test(name)) {
      return { valid: false, error: 'Topic name contains invalid characters. Allowed: letters, numbers, hyphen, underscore, dot' };
    }
    return { valid: true };
  }

  /**
   * Validates partition count against rules:
   * - Must be an integer
   * - Range: 1 <= partitions <= 100
   * 
   * @param {any} count 
   * @returns {{ valid: boolean, error?: string }}
   */
  static validatePartitionCount(count) {
    if (typeof count !== 'number' || !Number.isInteger(count)) {
      return { valid: false, error: 'Partition count must be an integer between 1 and 100' };
    }
    if (count < 1 || count > 100) {
      return { valid: false, error: 'Partition count must be between 1 and 100' };
    }
    return { valid: true };
  }

  /**
   * Creates a new topic with the specified partition count.
   * 
   * @param {string} name 
   * @param {number} [partitionCount=3] 
   * @returns {{ success: boolean, topic?: string, partitions?: number, code?: string, message?: string }}
   */
  createTopic(name, partitionCount = 3) {
    const nameValidation = TopicManager.validateTopicName(name);
    if (!nameValidation.valid) {
      return {
        success: false,
        code: 'INVALID_TOPIC_NAME',
        message: nameValidation.error
      };
    }

    const countToUse = partitionCount !== undefined && partitionCount !== null ? partitionCount : 3;
    const partitionValidation = TopicManager.validatePartitionCount(countToUse);
    if (!partitionValidation.valid) {
      return {
        success: false,
        code: 'INVALID_PARTITION_COUNT',
        message: partitionValidation.error
      };
    }

    if (this.topics.has(name)) {
      return {
        success: false,
        code: 'TOPIC_ALREADY_EXISTS',
        message: `Topic '${name}' already exists`
      };
    }

    const topic = new Topic(name, countToUse);
    this.topics.set(name, topic);

    return {
      success: true,
      topic: name,
      partitions: countToUse
    };
  }

  /**
   * Checks if topic exists.
   * @param {string} name 
   * @returns {boolean}
   */
  hasTopic(name) {
    return this.topics.has(name);
  }

  /**
   * Gets internal Topic entity or null.
   * @param {string} name 
   * @returns {Topic | null}
   */
  getTopic(name) {
    return this.topics.get(name) || null;
  }

  /**
   * Lists all existing topics with partition metadata.
   * @returns {Array<{ name: string, partitions: number }>}
   */
  listTopics() {
    const list = [];
    for (const topic of this.topics.values()) {
      list.push({
        name: topic.name,
        partitions: topic.partitionCount
      });
    }
    return list;
  }

  /**
   * Retrieves comprehensive metadata for topic and its partitions.
   * @param {string} name 
   * @returns {{ success: boolean, topic?: string, partitions?: number, messageCount?: number, partitionInfo?: Array<any>, code?: string, message?: string }}
   */
  getTopicInfo(name) {
    if (!this.topics.has(name)) {
      return {
        success: false,
        code: 'TOPIC_NOT_FOUND',
        message: `Topic '${name}' does not exist`
      };
    }

    const topic = this.topics.get(name);
    return {
      success: true,
      ...topic.getInfo()
    };
  }

  /**
   * Retrieves metadata for a specific partition within a topic.
   * @param {string} name 
   * @param {number} partitionId 
   */
  getPartitionInfo(name, partitionId) {
    if (!this.topics.has(name)) {
      return {
        success: false,
        code: 'TOPIC_NOT_FOUND',
        message: `Topic '${name}' does not exist`
      };
    }

    const topic = this.topics.get(name);
    return topic.getPartitionInfo(partitionId);
  }

  /**
   * Deletes a topic if empty (or if forced).
   * @param {string} name 
   * @param {boolean} [force=false]
   * @returns {{ success: boolean, topic?: string, code?: string, message?: string }}
   */
  deleteTopic(name, force = false) {
    if (!this.topics.has(name)) {
      return {
        success: false,
        code: 'TOPIC_NOT_FOUND',
        message: `Topic '${name}' does not exist`
      };
    }

    const topic = this.topics.get(name);
    const totalMessages = topic.getTotalMessageCount();
    if (totalMessages > 0 && !force) {
      return {
        success: false,
        code: 'TOPIC_NOT_EMPTY',
        message: `Cannot delete topic '${name}': topic contains ${totalMessages} message(s)`
      };
    }

    this.topics.delete(name);
    return {
      success: true,
      topic: name
    };
  }

  /**
   * Enqueues a message into a specific topic partition and returns assigned offset.
   * @param {string} name 
   * @param {any} message 
   * @param {number} [partition] 
   * @param {string|number} [key] 
   * @returns {{ success: boolean, partitionId?: number, offset?: number, code?: string, message?: string }}
   */
  enqueue(name, message, partition, key) {
    if (!this.topics.has(name)) {
      return {
        success: false,
        code: 'TOPIC_NOT_FOUND',
        message: `Topic '${name}' does not exist`
      };
    }

    const topic = this.topics.get(name);
    return topic.enqueue(message, partition, key);
  }

  /**
   * Reads a message at a specific offset from a topic partition.
   * @param {string} name 
   * @param {number} partition 
   * @param {number} offset 
   * @returns {{ success: boolean, partitionId?: number, offset?: number, message?: any, code?: string, message?: string }}
   */
  readOffset(name, partition, offset) {
    if (!this.topics.has(name)) {
      return {
        success: false,
        code: 'TOPIC_NOT_FOUND',
        message: `Topic '${name}' does not exist`
      };
    }

    const topic = this.topics.get(name);
    return topic.readOffset(partition, offset);
  }

  /**
   * Dequeues a message from a topic partition (legacy un-grouped consumption).
   * @param {string} name 
   * @param {number} [partition] 
   * @returns {{ success: boolean, message?: any, partitionId?: number, offset?: number, code?: string, message?: string }}
   */
  dequeue(name, partition) {
    if (!this.topics.has(name)) {
      return {
        success: false,
        code: 'TOPIC_NOT_FOUND',
        message: `Topic '${name}' does not exist`
      };
    }

    const topic = this.topics.get(name);
    return topic.dequeue(partition);
  }

  /**
   * Clears all topics and messages.
   */
  clear() {
    this.topics.clear();
  }
}
