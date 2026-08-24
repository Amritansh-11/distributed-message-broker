/**
 * TopicManager — Pure Domain Entity for Broker Topic Lifecycle & Queues
 * 
 * Responsible for creating, listing, deleting, validating, and managing
 * isolated FIFO message queues per topic.
 */

export class TopicManager {
  constructor() {
    /** @type {Map<string, { name: string, messages: Array<any> }>} */
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
   * Creates a new topic.
   * 
   * @param {string} name 
   * @returns {{ success: boolean, topic?: string, code?: string, message?: string }}
   */
  createTopic(name) {
    const validation = TopicManager.validateTopicName(name);
    if (!validation.valid) {
      return {
        success: false,
        code: 'INVALID_TOPIC_NAME',
        message: validation.error
      };
    }

    if (this.topics.has(name)) {
      return {
        success: false,
        code: 'TOPIC_ALREADY_EXISTS',
        message: `Topic '${name}' already exists`
      };
    }

    this.topics.set(name, {
      name,
      messages: []
    });

    return {
      success: true,
      topic: name
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
   * Gets internal topic representation or null.
   * @param {string} name 
   * @returns {{ name: string, messages: Array<any> } | null}
   */
  getTopic(name) {
    return this.topics.get(name) || null;
  }

  /**
   * Lists all existing topic names.
   * @returns {string[]}
   */
  listTopics() {
    return Array.from(this.topics.keys());
  }

  /**
   * Retrieves topic metadata.
   * @param {string} name 
   * @returns {{ success: boolean, topic?: string, messageCount?: number, code?: string, message?: string }}
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
      topic: name,
      messageCount: topic.messages.length
    };
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
    if (topic.messages.length > 0 && !force) {
      return {
        success: false,
        code: 'TOPIC_NOT_EMPTY',
        message: `Cannot delete topic '${name}': topic contains ${topic.messages.length} message(s)`
      };
    }

    this.topics.delete(name);
    return {
      success: true,
      topic: name
    };
  }

  /**
   * Enqueues a message into a specific topic queue.
   * @param {string} name 
   * @param {any} message 
   * @returns {{ success: boolean, code?: string, message?: string }}
   */
  enqueue(name, message) {
    if (!this.topics.has(name)) {
      return {
        success: false,
        code: 'TOPIC_NOT_FOUND',
        message: `Topic '${name}' does not exist`
      };
    }

    const topic = this.topics.get(name);
    topic.messages.push(message);
    return { success: true };
  }

  /**
   * Dequeues a message from a specific topic queue (FIFO).
   * @param {string} name 
   * @returns {{ success: boolean, message?: any, code?: string, message?: string }}
   */
  dequeue(name) {
    if (!this.topics.has(name)) {
      return {
        success: false,
        code: 'TOPIC_NOT_FOUND',
        message: `Topic '${name}' does not exist`
      };
    }

    const topic = this.topics.get(name);
    if (topic.messages.length === 0) {
      return {
        success: true,
        message: null
      };
    }

    const msg = topic.messages.shift();
    return {
      success: true,
      message: msg
    };
  }

  /**
   * Clears all topics and messages.
   */
  clear() {
    this.topics.clear();
  }
}
