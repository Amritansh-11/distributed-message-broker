/**
 * ConsumerGroup — Pure Domain Entity representing a Consumer Group
 * 
 * Manages group membership, partition assignment state, committed offsets,
 * and current read positions for consumers within the group.
 */

export class ConsumerGroup {
  /**
   * @param {string} groupId - Unique identifier for the consumer group
   */
  constructor(groupId) {
    this.groupId = groupId;
    /** @type {Set<string>} Registered consumer IDs */
    this.consumers = new Set();
    /** @type {Map<string, Array<{ topic: string, partition: number }>>} Map of consumerId -> assigned partitions */
    this.assignments = new Map();
    /** @type {Map<string, number>} Map of "topic:partition" -> committed offset */
    this.committedOffsets = new Map();
    /** @type {Map<string, number>} Map of "topic:partition" -> current read position */
    this.currentPositions = new Map();
    /** @type {Map<string, number>} Map of consumerId -> next assignment round-robin index */
    this.consumerPartitionIndex = new Map();
  }

  /**
   * Adds a consumer to this group.
   * @param {string} consumerId 
   * @returns {boolean} True if added, false if already present
   */
  addConsumer(consumerId) {
    const isNew = !this.consumers.has(consumerId);
    this.consumers.add(consumerId);
    return isNew;
  }

  /**
   * Removes a consumer from this group.
   * @param {string} consumerId 
   * @returns {boolean} True if removed, false if not found
   */
  removeConsumer(consumerId) {
    const exists = this.consumers.has(consumerId);
    this.consumers.delete(consumerId);
    this.assignments.delete(consumerId);
    this.consumerPartitionIndex.delete(consumerId);
    return exists;
  }

  /**
   * Checks if consumer belongs to this group.
   * @param {string} consumerId 
   * @returns {boolean}
   */
  hasConsumer(consumerId) {
    return this.consumers.has(consumerId);
  }

  /**
   * Returns array of active consumer IDs in deterministic sorted order.
   * @returns {string[]}
   */
  getConsumers() {
    return Array.from(this.consumers).sort();
  }

  /**
   * Sets partition assignments for all consumers in this group.
   * @param {Map<string, Array<{ topic: string, partition: number }>>} newAssignments 
   */
  setAssignments(newAssignments) {
    this.assignments = newAssignments;
  }

  /**
   * Gets assigned partitions for a specific consumer.
   * @param {string} consumerId 
   * @returns {Array<{ topic: string, partition: number }>}
   */
  getAssignments(consumerId) {
    return this.assignments.get(consumerId) || [];
  }

  /**
   * Commits offset for a topic and partition in this group.
   * @param {string} topic 
   * @param {number} partition 
   * @param {number} offset 
   */
  commitOffset(topic, partition, offset) {
    const key = `${topic}:${partition}`;
    this.committedOffsets.set(key, offset);
    
    // Align current position if position is behind committed offset
    const current = this.currentPositions.get(key);
    if (current === undefined || current < offset) {
      this.currentPositions.set(key, offset);
    }
  }

  /**
   * Gets committed offset for a topic partition.
   * @param {string} topic 
   * @param {number} partition 
   * @returns {number|undefined}
   */
  getCommittedOffset(topic, partition) {
    const key = `${topic}:${partition}`;
    return this.committedOffsets.get(key);
  }

  /**
   * Gets current transient read position for a topic partition.
   * @param {string} topic 
   * @param {number} partition 
   * @returns {number}
   */
  getCurrentPosition(topic, partition) {
    const key = `${topic}:${partition}`;
    const pos = this.currentPositions.get(key);
    if (pos !== undefined) {
      return pos;
    }
    const committed = this.committedOffsets.get(key);
    if (committed !== undefined) {
      return committed;
    }
    return 0;
  }

  /**
   * Sets current transient read position for a topic partition.
   * @param {string} topic 
   * @param {number} partition 
   * @param {number} position 
   */
  setCurrentPosition(topic, partition, position) {
    const key = `${topic}:${partition}`;
    this.currentPositions.set(key, position);
  }

  /**
   * Returns structured metadata summary for this consumer group.
   */
  getInfo() {
    const sortedConsumers = this.getConsumers();
    const assignmentsObj = {};
    for (const cId of sortedConsumers) {
      assignmentsObj[cId] = this.getAssignments(cId);
    }

    const committedList = [];
    for (const [key, offset] of this.committedOffsets.entries()) {
      const [topic, partitionStr] = key.split(':');
      committedList.push({
        topic,
        partition: parseInt(partitionStr, 10),
        offset
      });
    }

    return {
      groupId: this.groupId,
      consumers: sortedConsumers,
      assignments: assignmentsObj,
      committedOffsets: committedList
    };
  }
}
