/**
 * ConsumerGroupManager — Domain Orchestrator for Consumer Groups & Partition Assignments
 * 
 * Manages group lifecycle, JOIN/LEAVE operations, deterministic round-robin rebalancing,
 * group offset commitments, and group-based consumption.
 */

import { ConsumerGroup } from './consumer-group.js';
import { TopicManager } from './topic-manager.js';

export class ConsumerGroupManager {
  /**
   * @param {TopicManager} topicManager 
   */
  constructor(topicManager) {
    this.topicManager = topicManager;
    /** @type {Map<string, ConsumerGroup>} */
    this.groups = new Map();
    /** @type {Map<string, Set<string>>} Map of groupId -> subscribed topic names */
    this.groupTopics = new Map();
  }

  /**
   * Checks if group exists.
   * @param {string} groupId 
   * @returns {boolean}
   */
  hasGroup(groupId) {
    return this.groups.has(groupId);
  }

  /**
   * Gets ConsumerGroup instance or null.
   * @param {string} groupId 
   * @returns {ConsumerGroup|null}
   */
  getGroup(groupId) {
    return this.groups.get(groupId) || null;
  }

  /**
   * Registers or gets a consumer group.
   * @param {string} groupId 
   * @returns {ConsumerGroup}
   */
  ensureGroup(groupId) {
    let group = this.groups.get(groupId);
    if (!group) {
      group = new ConsumerGroup(groupId);
      this.groups.set(groupId, group);
      this.groupTopics.set(groupId, new Set());
    }
    return group;
  }

  /**
   * Registers a topic for a consumer group to consume/rebalance across.
   * @param {string} groupId 
   * @param {string} topic 
   */
  registerGroupTopic(groupId, topic) {
    if (!this.groupTopics.has(groupId)) {
      this.groupTopics.set(groupId, new Set());
    }
    this.groupTopics.get(groupId).add(topic);
  }

  /**
   * Handles a consumer joining a group and triggers rebalancing.
   * 
   * @param {string} groupId 
   * @param {string} consumerId 
   * @param {string|string[]} [topics] 
   * @returns {{ success: boolean, groupId?: string, consumerId?: string, assignments?: Array<any>, code?: string, message?: string }}
   */
  joinGroup(groupId, consumerId, topics) {
    if (typeof groupId !== 'string' || groupId.trim().length === 0) {
      return { success: false, code: 'INVALID_GROUP_ID', message: 'Group ID must be a non-empty string' };
    }
    if (typeof consumerId !== 'string' || consumerId.trim().length === 0) {
      return { success: false, code: 'INVALID_CONSUMER_ID', message: 'Consumer ID must be a non-empty string' };
    }

    const group = this.ensureGroup(groupId);
    group.addConsumer(consumerId);

    if (topics) {
      const topicList = Array.isArray(topics) ? topics : [topics];
      for (const t of topicList) {
        if (typeof t === 'string' && t.trim().length > 0) {
          this.registerGroupTopic(groupId, t);
        }
      }
    }

    this.rebalanceGroup(groupId);

    return {
      success: true,
      groupId,
      consumerId,
      assignments: group.getAssignments(consumerId)
    };
  }

  /**
   * Handles a consumer leaving a group and triggers rebalancing.
   * 
   * @param {string} groupId 
   * @param {string} consumerId 
   * @returns {{ success: boolean, groupId?: string, consumerId?: string, code?: string, message?: string }}
   */
  leaveGroup(groupId, consumerId) {
    if (!this.groups.has(groupId)) {
      return {
        success: false,
        code: 'GROUP_NOT_FOUND',
        message: `Consumer group '${groupId}' does not exist`
      };
    }

    const group = this.groups.get(groupId);
    if (!group.hasConsumer(consumerId)) {
      return {
        success: false,
        code: 'CONSUMER_NOT_IN_GROUP',
        message: `Consumer '${consumerId}' is not in group '${groupId}'`
      };
    }

    group.removeConsumer(consumerId);
    this.rebalanceGroup(groupId);

    return {
      success: true,
      groupId,
      consumerId
    };
  }

  /**
   * Deterministically rebalances partition assignments for all consumers in a group.
   * Round-robin algorithm across assigned topics.
   * 
   * @param {string} groupId 
   */
  rebalanceGroup(groupId) {
    const group = this.groups.get(groupId);
    if (!group) return;

    const sortedConsumers = group.getConsumers();
    const newAssignments = new Map();
    for (const cId of sortedConsumers) {
      newAssignments.set(cId, []);
    }

    if (sortedConsumers.length === 0) {
      group.setAssignments(newAssignments);
      return;
    }

    // Determine target topics for group
    let topicsToAssign = [];
    const subscribedSet = this.groupTopics.get(groupId);
    if (subscribedSet && subscribedSet.size > 0) {
      topicsToAssign = Array.from(subscribedSet);
    } else {
      topicsToAssign = this.topicManager.listTopics().map(t => typeof t === 'string' ? t : t.name);
    }

    // Assign partitions round-robin per topic
    for (const topicName of topicsToAssign) {
      if (!this.topicManager.hasTopic(topicName)) continue;
      const topicInfo = this.topicManager.getTopicInfo(topicName);
      const partitionCount = topicInfo.partitions || 3;

      for (let p = 0; p < partitionCount; p++) {
        const assignedConsumerIndex = p % sortedConsumers.length;
        const assignedConsumer = sortedConsumers[assignedConsumerIndex];
        const list = newAssignments.get(assignedConsumer);
        list.push({ topic: topicName, partition: p });
      }
    }

    group.setAssignments(newAssignments);
  }

  /**
   * Commits an offset for a topic/partition within a consumer group.
   * 
   * @param {string} groupId 
   * @param {string} topic 
   * @param {number} partition 
   * @param {number} offset 
   * @returns {{ success: boolean, groupId?: string, topic?: string, partition?: number, offset?: number, code?: string, message?: string }}
   */
  commitOffset(groupId, topic, partition, offset) {
    if (!this.groups.has(groupId)) {
      return {
        success: false,
        code: 'GROUP_NOT_FOUND',
        message: `Consumer group '${groupId}' does not exist`
      };
    }

    if (!this.topicManager.hasTopic(topic)) {
      return {
        success: false,
        code: 'TOPIC_NOT_FOUND',
        message: `Topic '${topic}' does not exist`
      };
    }

    const partInfo = this.topicManager.getPartitionInfo(topic, partition);
    if (!partInfo.success) {
      return {
        success: false,
        code: partInfo.code,
        message: partInfo.message
      };
    }

    if (typeof offset !== 'number' || !Number.isInteger(offset) || offset < 0 || offset > partInfo.nextOffset) {
      return {
        success: false,
        code: 'OFFSET_OUT_OF_RANGE',
        message: `Offset ${offset} is out of range for partition ${partition} of topic '${topic}'`
      };
    }

    const group = this.groups.get(groupId);
    group.commitOffset(topic, partition, offset);

    // Register topic and trigger rebalance if topic wasn't subscribed yet
    if (!this.groupTopics.get(groupId).has(topic)) {
      this.registerGroupTopic(groupId, topic);
      this.rebalanceGroup(groupId);
    }

    return {
      success: true,
      groupId,
      topic,
      partition,
      offset
    };
  }

  /**
   * Group-based message consumption.
   * 
   * @param {string} groupId 
   * @param {string} consumerId 
   * @param {string} topic 
   * @returns {{ success: boolean, topic?: string, partition?: number, offset?: number, message?: any, code?: string, message?: string }}
   */
  consume(groupId, consumerId, topic) {
    if (!this.groups.has(groupId)) {
      return {
        success: false,
        code: 'GROUP_NOT_FOUND',
        message: `Consumer group '${groupId}' does not exist`
      };
    }

    const group = this.groups.get(groupId);
    if (!group.hasConsumer(consumerId)) {
      return {
        success: false,
        code: 'CONSUMER_NOT_IN_GROUP',
        message: `Consumer '${consumerId}' is not in group '${groupId}'`
      };
    }

    if (!this.topicManager.hasTopic(topic)) {
      return {
        success: false,
        code: 'TOPIC_NOT_FOUND',
        message: `Topic '${topic}' does not exist`
      };
    }

    // Auto-register topic if group has no registered topics
    if (!this.groupTopics.get(groupId).has(topic)) {
      this.registerGroupTopic(groupId, topic);
      this.rebalanceGroup(groupId);
    }

    // Get assigned partitions for this consumer on this topic
    const assignments = group.getAssignments(consumerId).filter(a => a.topic === topic);
    if (assignments.length === 0) {
      return {
        success: true,
        topic,
        message: null
      };
    }

    // Round-robin selection among assigned partitions
    let nextIdx = group.consumerPartitionIndex.get(consumerId) || 0;
    const count = assignments.length;

    for (let i = 0; i < count; i++) {
      const pIdx = (nextIdx + i) % count;
      const targetPartition = assignments[pIdx].partition;
      const pos = group.getCurrentPosition(topic, targetPartition);

      const readResult = this.topicManager.readOffset(topic, targetPartition, pos);
      if (readResult.success && readResult.message !== null) {
        // Advance current position (transient, without auto-committing)
        group.setCurrentPosition(topic, targetPartition, pos + 1);
        group.consumerPartitionIndex.set(consumerId, (pIdx + 1) % count);

        return {
          success: true,
          topic,
          partition: targetPartition,
          offset: readResult.offset,
          message: readResult.message
        };
      }
    }

    // No available messages in assigned partitions
    group.consumerPartitionIndex.set(consumerId, (nextIdx + 1) % count);
    return {
      success: true,
      topic,
      message: null
    };
  }

  /**
   * Retrieves metadata for a consumer group.
   * 
   * @param {string} groupId 
   * @returns {{ success: boolean, groupId?: string, consumers?: string[], assignments?: object, committedOffsets?: Array<any>, code?: string, message?: string }}
   */
  getGroupInfo(groupId) {
    if (!this.groups.has(groupId)) {
      return {
        success: false,
        code: 'GROUP_NOT_FOUND',
        message: `Consumer group '${groupId}' does not exist`
      };
    }

    const group = this.groups.get(groupId);
    return {
      success: true,
      ...group.getInfo()
    };
  }

  /**
   * Clears all consumer groups and state.
   */
  clear() {
    this.groups.clear();
    this.groupTopics.clear();
  }
}
