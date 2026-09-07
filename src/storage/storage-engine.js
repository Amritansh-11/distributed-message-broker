/**
 * StorageEngine — Persistent Storage Engine Coordinator
 * 
 * Manages partition log directory structures, append-only segment files,
 * durability policies, consumer-group offset persistence, and complete
 * crash recovery on broker startup.
 */

import fs from 'fs';
import path from 'path';
import { LogSegment } from './segment.js';

export class StorageEngine {
  /**
   * @param {object} [options={}] 
   * @param {string} [options.dataDir='./data'] - Root data directory
   * @param {number} [options.maxMessagesPerSegment=1000] - Max messages per segment before rollover
   */
  constructor(options = {}) {
    this.dataDir = options.dataDir || './data';
    this.maxMessagesPerSegment = options.maxMessagesPerSegment || 1000;
    this.topicsDir = path.join(this.dataDir, 'topics');
    this.groupsDir = path.join(this.dataDir, 'consumer-groups');

    /** @type {Map<string, LogSegment>} Active segment per topic:partition key */
    this.activeSegments = new Map();
    /** @type {Map<string, LogSegment[]>} All segments per topic:partition key */
    this.allSegments = new Map();
  }

  /**
   * Initializes storage root directories.
   */
  init() {
    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
    }
    if (!fs.existsSync(this.topicsDir)) {
      fs.mkdirSync(this.topicsDir, { recursive: true });
    }
    if (!fs.existsSync(this.groupsDir)) {
      fs.mkdirSync(this.groupsDir, { recursive: true });
    }
  }

  /**
   * Ensures partition log directory exists.
   * @param {string} topic 
   * @param {number} partitionId 
   * @returns {string} Partition directory path
   */
  ensurePartitionDir(topic, partitionId) {
    const pDir = path.join(this.topicsDir, topic, `partition-${partitionId}`);
    if (!fs.existsSync(pDir)) {
      fs.mkdirSync(pDir, { recursive: true });
    }
    return pDir;
  }

  /**
   * Gets or creates the active segment for a partition, handling rollover when full.
   * 
   * @param {string} topic 
   * @param {number} partitionId 
   * @param {number} currentOffset 
   * @returns {LogSegment}
   */
  getActiveSegment(topic, partitionId, currentOffset = 0) {
    const key = `${topic}:${partitionId}`;
    let segment = this.activeSegments.get(key);

    if (!segment) {
      const pDir = this.ensurePartitionDir(topic, partitionId);
      segment = new LogSegment(pDir, currentOffset, this.maxMessagesPerSegment);
      this.activeSegments.set(key, segment);
      
      const list = this.allSegments.get(key) || [];
      list.push(segment);
      this.allSegments.set(key, list);
    } else if (segment.isFull()) {
      // Segment rollover: create new segment starting at currentOffset
      const pDir = this.ensurePartitionDir(topic, partitionId);
      segment = new LogSegment(pDir, currentOffset, this.maxMessagesPerSegment);
      this.activeSegments.set(key, segment);

      const list = this.allSegments.get(key) || [];
      list.push(segment);
      this.allSegments.set(key, list);
    }

    return segment;
  }

  /**
   * Persists a produced message to disk log segment.
   * 
   * @param {string} topic 
   * @param {number} partitionId 
   * @param {number} offset 
   * @param {any} message 
   * @returns {{ offset: number, message: any, bytesWritten: number }}
   */
  appendMessage(topic, partitionId, offset, message) {
    this.ensurePartitionDir(topic, partitionId);
    const segment = this.getActiveSegment(topic, partitionId, offset);
    return segment.appendSync(offset, message);
  }

  /**
   * Persists committed consumer group offsets to disk.
   * 
   * @param {string} groupId 
   * @param {object} groupInfoData 
   */
  saveConsumerGroupOffsets(groupId, groupInfoData) {
    this.init();
    const groupFile = path.join(this.groupsDir, `${groupId}.json`);
    const dataStr = JSON.stringify(groupInfoData, null, 2);
    fs.writeFileSync(groupFile, dataStr, 'utf8');
  }

  /**
   * Flushes all open active segments to disk.
   */
  flushAll() {
    for (const segment of this.activeSegments.values()) {
      if (segment && typeof segment.flush === 'function') {
        segment.flush();
      }
    }
  }

  /**
   * Recovers broker in-memory state from disk logs on startup.
   * Reconstructs topics, partition logs, offsets, and consumer-group checkpoints.
   * 
   * @param {import('../broker/topic-manager.js').TopicManager} topicManager 
   * @param {import('../broker/consumer-group-manager.js').ConsumerGroupManager} consumerGroupManager 
   */
  recoverAllState(topicManager, consumerGroupManager) {
    this.init();

    // 1. Recover Topic & Partition Logs
    if (fs.existsSync(this.topicsDir)) {
      const topicEntries = fs.readdirSync(this.topicsDir, { withFileTypes: true });

      for (const tEntry of topicEntries) {
        if (!tEntry.isDirectory()) continue;
        const topicName = tEntry.name;
        const topicPath = path.join(this.topicsDir, topicName);

        const partitionEntries = fs.readdirSync(topicPath, { withFileTypes: true });
        const partitionDirs = partitionEntries.filter(e => e.isDirectory() && e.name.startsWith('partition-'));

        if (partitionDirs.length === 0) continue;

        // Calculate partition count (e.g. max index + 1 or default 3)
        let maxPartitionId = 0;
        for (const pDir of partitionDirs) {
          const pId = parseInt(pDir.name.replace('partition-', ''), 10);
          if (!isNaN(pId) && pId > maxPartitionId) {
            maxPartitionId = pId;
          }
        }
        const partitionCount = Math.max(partitionDirs.length, maxPartitionId + 1);

        // Ensure topic is registered in TopicManager
        if (!topicManager.hasTopic(topicName)) {
          topicManager.createTopic(topicName, partitionCount);
        }

        const topicEntity = topicManager.getTopic(topicName);

        // Process each partition log directory
        for (const pDir of partitionDirs) {
          const pId = parseInt(pDir.name.replace('partition-', ''), 10);
          if (isNaN(pId)) continue;

          const pDirPath = path.join(topicPath, pDir.name);
          const pKey = `${topicName}:${pId}`;

          const files = fs.readdirSync(pDirPath);
          const logFiles = files.filter(f => f.endsWith('.log'));

          // Sort segment files deterministically by base offset
          logFiles.sort((a, b) => {
            const offA = LogSegment.parseBaseOffset(a) ?? 0;
            const offB = LogSegment.parseBaseOffset(b) ?? 0;
            return offA - offB;
          });

          const partitionEntity = topicEntity.partitions.get(pId);
          if (!partitionEntity) continue;

          const segmentList = [];

          if (logFiles.length === 0) {
            // Initial segment
            const initSeg = new LogSegment(pDirPath, 0, this.maxMessagesPerSegment);
            this.activeSegments.set(pKey, initSeg);
            this.allSegments.set(pKey, [initSeg]);
            continue;
          }

          for (const logFile of logFiles) {
            const baseOffset = LogSegment.parseBaseOffset(logFile) ?? 0;
            const segment = new LogSegment(pDirPath, baseOffset, this.maxMessagesPerSegment);
            
            const res = segment.readRecordsSync();
            if (res.corrupted) {
              throw new Error(`[StorageEngine Error] Corrupted storage log file ${segment.filePath}: ${res.error}`);
            }

            for (const record of res.records) {
              // Reconstruct in-memory partition state
              partitionEntity.messages.push({ offset: record.offset, message: record.message });
              partitionEntity.nextOffset = Math.max(partitionEntity.nextOffset, record.offset + 1);
            }

            segmentList.push(segment);
          }

          this.allSegments.set(pKey, segmentList);
          // Last segment is the active segment
          const lastSegment = segmentList[segmentList.length - 1];
          this.activeSegments.set(pKey, lastSegment);
        }
      }
    }

    // 2. Recover Consumer Group Committed Offsets
    if (fs.existsSync(this.groupsDir)) {
      const groupFiles = fs.readdirSync(this.groupsDir);
      for (const gFile of groupFiles) {
        if (!gFile.endsWith('.json')) continue;
        const gFilePath = path.join(this.groupsDir, gFile);
        try {
          const content = fs.readFileSync(gFilePath, 'utf8');
          const groupData = JSON.parse(content);

          if (groupData && groupData.groupId) {
            const groupId = groupData.groupId;
            consumerGroupManager.ensureGroup(groupId);

            if (Array.isArray(groupData.committedOffsets)) {
              for (const item of groupData.committedOffsets) {
                if (item.topic && typeof item.partition === 'number' && typeof item.offset === 'number') {
                  consumerGroupManager.commitOffset(groupId, item.topic, item.partition, item.offset);
                }
              }
            }
          }
        } catch (err) {
          console.warn(`[StorageEngine] Failed to recover consumer group file ${gFile}: ${err.message}`);
        }
      }
    }
  }

  /**
   * Resets active segment maps.
   */
  close() {
    this.flushAll();
    this.activeSegments.clear();
    this.allSegments.clear();
  }
}
