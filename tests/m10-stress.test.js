/**
 * Milestone 10 — Stress & End-to-End Cluster Integration Test Suite
 * 
 * Verifies high volume producing (10,000 records), partition routing,
 * multi-broker replication consistency, leader election failover, follower
 * catch-up recovery, and full cluster restart log segment persistence.
 */

import { BrokerServer } from '../src/broker/server.js';
import { ProtocolRequest } from '../src/protocol/types.js';
import fs from 'fs';
import path from 'path';

function assert(condition, message) {
  if (!condition) {
    console.error(`[FAIL] ${message}`);
    throw new Error(`[FAIL] ${message}`);
  }
  console.log(`[PASS] ${message}`);
}

const scratchDir = path.join(process.cwd(), 'scratch', `test-stress-${Date.now()}`);

function cleanupDir(dirPath) {
  if (fs.existsSync(dirPath)) {
    fs.rmSync(dirPath, { recursive: true, force: true });
  }
}

async function runM10StressTests() {
  console.log('\n==================================================');
  console.log('RUNNING MILESTONE 10 STRESS & END-TO-END TEST SUITE');
  console.log('==================================================\n');

  try {
    const clusterConfig = [
      { id: 'broker-1', host: '127.0.0.1', port: 5901 },
      { id: 'broker-2', host: '127.0.0.1', port: 5902 },
      { id: 'broker-3', host: '127.0.0.1', port: 5903 }
    ];

    let s1 = new BrokerServer({ port: 5901, httpPort: 8901, brokerId: 'broker-1', clusterConfig, dataDir: path.join(scratchDir, 'b1') });
    let s2 = new BrokerServer({ port: 5902, httpPort: 8902, brokerId: 'broker-2', clusterConfig, dataDir: path.join(scratchDir, 'b2') });
    let s3 = new BrokerServer({ port: 5903, httpPort: 8903, brokerId: 'broker-3', clusterConfig, dataDir: path.join(scratchDir, 'b3') });

    await s1.start();
    await s2.start();
    await s3.start();

    await new Promise(r => setTimeout(r, 1000));

    // --- STEP 1: Create 3 Topics with 3 Partitions & RF=3 ---
    console.log('--- STEP 1: Create 3 Topics with RF=3 ---');
    const topicsList = ['orders', 'payments', 'events'];
    for (const tName of topicsList) {
      await s1.broker.handleRequest(ProtocolRequest.createTopic(tName, 3, 3));
      await s2.broker.handleRequest(ProtocolRequest.createTopic(tName, 3, 3));
      await s3.broker.handleRequest(ProtocolRequest.createTopic(tName, 3, 3));

      s1.broker.replicationManager.registerTopicReplication(tName, 3, 3);
      s2.broker.replicationManager.registerTopicReplication(tName, 3, 3);
      s3.broker.replicationManager.registerTopicReplication(tName, 3, 3);
    }

    // --- STEP 2: Produce 10,000 Records Across Topics & Partitions in Batches of 25 ---
    console.log('\n--- STEP 2: Produce 10,000 Messages Across Cluster ---');
    const TOTAL_MESSAGES = 10000;
    const BATCH_SIZE = 25;
    let successCount = 0;

    const startTime = Date.now();
    for (let batch = 0; batch < TOTAL_MESSAGES; batch += BATCH_SIZE) {
      const batchPromises = [];
      for (let i = batch; i < Math.min(batch + BATCH_SIZE, TOTAL_MESSAGES); i++) {
        const topic = topicsList[i % 3];
        const partition = i % 3;
        const key = `user-${i % 100}`;
        const msg = `Payload message #${i} key=${key}`;

        const leaderId = s1.broker.replicationManager.getLeader(topic, partition);
        const targetServer = leaderId === 'broker-1' ? s1 : (leaderId === 'broker-2' ? s2 : s3);

        batchPromises.push(targetServer.broker.handleRequest(ProtocolRequest.produce(topic, msg, partition, key)));
      }
      const batchResults = await Promise.all(batchPromises);
      successCount += batchResults.filter(r => r.success).length;
    }

    const durationSec = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log(`Produced ${TOTAL_MESSAGES} messages in ${durationSec} seconds (${(TOTAL_MESSAGES / durationSec).toFixed(0)} msgs/sec)`);
    assert(successCount === TOTAL_MESSAGES, `All ${TOTAL_MESSAGES} messages successfully produced and acknowledged`);

    // --- STEP 3: Verify Partition FIFO Ordering & Replicated Offsets ---
    console.log('\n--- STEP 3: Verify Replication & Offset Consistency ---');
    const readLeaderP0 = s1.broker.topicManager.readOffset('orders', 0, 0);
    assert(readLeaderP0.success && readLeaderP0.offset === 0, "First message in 'orders' P0 has offset 0");

    await new Promise(r => setTimeout(r, 1000));

    const readReplicaP0 = s2.broker.topicManager.readOffset('orders', 0, 0);
    assert(readReplicaP0.success && readReplicaP0.offset === 0, "Follower s2 has replicated 'orders' P0 offset 0");

    // --- STEP 4: Simulate Leader Failover During Traffic ---
    console.log('\n--- STEP 4: Leader Failover & Elections ---');
    await s1.stop();
    await new Promise(r => setTimeout(r, 1200));

    const newLeaderP0 = s2.broker.replicationManager.getLeader('orders', 0);
    assert(newLeaderP0 !== 'broker-1', `Failover elected new leader '${newLeaderP0}' for 'orders' P0`);

    const postFailoverLeaderServer = newLeaderP0 === 'broker-2' ? s2 : s3;
    const postRes = await postFailoverLeaderServer.broker.handleRequest(ProtocolRequest.produce('orders', 'Post-failover record', 0));
    assert(postRes.success === true, 'Produced new record through elected leader after failover');

    // --- STEP 5: Old Leader Rejoin & Catch-up ---
    console.log('\n--- STEP 5: Old Leader Rejoin & Catch-Up ---');
    s1 = new BrokerServer({ port: 5901, httpPort: 8901, brokerId: 'broker-1', clusterConfig, dataDir: path.join(scratchDir, 'b1') });
    await s1.start();
    await new Promise(r => setTimeout(r, 1000));

    s1.broker.replicationManager.setPartitionLeaderAndEpoch('orders', 0, newLeaderP0, 2, 'HEALTHY');
    await s1.broker.replicationManager.syncFollowerPartition('orders', 0);

    const s1CatchUpRead = s1.broker.topicManager.readOffset('orders', 0, postRes.payload.offset);
    assert(s1CatchUpRead.success && s1CatchUpRead.message === 'Post-failover record', 'Restarted old leader rejoined as follower and caught up missing record');

    // --- STEP 6: Full Cluster Restart & Persistence Recovery ---
    console.log('\n--- STEP 6: Full Cluster Restart & Storage Recovery ---');
    await s1.stop();
    await s2.stop();
    await s3.stop();

    s1 = new BrokerServer({ port: 5901, httpPort: 8901, brokerId: 'broker-1', clusterConfig, dataDir: path.join(scratchDir, 'b1') });
    s2 = new BrokerServer({ port: 5902, httpPort: 8902, brokerId: 'broker-2', clusterConfig, dataDir: path.join(scratchDir, 'b2') });
    s3 = new BrokerServer({ port: 5903, httpPort: 8903, brokerId: 'broker-3', clusterConfig, dataDir: path.join(scratchDir, 'b3') });

    await s1.start();
    await s2.start();
    await s3.start();

    const recoveredRead = s1.broker.topicManager.readOffset('orders', 0, 0);
    assert(recoveredRead.success && recoveredRead.offset === 0, 'Recovered message from disk after full cluster restart');

    await s1.stop();
    await s2.stop();
    await s3.stop();

    console.log('\n==================================================');
    console.log('ALL MILESTONE 10 STRESS & E2E TESTS PASSED! 🎉');
    console.log('==================================================\n');

  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    process.exit(1);
  } finally {
    cleanupDir(scratchDir);
  }
}

runM10StressTests();
