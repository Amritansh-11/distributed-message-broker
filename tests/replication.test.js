import fs from 'fs';
import path from 'path';
import net from 'net';
import { BrokerServer } from '../src/broker/server.js';
import { ClusterManager } from '../src/cluster/cluster-manager.js';
import { ReplicationManager } from '../src/cluster/replication-manager.js';
import { StreamFramer } from '../src/protocol/framing.js';
import { ProtocolEncoder, ProtocolDecoder } from '../src/protocol/codec.js';
import { ProtocolRequest } from '../src/protocol/types.js';

const SCRATCH_BASE = path.join(process.cwd(), 'scratch');

function cleanupDataDir(dir) {
  if (fs.existsSync(dir)) {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

function assert(condition, message) {
  if (!condition) {
    throw new Error(`[FAIL] ${message}`);
  }
  console.log(`[PASS] ${message}`);
}

async function runReplicationTests() {
  console.log('==================================================');
  console.log('RUNNING MILESTONE 8 PARTITION REPLICATION TEST SUITE');
  console.log('==================================================\n');

  try {
    // TEST 1: Replication Factor Validation
    console.log('--- TEST 1: Replication Factor Validation ---');
    const cm1 = new ClusterManager({
      brokerId: 'b1',
      clusterConfig: [
        { id: 'b1', host: '127.0.0.1', port: 5041 },
        { id: 'b2', host: '127.0.0.1', port: 5042 }
      ]
    });
    const rm1 = new ReplicationManager({ clusterManager: cm1, topicManager: null, storageEngine: null });

    const val1 = rm1.validateReplicationFactor(1);
    assert(val1.valid === true, 'Replication factor 1 is valid for 2 brokers');
    const val2 = rm1.validateReplicationFactor(2);
    assert(val2.valid === true, 'Replication factor 2 is valid for 2 brokers');
    const val3 = rm1.validateReplicationFactor(3);
    assert(val3.valid === false && val3.error.code === 'INVALID_REPLICATION_FACTOR', 'Replication factor 3 rejected for 2 brokers');

    // TEST 2 & 3: Deterministic Replica Assignment & Leadership Distribution
    console.log('\n--- TEST 2 & 3: Deterministic Replica Assignment & Leaders ---');
    const cm3 = new ClusterManager({
      brokerId: 'broker-1',
      clusterConfig: [
        { id: 'broker-1', host: '127.0.0.1', port: 5041 },
        { id: 'broker-2', host: '127.0.0.1', port: 5042 },
        { id: 'broker-3', host: '127.0.0.1', port: 5043 }
      ]
    });
    const rm3 = new ReplicationManager({ clusterManager: cm3, topicManager: null, storageEngine: null });

    rm3.registerTopicReplication('orders', 3, 3);
    const metaP0 = rm3.getPartitionMetadata('orders', 0);
    const metaP1 = rm3.getPartitionMetadata('orders', 1);
    const metaP2 = rm3.getPartitionMetadata('orders', 2);

    assert(metaP0.leader === 'broker-1', 'Partition 0 leader is broker-1');
    assert(JSON.stringify(metaP0.replicas) === JSON.stringify(['broker-1', 'broker-2', 'broker-3']), 'Partition 0 replicas are broker-1, broker-2, broker-3');

    assert(metaP1.leader === 'broker-2', 'Partition 1 leader is broker-2');
    assert(JSON.stringify(metaP1.replicas) === JSON.stringify(['broker-2', 'broker-3', 'broker-1']), 'Partition 1 replicas are broker-2, broker-3, broker-1');

    assert(metaP2.leader === 'broker-3', 'Partition 2 leader is broker-3');

    // TEST 4 & 5: PRODUCE to Leader vs Follower (NOT_LEADER Error)
    console.log('\n--- TEST 4 & 5: PRODUCE to Leader vs Follower ---');
    const d1 = path.join(SCRATCH_BASE, `rep-b1-${Date.now()}`);
    const d2 = path.join(SCRATCH_BASE, `rep-b2-${Date.now()}`);
    const d3 = path.join(SCRATCH_BASE, `rep-b3-${Date.now()}`);
    cleanupDataDir(d1);
    cleanupDataDir(d2);
    cleanupDataDir(d3);

    const cluster3Config = [
      { id: 'broker-1', host: '127.0.0.1', port: 5051 },
      { id: 'broker-2', host: '127.0.0.1', port: 5052 },
      { id: 'broker-3', host: '127.0.0.1', port: 5053 }
    ];

    const b1 = new BrokerServer({ brokerId: 'broker-1', port: 5051, host: '127.0.0.1', dataDir: d1, clusterConfig: cluster3Config, heartbeatIntervalMs: 300 });
    const b2 = new BrokerServer({ brokerId: 'broker-2', port: 5052, host: '127.0.0.1', dataDir: d2, clusterConfig: cluster3Config, heartbeatIntervalMs: 300 });
    const b3 = new BrokerServer({ brokerId: 'broker-3', port: 5053, host: '127.0.0.1', dataDir: d3, clusterConfig: cluster3Config, heartbeatIntervalMs: 300 });

    await b1.start();
    await b2.start();
    await b3.start();

    // Wait for inter-broker handshakes
    await new Promise(r => setTimeout(r, 1200));

    // Create topic with replication factor 3
    const createRes = await b1.broker.handleRequest(ProtocolRequest.createTopic('replicated_topic', 3, 3));
    assert(createRes.success === true, 'Created replicated_topic with RF=3');

    // Register replication mapping on b2 and b3 as well for test environment sync
    b2.broker.replicationManager.registerTopicReplication('replicated_topic', 3, 3);
    b3.broker.replicationManager.registerTopicReplication('replicated_topic', 3, 3);
    b2.broker.topicManager.createTopic('replicated_topic', 3);
    b3.broker.topicManager.createTopic('replicated_topic', 3);

    // Produce to Leader (Partition 0 -> Leader is broker-1)
    const prodRes1 = await b1.broker.handleRequest(ProtocolRequest.produce('replicated_topic', 'Msg for Leader', 0));
    assert(prodRes1.success === true, 'PRODUCE to Leader broker-1 partition 0 succeeds');
    assert(prodRes1.payload.offset === 0, 'Returned offset is 0');

    // Produce to Follower (Partition 0 -> Follower is broker-2)
    const prodResFollower = await b2.broker.handleRequest(ProtocolRequest.produce('replicated_topic', 'Msg for Follower', 0));
    assert(prodResFollower.success === false, 'PRODUCE to Follower broker-2 rejected');
    assert(prodResFollower.error.code === 'NOT_LEADER', 'Error code is NOT_LEADER');
    assert(prodResFollower.error.leader === 'broker-1', 'Error contains correct leader broker ID broker-1');

    // TEST 6, 7, 8, 9, 10: Follower Record Persistence, Offset Preservation & ACKs
    console.log('\n--- TEST 6-10: Follower Replication & Preservation ---');
    // Read from follower broker-2's local log for partition 0
    const b2Partition0Msg = b2.broker.topicManager.readOffset('replicated_topic', 0, 0);
    assert(b2Partition0Msg.success === true, 'Follower broker-2 persisted replicated record at offset 0');
    assert(b2Partition0Msg.message === 'Msg for Leader', 'Follower preserved leader record content');

    const b3Partition0Msg = b3.broker.topicManager.readOffset('replicated_topic', 0, 0);
    assert(b3Partition0Msg.success === true, 'Follower broker-3 persisted replicated record at offset 0');

    // TEST 11 & 12: High-Water Mark Calculation
    console.log('\n--- TEST 11 & 12: High-Water Mark Calculation ---');
    const pMetaInfo = b1.broker.replicationManager.getPartitionMetadata('replicated_topic', 0);
    assert(pMetaInfo.highWaterMark === 0, 'High-water mark is updated to 0');

    // TEST 13, 14, 15: Follower Failure & Catch-Up Recovery (REPLICA_SYNC)
    console.log('\n--- TEST 13-15: Follower Failure & Recovery Catch-Up ---');
    // Stop follower broker-3
    await b3.stop();
    await new Promise(r => setTimeout(r, 1000));

    // Produce 2 additional messages on leader broker-1 while broker-3 is DOWN
    await b1.broker.handleRequest(ProtocolRequest.produce('replicated_topic', 'Msg 1 while b3 down', 0));
    await b1.broker.handleRequest(ProtocolRequest.produce('replicated_topic', 'Msg 2 while b3 down', 0));

    // Restart follower broker-3
    const b3Restart = new BrokerServer({ brokerId: 'broker-3', port: 5053, host: '127.0.0.1', dataDir: d3, clusterConfig: cluster3Config, heartbeatIntervalMs: 300 });
    await b3Restart.start();
    b3Restart.broker.topicManager.createTopic('replicated_topic', 3);
    b3Restart.broker.replicationManager.registerTopicReplication('replicated_topic', 3, 3);

    await new Promise(r => setTimeout(r, 1000));

    // Trigger follower catch-up sync
    await b3Restart.broker.replicationManager.syncFollowerPartition('replicated_topic', 0);

    const b3SyncRes1 = b3Restart.broker.topicManager.readOffset('replicated_topic', 0, 1);
    const b3SyncRes2 = b3Restart.broker.topicManager.readOffset('replicated_topic', 0, 2);

    assert(b3SyncRes1.success === true && b3SyncRes1.message === 'Msg 1 while b3 down', 'Restarted broker-3 synchronized missing offset 1');
    assert(b3SyncRes2.success === true && b3SyncRes2.message === 'Msg 2 while b3 down', 'Restarted broker-3 synchronized missing offset 2');

    // TEST 16 & 17: Persistent Replica Data After Restart
    console.log('\n--- TEST 16 & 17: Persistent Replica Data ---');
    assert(fs.existsSync(path.join(d2, 'topics', 'replicated_topic', 'partition-0', '000000000000.log')), 'Follower broker-2 has disk log segment file');
    assert(fs.existsSync(path.join(d3, 'topics', 'replicated_topic', 'partition-0', '000000000000.log')), 'Follower broker-3 has disk log segment file');

    await b1.stop();
    await b2.stop();
    await b3Restart.stop();

    cleanupDataDir(d1);
    cleanupDataDir(d2);
    cleanupDataDir(d3);

    console.log('\n==================================================');
    console.log('ALL MILESTONE 8 REPLICATION TESTS PASSED! 🎉');
    console.log('==================================================\n');
  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    process.exit(1);
  }
}

runReplicationTests();
