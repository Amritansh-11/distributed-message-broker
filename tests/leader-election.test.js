/**
 * Milestone 9 — Leader Election & Failure Handling Test Suite
 * 
 * Tests broker failure detection, partition leader failover, deterministic leader selection
 * (highest offset primary, alphabetical broker ID tie-breaker), leader epoch incrementing,
 * stale leader protection, old leader rejoin as follower, replica catch-up, no available leader,
 * and 3-broker multi-partition cluster integration.
 */

import { MessageBroker } from '../src/broker/broker.js';
import { BrokerServer } from '../src/broker/server.js';
import { ClusterManager } from '../src/cluster/cluster-manager.js';
import { LeaderElectionManager } from '../src/cluster/leader-election-manager.js';
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

const scratchDir = path.join(process.cwd(), 'scratch', `test-election-${Date.now()}`);

function cleanupDir(dirPath) {
  if (fs.existsSync(dirPath)) {
    fs.rmSync(dirPath, { recursive: true, force: true });
  }
}

async function runLeaderElectionTests() {
  console.log('\n==================================================');
  console.log('RUNNING MILESTONE 9 LEADER ELECTION TEST SUITE');
  console.log('==================================================\n');

  try {
    // --- TEST 1: Candidate Ranking by Offset & Tie-Breaking ---
    console.log('--- TEST 1 & 2: Candidate Ranking & Tie-Breaking ---');
    {
      const clusterConfig = [
        { id: 'broker-1', host: '127.0.0.1', port: 5101 },
        { id: 'broker-2', host: '127.0.0.1', port: 5102 },
        { id: 'broker-3', host: '127.0.0.1', port: 5103 }
      ];

      const cm1 = new ClusterManager({ brokerId: 'broker-1', clusterConfig });
      const b1 = new MessageBroker({ brokerId: 'broker-1', clusterManager: cm1, dataDir: path.join(scratchDir, 'b1') });

      // Register topic with RF=3
      b1.topicManager.createTopic('test-topic', 1);
      b1.replicationManager.registerTopicReplication('test-topic', 1, 3);

      // Simulate b1 (leader) down
      cm1.nodes.get('broker-1').markDown();
      cm1.nodes.get('broker-2').markAlive();
      cm1.nodes.get('broker-3').markAlive();

      // Set replica offsets: broker-2 offset=10, broker-3 offset=20
      b1.replicationManager.updateReplicaState('test-topic', 0, 'broker-2', { lastReplicatedOffset: 10 });
      b1.replicationManager.updateReplicaState('test-topic', 0, 'broker-3', { lastReplicatedOffset: 20 });

      // Elect leader on b1's election engine
      const electRes = b1.leaderElectionManager.electLeader('test-topic', 0);
      assert(electRes.success === true, 'Election succeeded');
      assert(electRes.leader === 'broker-3', 'broker-3 elected as leader due to higher offset (20 vs 10)');
      assert(electRes.leaderEpoch === 2, 'leaderEpoch incremented from 1 to 2');

      // Equal offset tie-breaking test
      b1.replicationManager.updateReplicaState('test-topic', 0, 'broker-2', { lastReplicatedOffset: 25 });
      b1.replicationManager.updateReplicaState('test-topic', 0, 'broker-3', { lastReplicatedOffset: 25 });

      const tieRes = b1.leaderElectionManager.electLeader('test-topic', 0);
      assert(tieRes.leader === 'broker-2', 'broker-2 elected as leader on offset tie due to alphabetical brokerId ("broker-2" < "broker-3")');
      assert(tieRes.leaderEpoch === 3, 'leaderEpoch incremented to 3');

      b1.clear();
    }

    // --- TEST 3: No Available Leader State ---
    console.log('\n--- TEST 3: No Available Leader State ---');
    {
      const clusterConfig = [
        { id: 'broker-1', host: '127.0.0.1', port: 5104 },
        { id: 'broker-2', host: '127.0.0.1', port: 5105 }
      ];
      const cm1 = new ClusterManager({ brokerId: 'broker-1', clusterConfig });
      const b1 = new MessageBroker({ brokerId: 'broker-1', clusterManager: cm1, dataDir: path.join(scratchDir, 'b2') });

      b1.topicManager.createTopic('fail-topic', 1);
      b1.replicationManager.registerTopicReplication('fail-topic', 1, 2);

      // Both brokers down
      cm1.nodes.get('broker-1').markDown();
      cm1.nodes.get('broker-2').markDown();

      const electRes = b1.leaderElectionManager.electLeader('fail-topic', 0);
      assert(electRes.success === false, 'Election failed when no candidate is ALIVE');
      assert(electRes.code === 'PARTITION_UNAVAILABLE', 'Returned code PARTITION_UNAVAILABLE');

      const prodRes = await b1.handleRequest(ProtocolRequest.produce('fail-topic', 'Payload'));
      assert(prodRes.success === false, 'PRODUCE rejected when partition has NO_LEADER');
      assert(prodRes.error.code === 'PARTITION_UNAVAILABLE', 'Returned PARTITION_UNAVAILABLE on PRODUCE');

      const consRes = await b1.handleRequest(ProtocolRequest.consume('fail-topic', 0, 0));
      assert(consRes.success === false, 'CONSUME rejected when partition has NO_LEADER');
      assert(consRes.error.code === 'PARTITION_UNAVAILABLE', 'Returned PARTITION_UNAVAILABLE on CONSUME');

      b1.clear();
    }

    // --- TEST 4: Stale Leader & Leader Epoch Enforcement ---
    console.log('\n--- TEST 4: Stale Leader & Epoch Enforcement ---');
    {
      const clusterConfig = [
        { id: 'broker-1', host: '127.0.0.1', port: 5106 },
        { id: 'broker-2', host: '127.0.0.1', port: 5107 }
      ];
      const cm1 = new ClusterManager({ brokerId: 'broker-1', clusterConfig });
      const b1 = new MessageBroker({ brokerId: 'broker-1', clusterManager: cm1, dataDir: path.join(scratchDir, 'b3') });

      b1.topicManager.createTopic('epoch-topic', 1);
      b1.replicationManager.registerTopicReplication('epoch-topic', 1, 2);

      // Transition leadership to broker-2 with epoch 2
      b1.replicationManager.setPartitionLeaderAndEpoch('epoch-topic', 0, 'broker-2', 2, 'HEALTHY');

      // Attempt produce to b1 (old leader)
      const prodRes = await b1.handleRequest(ProtocolRequest.produce('epoch-topic', 'Stale Payload'));
      assert(prodRes.success === false, 'PRODUCE rejected on old leader');
      assert(prodRes.error.code === 'NOT_LEADER', 'Error code is NOT_LEADER');
      assert(prodRes.error.leader === 'broker-2', 'Error metadata contains current leader broker-2');
      assert(prodRes.error.leaderEpoch === 2, 'Error metadata contains current leaderEpoch 2');

      // Attempt incoming stale LEADER_ANNOUNCE with epoch 1
      const announceRes = b1.leaderElectionManager.handleLeaderAnnounce('epoch-topic', 0, 'broker-1', 1, ['broker-1', 'broker-2']);
      assert(announceRes.success === false, 'Stale LEADER_ANNOUNCE rejected');
      assert(announceRes.error.code === 'STALE_LEADER', 'Error code is STALE_LEADER');

      b1.clear();
    }

    // --- TEST 5: Independent Partition Election ---
    console.log('\n--- TEST 5: Independent Partition Failover ---');
    {
      const clusterConfig = [
        { id: 'broker-1', host: '127.0.0.1', port: 5108 },
        { id: 'broker-2', host: '127.0.0.1', port: 5109 },
        { id: 'broker-3', host: '127.0.0.1', port: 5110 }
      ];
      const cm1 = new ClusterManager({ brokerId: 'broker-1', clusterConfig });
      const b1 = new MessageBroker({ brokerId: 'broker-1', clusterManager: cm1, dataDir: path.join(scratchDir, 'b4') });

      // Create topic with 3 partitions: P0 led by b1, P1 led by b2, P2 led by b3
      b1.topicManager.createTopic('multi-p-topic', 3);
      b1.replicationManager.registerTopicReplication('multi-p-topic', 3, 3);

      assert(b1.replicationManager.getLeader('multi-p-topic', 0) === 'broker-1', 'Partition 0 leader is broker-1');
      assert(b1.replicationManager.getLeader('multi-p-topic', 1) === 'broker-2', 'Partition 1 leader is broker-2');
      assert(b1.replicationManager.getLeader('multi-p-topic', 2) === 'broker-3', 'Partition 2 leader is broker-3');

      // Mark broker-1 down and trigger failover
      cm1.nodes.get('broker-1').markDown();
      cm1.nodes.get('broker-2').markAlive();
      cm1.nodes.get('broker-3').markAlive();

      b1.leaderElectionManager.handleBrokerFailure('broker-1');

      assert(b1.replicationManager.getLeader('multi-p-topic', 0) === 'broker-2', 'Partition 0 failover elected broker-2');
      assert(b1.replicationManager.getLeader('multi-p-topic', 1) === 'broker-2', 'Partition 1 leader remains broker-2');
      assert(b1.replicationManager.getLeader('multi-p-topic', 2) === 'broker-3', 'Partition 2 leader remains broker-3');

      b1.clear();
    }

    // --- TEST 6: 3-Broker Full Cluster Failover Integration ---
    console.log('\n--- TEST 6: 3-Broker Full Cluster Failover Integration ---');
    {
      const clusterConfig = [
        { id: 'broker-1', host: '127.0.0.1', port: 5151 },
        { id: 'broker-2', host: '127.0.0.1', port: 5152 },
        { id: 'broker-3', host: '127.0.0.1', port: 5153 }
      ];

      const s1 = new BrokerServer({ port: 5151, brokerId: 'broker-1', clusterConfig, dataDir: path.join(scratchDir, 'c1') });
      const s2 = new BrokerServer({ port: 5152, brokerId: 'broker-2', clusterConfig, dataDir: path.join(scratchDir, 'c2') });
      const s3 = new BrokerServer({ port: 5153, brokerId: 'broker-3', clusterConfig, dataDir: path.join(scratchDir, 'c3') });

      await s1.start();
      await s2.start();
      await s3.start();

      await new Promise(r => setTimeout(r, 1000));

      // Create topic on broker-1 (Leader of P0) with RF=3
      const createRes = await s1.broker.handleRequest(ProtocolRequest.createTopic('failover-topic', 1, 3));
      assert(createRes.success === true, 'Created failover-topic with RF=3');

      // Synchronize metadata on s2 and s3
      s2.broker.replicationManager.registerTopicReplication('failover-topic', 1, 3);
      s3.broker.replicationManager.registerTopicReplication('failover-topic', 1, 3);

      // Produce initial records to leader (broker-1)
      const p1 = await s1.broker.handleRequest(ProtocolRequest.produce('failover-topic', 'Record 0 before failover', 0));
      const p2 = await s1.broker.handleRequest(ProtocolRequest.produce('failover-topic', 'Record 1 before failover', 0));
      assert(p1.success && p1.payload.offset === 0, 'Produced Record 0 to Leader broker-1');
      assert(p2.success && p2.payload.offset === 1, 'Produced Record 1 to Leader broker-1');

      await new Promise(r => setTimeout(r, 500));

      // Stop broker-1 (simulating leader failure)
      await s1.stop();

      // Wait for automatic socket failure detection & failover election on s2 / s3
      await new Promise(r => setTimeout(r, 1200));

      const newLeader = s2.broker.replicationManager.getLeader('failover-topic', 0);
      const newEpoch = s2.broker.replicationManager.getLeaderEpoch('failover-topic', 0);
      assert(newLeader === 'broker-2', 'broker-2 elected as new leader for failover-topic');
      assert(newEpoch === 2, 'leaderEpoch incremented to 2');

      // Produce new record through new leader (broker-2)
      const p3 = await s2.broker.handleRequest(ProtocolRequest.produce('failover-topic', 'Record 2 after failover', 0));
      assert(p3.success === true, 'Produced Record 2 through new leader broker-2');
      assert(p3.payload.offset === 2, 'New record received offset 2 on new leader');

      // Consume records from new leader (broker-2)
      const c0 = await s2.broker.handleRequest(ProtocolRequest.consume('failover-topic', 0, 0));
      const c2 = await s2.broker.handleRequest(ProtocolRequest.consume('failover-topic', 0, 2));
      assert(c0.success && c0.payload.message === 'Record 0 before failover', 'Consumed Record 0 from new leader');
      assert(c2.success && c2.payload.message === 'Record 2 after failover', 'Consumed Record 2 from new leader');

      // Restart broker-1 (old leader rejoins)
      const s1_new = new BrokerServer({ port: 5151, brokerId: 'broker-1', clusterConfig, dataDir: path.join(scratchDir, 'c1') });
      await s1_new.start();

      await new Promise(r => setTimeout(r, 1000));

      // Update s1_new metadata to reflect current leader broker-2 with epoch 2
      s1_new.broker.replicationManager.setPartitionLeaderAndEpoch('failover-topic', 0, 'broker-2', 2, 'HEALTHY', ['broker-1', 'broker-2', 'broker-3']);

      // Verify broker-1 does NOT automatically become leader
      assert(!s1_new.broker.replicationManager.isLeader('failover-topic', 0), 'Restarted broker-1 remains follower and does NOT reclaim leadership');

      // Trigger catch-up sync on broker-1
      await s1_new.broker.replicationManager.syncFollowerPartition('failover-topic', 0);

      // Verify broker-1 caught up and preserved record 2 in its local partition log
      const c1_read = s1_new.broker.topicManager.readOffset('failover-topic', 0, 2);
      assert(c1_read.success && c1_read.message === 'Record 2 after failover', 'Restarted broker-1 synchronized missing Record 2 as follower');

      await s1_new.stop();
      await s2.stop();
      await s3.stop();
    }

    console.log('\n==================================================');
    console.log('ALL MILESTONE 9 LEADER ELECTION TESTS PASSED! 🎉');
    console.log('==================================================\n');

  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    process.exit(1);
  } finally {
    cleanupDir(scratchDir);
  }
}

runLeaderElectionTests();
