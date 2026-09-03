import fs from 'fs';
import path from 'path';
import net from 'net';
import { BrokerServer } from '../src/broker/server.js';
import { ClusterManager } from '../src/cluster/cluster-manager.js';
import { BrokerNode } from '../src/cluster/broker-node.js';
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

async function runClusterTests() {
  console.log('==================================================');
  console.log('RUNNING MILESTONE 7 MULTI-BROKER CLUSTER TEST SUITE');
  console.log('==================================================\n');

  try {
    // TEST 1: Broker Node Abstraction
    console.log('--- TEST 1: Broker Node Abstraction ---');
    const node1 = new BrokerNode({ id: 'broker-1', host: '127.0.0.1', port: 5000, status: 'unknown' });
    assert(node1.id === 'broker-1' && node1.port === 5000, 'BrokerNode initialized with ID and port');
    assert(node1.status === 'unknown', 'Initial status is unknown');
    node1.markAlive();
    assert(node1.status === 'alive' && node1.lastHeartbeat !== null, 'markAlive sets status alive and updates timestamp');
    node1.markDown();
    assert(node1.status === 'down' && node1.socket === null, 'markDown sets status down and clears socket');

    // TEST 2: ClusterManager Configuration & Local Identification
    console.log('\n--- TEST 2: ClusterManager Configuration ---');
    const staticConfig = [
      { id: 'b1', host: '127.0.0.1', port: 5011 },
      { id: 'b2', host: '127.0.0.1', port: 5012 },
      { id: 'b3', host: '127.0.0.1', port: 5013 }
    ];
    const cm = new ClusterManager({ brokerId: 'b1', host: '127.0.0.1', port: 5011, clusterConfig: staticConfig });
    assert(cm.localBrokerId === 'b1', 'Identified local broker b1');
    assert(cm.nodes.size === 3, 'Loaded 3 nodes from static cluster config');
    assert(cm.nodes.get('b1').status === 'alive', 'Local broker b1 is marked ALIVE');
    assert(cm.nodes.get('b2').status === 'unknown', 'Remote broker b2 is initially UNKNOWN');

    // TEST 3 & 4: BROKER_HELLO Handshake & BROKER_PING Heartbeat over TCP
    console.log('\n--- TEST 3 & 4: Handshake & Heartbeat over TCP ---');
    const dirB1 = path.join(SCRATCH_BASE, `test-cluster-b1-${Date.now()}`);
    const dirB2 = path.join(SCRATCH_BASE, `test-cluster-b2-${Date.now()}`);
    cleanupDataDir(dirB1);
    cleanupDataDir(dirB2);

    const clusterConf2 = [
      { id: 'broker-1', host: '127.0.0.1', port: 5021 },
      { id: 'broker-2', host: '127.0.0.1', port: 5022 }
    ];

    const serverB1 = new BrokerServer({ brokerId: 'broker-1', port: 5021, host: '127.0.0.1', dataDir: dirB1, clusterConfig: clusterConf2, heartbeatIntervalMs: 300, heartbeatTimeoutMs: 1000 });
    const serverB2 = new BrokerServer({ brokerId: 'broker-2', port: 5022, host: '127.0.0.1', dataDir: dirB2, clusterConfig: clusterConf2, heartbeatIntervalMs: 300, heartbeatTimeoutMs: 1000 });

    await serverB1.start();
    await serverB2.start();

    // Wait for inter-broker handshake & heartbeat loop to execute
    await new Promise((r) => setTimeout(r, 1200));

    const infoB1 = serverB1.clusterManager.getClusterInfo();
    const infoB2 = serverB2.clusterManager.getClusterInfo();

    const node2FromB1 = infoB1.find(n => n.id === 'broker-2');
    const node1FromB2 = infoB2.find(n => n.id === 'broker-1');

    assert(node2FromB1 && node2FromB1.status === 'alive', 'broker-1 detected broker-2 as ALIVE');
    assert(node1FromB2 && node1FromB2.status === 'alive', 'broker-2 detected broker-1 as ALIVE');

    // TEST 5 & 6: Failure Detection when Broker Stops
    console.log('\n--- TEST 5 & 6: Failure Detection on Broker Shutdown ---');
    await serverB2.stop();

    // Wait for socket close event & heartbeat check timeout in serverB1
    await new Promise((r) => setTimeout(r, 1500));

    const infoAfterStop = serverB1.clusterManager.getClusterInfo();
    const node2AfterStop = infoAfterStop.find(n => n.id === 'broker-2');
    assert(node2AfterStop && node2AfterStop.status === 'down', 'broker-1 detected broker-2 as DOWN after shutdown');

    // TEST 7: Failure Recovery when Broker Restarts
    console.log('\n--- TEST 7: Failure Recovery on Broker Restart ---');
    const serverB2Restart = new BrokerServer({ brokerId: 'broker-2', port: 5022, host: '127.0.0.1', dataDir: dirB2, clusterConfig: clusterConf2, heartbeatIntervalMs: 300, heartbeatTimeoutMs: 1000 });
    await serverB2Restart.start();

    // Wait for reconnect and handshake
    await new Promise((r) => setTimeout(r, 1200));

    const infoAfterRestart = serverB1.clusterManager.getClusterInfo();
    const node2AfterRestart = infoAfterRestart.find(n => n.id === 'broker-2');
    assert(node2AfterRestart && node2AfterRestart.status === 'alive', 'broker-1 detected broker-2 as ALIVE after restart');

    await serverB1.stop();
    await serverB2Restart.stop();

    cleanupDataDir(dirB1);
    cleanupDataDir(dirB2);

    // TEST 8: GET_CLUSTER_INFO Client TCP Request
    console.log('\n--- TEST 8: GET_CLUSTER_INFO Client TCP Request ---');
    const dirB3 = path.join(SCRATCH_BASE, `test-cluster-b3-${Date.now()}`);
    cleanupDataDir(dirB3);

    const cluster3Config = [
      { id: 'broker-1', host: '127.0.0.1', port: 5023 },
      { id: 'broker-2', host: '127.0.0.1', port: 5024 },
      { id: 'broker-3', host: '127.0.0.1', port: 5025 }
    ];

    const serverInfo = new BrokerServer({ brokerId: 'broker-1', port: 5023, host: '127.0.0.1', dataDir: dirB3, clusterConfig: cluster3Config });
    await serverInfo.start();

    const clientSocket = net.createConnection({ port: 5023, host: '127.0.0.1' });
    const framer = new StreamFramer();
    let clusterInfoRes = null;

    clientSocket.on('connect', () => {
      clientSocket.write(ProtocolEncoder.encode(ProtocolRequest.getClusterInfo()));
    });

    clientSocket.on('data', (chunk) => {
      const frames = framer.feed(chunk);
      if (frames.length > 0) {
        clusterInfoRes = ProtocolDecoder.decode(frames[0].raw);
      }
      clientSocket.end();
    });

    await new Promise((resolve) => clientSocket.on('close', resolve));
    assert(clusterInfoRes && clusterInfoRes.parsed && clusterInfoRes.parsed.type === 'CLUSTER_INFO', 'Client receives CLUSTER_INFO response');
    assert(Array.isArray(clusterInfoRes.parsed.payload.brokers), 'CLUSTER_INFO contains brokers array');
    assert(clusterInfoRes.parsed.payload.brokers.length === 3, 'CLUSTER_INFO contains 3 cluster brokers');

    await serverInfo.stop();
    cleanupDataDir(dirB3);

    // TEST 9 & 10: 3-Broker Cluster Integration & Isolated Data Directories
    console.log('\n--- TEST 9 & 10: 3-Broker Cluster Integration & Isolated Storage ---');
    const d1 = path.join(SCRATCH_BASE, `cluster-b1-${Date.now()}`);
    const d2 = path.join(SCRATCH_BASE, `cluster-b2-${Date.now()}`);
    const d3 = path.join(SCRATCH_BASE, `cluster-b3-${Date.now()}`);
    cleanupDataDir(d1);
    cleanupDataDir(d2);
    cleanupDataDir(d3);

    const cluster3ConfigIntegration = [
      { id: 'broker-1', host: '127.0.0.1', port: 5031 },
      { id: 'broker-2', host: '127.0.0.1', port: 5032 },
      { id: 'broker-3', host: '127.0.0.1', port: 5033 }
    ];

    const b1 = new BrokerServer({ brokerId: 'broker-1', port: 5031, host: '127.0.0.1', dataDir: d1, clusterConfig: cluster3ConfigIntegration, heartbeatIntervalMs: 300, heartbeatTimeoutMs: 1000 });
    const b2 = new BrokerServer({ brokerId: 'broker-2', port: 5032, host: '127.0.0.1', dataDir: d2, clusterConfig: cluster3ConfigIntegration, heartbeatIntervalMs: 300, heartbeatTimeoutMs: 1000 });
    const b3 = new BrokerServer({ brokerId: 'broker-3', port: 5033, host: '127.0.0.1', dataDir: d3, clusterConfig: cluster3ConfigIntegration, heartbeatIntervalMs: 300, heartbeatTimeoutMs: 1000 });

    await b1.start();
    await b2.start();
    await b3.start();

    // Wait for all 3 brokers to establish inter-broker connections
    await new Promise((r) => setTimeout(r, 1500));

    const clusterInfo1 = b1.clusterManager.getClusterInfo();
    const aliveCount1 = clusterInfo1.filter(n => n.status === 'alive').length;
    assert(aliveCount1 === 3, 'All 3 brokers in cluster are marked ALIVE');

    // Produce message on broker-1 and verify broker-1 persistent storage directory
    await b1.broker.handleRequest({ type: 'CREATE_TOPIC', payload: { topic: 'cluster-topic', partitions: 1 } });
    await b1.broker.handleRequest({ type: 'PRODUCE', payload: { topic: 'cluster-topic', partition: 0, message: 'Broker 1 Payload' } });

    assert(fs.existsSync(path.join(d1, 'topics', 'cluster-topic')), 'Broker-1 data directory contains "cluster-topic"');
    assert(!fs.existsSync(path.join(d2, 'topics', 'cluster-topic')), 'Broker-2 data directory is isolated and does NOT contain "cluster-topic"');
    assert(!fs.existsSync(path.join(d3, 'topics', 'cluster-topic')), 'Broker-3 data directory is isolated and does NOT contain "cluster-topic"');

    await b1.stop();
    await b2.stop();
    await b3.stop();

    cleanupDataDir(d1);
    cleanupDataDir(d2);
    cleanupDataDir(d3);

    console.log('\n==================================================');
    console.log('ALL MILESTONE 7 CLUSTER TESTS PASSED SUCCESSFULLY! 🎉');
    console.log('==================================================\n');
  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    process.exit(1);
  }
}

runClusterTests();
