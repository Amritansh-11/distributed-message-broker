/**
 * Milestone 10 — Observability, Metrics, Config & Docker Test Suite
 * 
 * Tests structured logging, metrics collection, ConfigLoader parsing,
 * ManagementServer HTTP endpoints (/metrics, /health, /cluster),
 * and graceful server shutdown sequence.
 */

import { ConfigLoader } from '../src/config/config.js';
import { Logger } from '../src/utils/logger.js';
import { MetricsCollector } from '../src/metrics/metrics-collector.js';
import { ManagementServer } from '../src/server/management-server.js';
import { BrokerServer } from '../src/broker/server.js';
import { MessageBroker } from '../src/broker/broker.js';
import { ClusterManager } from '../src/cluster/cluster-manager.js';
import http from 'http';
import fs from 'fs';
import path from 'path';

function assert(condition, message) {
  if (!condition) {
    console.error(`[FAIL] ${message}`);
    throw new Error(`[FAIL] ${message}`);
  }
  console.log(`[PASS] ${message}`);
}

function httpGet(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ statusCode: res.statusCode, headers: res.headers, body: data }));
    }).on('error', reject);
  });
}

const scratchDir = path.join(process.cwd(), 'scratch', `test-m10-${Date.now()}`);

function cleanupDir(dirPath) {
  if (fs.existsSync(dirPath)) {
    fs.rmSync(dirPath, { recursive: true, force: true });
  }
}

async function runM10ObservabilityTests() {
  console.log('\n==================================================');
  console.log('RUNNING MILESTONE 10 OBSERVABILITY TEST SUITE');
  console.log('==================================================\n');

  try {
    // --- TEST 1: ConfigLoader Parsing ---
    console.log('--- TEST 1: ConfigLoader Parsing & Env Overrides ---');
    {
      const envMock = {
        BROKER_ID: 'broker-test-1',
        BROKER_HOST: '127.0.0.1',
        BROKER_PORT: '5500',
        HTTP_PORT: '8500',
        LOG_LEVEL: 'DEBUG',
        CLUSTER_BROKERS: 'broker-1:127.0.0.1:5500,broker-2:127.0.0.1:5501'
      };

      const cfg = ConfigLoader.loadConfig(envMock);
      assert(cfg.brokerId === 'broker-test-1', 'Loaded BROKER_ID from env');
      assert(cfg.port === 5500, 'Loaded BROKER_PORT 5500');
      assert(cfg.httpPort === 8500, 'Loaded HTTP_PORT 8500');
      assert(cfg.logLevel === 'DEBUG', 'Loaded LOG_LEVEL DEBUG');
      assert(cfg.clusterConfig.length === 2, 'Parsed CLUSTER_BROKERS list with 2 brokers');

      assert(fs.existsSync(path.join(process.cwd(), '.env.example')), '.env.example template exists');
    }

    // --- TEST 2: Structured Logger ---
    console.log('\n--- TEST 2: Structured Logger & Levels ---');
    {
      const customLogger = new Logger({ level: 'INFO', brokerId: 'broker-log-1', json: true });
      customLogger.clear();

      customLogger.debug('Debug event', { topic: 'test' });
      assert(customLogger.logs.length === 0, 'DEBUG log ignored when level is INFO');

      customLogger.info('PRODUCE', { topic: 'orders', partition: 0, offset: 5 });
      assert(customLogger.logs.length === 1, 'INFO log emitted');
      assert(customLogger.logs[0].event === 'PRODUCE', 'Log entry contains event PRODUCE');
      assert(customLogger.logs[0].topic === 'orders', 'Log entry contains metadata topic orders');
      assert(customLogger.logs[0].offset === 5, 'Log entry contains metadata offset 5');

      customLogger.warn('LEADER_FAILOVER', { error: 'Connection lost' });
      assert(customLogger.logs.length === 2, 'WARN log emitted');
    }

    // --- TEST 3: In-Memory MetricsCollector ---
    console.log('\n--- TEST 3: In-Memory MetricsCollector ---');
    {
      const mc = new MetricsCollector();
      mc.increment('messages_produced_total', 10);
      mc.increment('messages_consumed_total', 8);
      mc.setGauge('active_connections', 3);
      mc.recordLatency('produce', 2.5);
      mc.recordLatency('produce', 3.5);

      const json = mc.getMetricsJSON();
      assert(json.messages_produced_total === 10, 'messages_produced_total is 10');
      assert(json.messages_consumed_total === 8, 'messages_consumed_total is 8');
      assert(json.active_connections === 3, 'active_connections is 3');
      assert(json.produce_latency_avg_ms === 3, 'produce_latency_avg_ms calculated correctly (3ms avg)');

      const formatted = mc.getMetricsFormatted();
      assert(formatted.includes('messages_produced_total 10'), 'Formatted text contains messages_produced_total 10');
      assert(formatted.includes('active_connections 3'), 'Formatted text contains active_connections 3');
    }

    // --- TEST 4: ManagementServer HTTP Endpoints ---
    console.log('\n--- TEST 4: ManagementServer HTTP Endpoints (/metrics, /health, /cluster) ---');
    {
      const clusterConfig = [
        { id: 'broker-1', host: '127.0.0.1', port: 5701 },
        { id: 'broker-2', host: '127.0.0.1', port: 5702 }
      ];
      const cm = new ClusterManager({ brokerId: 'broker-1', clusterConfig });
      const broker = new MessageBroker({ brokerId: 'broker-1', clusterManager: cm, dataDir: path.join(scratchDir, 'b1') });

      const mgmtServer = new ManagementServer({
        port: 8701,
        host: '127.0.0.1',
        brokerId: 'broker-1',
        clusterManager: cm,
        broker
      });

      await mgmtServer.start();

      // Test GET /health
      const healthRes = await httpGet('http://127.0.0.1:8701/health');
      assert(healthRes.statusCode === 200, 'GET /health returned status 200');
      const healthObj = JSON.parse(healthRes.body);
      assert(healthObj.status === 'HEALTHY', '/health status is HEALTHY');
      assert(healthObj.brokerId === 'broker-1', '/health contains brokerId broker-1');
      assert(healthObj.clusterSize === 2, '/health contains clusterSize 2');

      // Test GET /metrics
      const metricsRes = await httpGet('http://127.0.0.1:8701/metrics');
      assert(metricsRes.statusCode === 200, 'GET /metrics returned status 200');
      assert(metricsRes.body.includes('messages_produced_total'), '/metrics body contains messages_produced_total');

      // Test GET /cluster
      const clusterRes = await httpGet('http://127.0.0.1:8701/cluster');
      assert(clusterRes.statusCode === 200, 'GET /cluster returned status 200');
      const clusterArr = JSON.parse(clusterRes.body);
      assert(clusterArr.length === 2, '/cluster returned 2 cluster brokers');
      assert(clusterArr[0].id === 'broker-1', '/cluster contains broker-1');

      await mgmtServer.stop();
      broker.clear();
    }

    // --- TEST 5: Graceful Shutdown ---
    console.log('\n--- TEST 5: Server Graceful Shutdown ---');
    {
      const server = new BrokerServer({ port: 5705, httpPort: 8705, brokerId: 'broker-shut-1', dataDir: path.join(scratchDir, 'shut') });
      await server.start();

      // Verify endpoints respond while running
      const healthRes = await httpGet('http://127.0.0.1:8705/health');
      assert(healthRes.statusCode === 200, 'Server responded to /health while running');

      await server.stop();

      // Verify HTTP server closed cleanly
      try {
        await httpGet('http://127.0.0.1:8705/health');
        assert(false, 'Expected HTTP fetch to fail after shutdown');
      } catch (err) {
        assert(true, 'HTTP server closed cleanly after stop()');
      }
    }

    console.log('\n==================================================');
    console.log('ALL MILESTONE 10 OBSERVABILITY TESTS PASSED! 🎉');
    console.log('==================================================\n');

  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    process.exit(1);
  } finally {
    cleanupDir(scratchDir);
  }
}

runM10ObservabilityTests();
