/**
 * Master Benchmark Automation Runner
 * 
 * Manages broker lifecycle, runs k6 HTTP load tests, runs TCP wire benchmarks,
 * collects metrics from ManagementServer, and outputs structured performance report.
 */

import path from 'path';
import fs from 'fs';
import { spawn } from 'child_process';
import { BrokerServer } from '../src/broker/server.js';
import { runTcpBenchmark } from './tcp-benchmark.js';

const scratchDir = path.join(process.cwd(), 'scratch', `benchmark-data-${Date.now()}`);

function cleanupDir(dir) {
  if (fs.existsSync(dir)) {
    try {
      fs.rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 });
    } catch (err) {
      // Ignore transient Windows file handle locks on shutdown
    }
  }
}

function findK6Executable() {
  const localK6Direct = path.join(process.cwd(), 'scratch', 'bin', 'k6.exe');
  if (fs.existsSync(localK6Direct)) {
    return localK6Direct;
  }
  const localK6Nested = path.join(process.cwd(), 'scratch', 'bin', 'k6-v0.54.0-windows-amd64', 'k6.exe');
  if (fs.existsSync(localK6Nested)) {
    return localK6Nested;
  }
  return 'k6';
}

function runK6Script(k6Path, scriptPath) {
  return new Promise((resolve) => {
    console.log(`\n--------------------------------------------------`);
    console.log(`Executing k6 Load Test: ${path.basename(scriptPath)}`);
    console.log(`--------------------------------------------------\n`);

    const k6Proc = spawn(k6Path, ['run', scriptPath], {
      stdio: 'inherit',
      env: { ...process.env, TARGET_URL: 'http://127.0.0.1:8555', BROKER_PORT: '5555' }
    });

    k6Proc.on('close', (code) => {
      console.log(`\n[k6] Benchmark script ${path.basename(scriptPath)} finished with exit code ${code}`);
      resolve(code);
    });

    k6Proc.on('error', (err) => {
      console.error(`[k6 Error] ${err.message}`);
      resolve(1);
    });
  });
}

async function main() {
  console.log('==================================================');
  console.log('DISTRIBUTED MESSAGE BROKER — FULL BENCHMARK SUITE');
  console.log('==================================================\n');

  cleanupDir(scratchDir);
  const brokerServer = new BrokerServer({
    port: 5555,
    httpPort: 8555,
    brokerId: 'broker-1',
    dataDir: scratchDir,
    logLevel: 'WARN'
  });

  try {
    console.log('[1/4] Starting Broker Server (TCP: 5555, HTTP: 8555)...');
    await brokerServer.start();
    await new Promise(r => setTimeout(r, 1000));

    console.log('\n[2/4] Locating k6 Executable...');
    const k6Binary = findK6Executable();
    console.log(`Using k6 binary: ${k6Binary}`);

    console.log('\n[3/4] Running k6 Multi-Scenario Load Test Suite...');
    const k6Script = path.join(process.cwd(), 'benchmarks', 'k6', 'full-suite.js');
    const k6ResultCode = await runK6Script(k6Binary, k6Script);

    console.log('\n[4/4] Running Native TCP Wire Protocol Benchmarks...');
    const tcpResults = await runTcpBenchmark({ port: 5555 });

    // Fetch metrics from HTTP Management Server
    console.log('\n==================================================');
    console.log('FINAL BENCHMARK SUMMARY & METRICS');
    console.log('==================================================');
    
    try {
      const resp = await fetch('http://127.0.0.1:8555/metrics');
      const metricsText = await resp.text();
      console.log('\n--- Broker Operational Metrics (/metrics) ---');
      console.log(metricsText.trim());
    } catch (err) {
      console.warn('Could not fetch /metrics endpoint:', err.message);
    }

    console.log('\n--- TCP Wire Performance ---');
    console.log(`Produce Throughput: ${tcpResults.produce.throughput.toLocaleString()} msgs/sec`);
    console.log(`Consume Throughput: ${tcpResults.consume.throughput.toLocaleString()} msgs/sec`);
    console.log('==================================================\n');

    await brokerServer.stop();
    await new Promise(r => setTimeout(r, 300));
    cleanupDir(scratchDir);
    process.exit(k6ResultCode === 0 ? 0 : 1);

  } catch (err) {
    console.error('[BENCHMARK ERROR]', err);
    await brokerServer.stop();
    await new Promise(r => setTimeout(r, 300));
    cleanupDir(scratchDir);
    process.exit(1);
  }
}

main();
