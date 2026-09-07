/**
 * TCP Wire Protocol Native Benchmark Suite
 * 
 * Benchmarks high-throughput TCP socket framing, wire encoding/decoding,
 * partition key hashing, and binary segment persistence over TCP.
 */

import net from 'net';
import { ProtocolEncoder, ProtocolDecoder } from '../src/protocol/codec.js';
import { StreamFramer } from '../src/protocol/framing.js';
import { ProtocolRequest } from '../src/protocol/types.js';

const HOST = process.env.BROKER_HOST || '127.0.0.1';
const PORT = Number(process.env.BROKER_PORT) || 5000;
const TOPIC = 'tcp-benchmark-topic';
const TOTAL_MESSAGES = Number(process.env.MSG_COUNT) || 10000;
const CONCURRENT_CLIENTS = Number(process.env.CONCURRENT_CLIENTS) || 10;

function sendRequest(socket, framer, reqObj) {
  return new Promise((resolve, reject) => {
    const encoded = ProtocolEncoder.encode(reqObj);
    
    const onData = (chunk) => {
      const frames = framer.feed(chunk);
      if (frames.length > 0) {
        socket.removeListener('data', onData);
        socket.removeListener('error', onError);
        if (frames[0].error) {
          return resolve({ error: frames[0].error });
        }
        const decoded = ProtocolDecoder.decode(frames[0].raw);
        resolve(decoded.parsed);
      }
    };

    const onError = (err) => {
      socket.removeListener('data', onData);
      socket.removeListener('error', onError);
      reject(err);
    };

    socket.on('data', onData);
    socket.on('error', onError);
    socket.write(encoded);
  });
}

export async function runTcpBenchmark(opts = {}) {
  const host = opts.host || process.env.BROKER_HOST || '127.0.0.1';
  const port = Number(opts.port) || Number(process.env.BROKER_PORT) || 5555;
  const TOPIC = 'tcp-benchmark-topic';
  const TOTAL_MESSAGES = Number(opts.totalMessages) || Number(process.env.MSG_COUNT) || 10000;
  const CONCURRENT_CLIENTS = Number(opts.concurrentClients) || Number(process.env.CONCURRENT_CLIENTS) || 10;

  console.log('\n==================================================');
  console.log('RUNNING NATIVE TCP WIRE PROTOCOL BENCHMARK SUITE');
  console.log('==================================================');
  console.log(`Target: tcp://${host}:${port}`);
  console.log(`Total Produce Records: ${TOTAL_MESSAGES.toLocaleString()}`);
  console.log(`Concurrent TCP Connections: ${CONCURRENT_CLIENTS}\n`);

  // Step 1: Create Topic
  const setupSocket = net.createConnection({ host: host, port: port });
  const setupFramer = new StreamFramer();
  await new Promise((r) => setupSocket.on('connect', r));
  await sendRequest(setupSocket, setupFramer, ProtocolRequest.createTopic(TOPIC, 5, 1));
  setupSocket.end();

  // Step 2: Produce Benchmark
  const msgsPerClient = Math.floor(TOTAL_MESSAGES / CONCURRENT_CLIENTS);
  const produceStartTime = Date.now();

  const clientPromises = Array.from({ length: CONCURRENT_CLIENTS }, async (_, clientId) => {
    const socket = net.createConnection({ host: host, port: port });
    const framer = new StreamFramer();
    await new Promise((r) => socket.on('connect', r));

    let clientSuccess = 0;
    for (let i = 0; i < msgsPerClient; i++) {
      const msg = `TCP payload client#${clientId} seq#${i}`;
      const key = `key-${i % 20}`;
      const res = await sendRequest(socket, framer, ProtocolRequest.produce(TOPIC, msg, undefined, key));
      if (res && res.success) clientSuccess++;
    }

    socket.end();
    return clientSuccess;
  });

  const produceResults = await Promise.all(clientPromises);
  const totalProduced = produceResults.reduce((a, b) => a + b, 0);
  const produceDurationSec = (Date.now() - produceStartTime) / 1000;
  const produceThroughput = Math.round(totalProduced / produceDurationSec);

  console.log(`[TCP PRODUCE RESULTS]`);
  console.log(`- Total Records Produced: ${totalProduced.toLocaleString()}`);
  console.log(`- Time Elapsed: ${produceDurationSec.toFixed(3)}s`);
  console.log(`- Throughput: ${produceThroughput.toLocaleString()} msgs/sec`);
  console.log(`- Avg Latency per Msg: ${(produceDurationSec * 1000 / totalProduced * CONCURRENT_CLIENTS).toFixed(3)}ms`);

  // Step 3: Consume Benchmark
  const consumeStartTime = Date.now();
  const consumeSocket = net.createConnection({ host: host, port: port });
  const consumeFramer = new StreamFramer();
  await new Promise((r) => consumeSocket.on('connect', r));

  let totalConsumed = 0;
  const consumeCount = Math.min(TOTAL_MESSAGES, 2000);
  for (let i = 0; i < consumeCount; i++) {
    const res = await sendRequest(consumeSocket, consumeFramer, ProtocolRequest.consume(TOPIC, i % 5, Math.floor(i / 5)));
    if (res && res.success && res.type === 'MESSAGE') totalConsumed++;
  }
  consumeSocket.end();

  const consumeDurationSec = (Date.now() - consumeStartTime) / 1000;
  const consumeThroughput = Math.round(totalConsumed / consumeDurationSec);

  console.log(`\n[TCP CONSUME RESULTS]`);
  console.log(`- Total Records Consumed: ${totalConsumed.toLocaleString()}`);
  console.log(`- Time Elapsed: ${consumeDurationSec.toFixed(3)}s`);
  console.log(`- Throughput: ${consumeThroughput.toLocaleString()} msgs/sec`);
  console.log(`- Avg Latency per Msg: ${(consumeDurationSec * 1000 / totalConsumed).toFixed(3)}ms`);

  console.log('==================================================\n');
  return {
    produce: { total: totalProduced, durationSec: produceDurationSec, throughput: produceThroughput },
    consume: { total: totalConsumed, durationSec: consumeDurationSec, throughput: consumeThroughput }
  };
}

if (process.argv[1] && process.argv[1].endsWith('tcp-benchmark.js')) {
  runTcpBenchmark().catch(console.error);
}
