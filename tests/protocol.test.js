import net from 'net';
import fs from 'fs';
import path from 'path';
import { BrokerServer } from '../src/broker/server.js';
import { StreamFramer } from '../src/protocol/framing.js';
import { ProtocolEncoder, ProtocolDecoder } from '../src/protocol/codec.js';

const PORT = 4223;
const HOST = '127.0.0.1';
const TEST_DATA_DIR = path.join(process.cwd(), 'scratch', 'test-protocol-4223');

function cleanupDataDir(dir) {
  if (fs.existsSync(dir)) {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

// Simple assertion helper
function assert(condition, message) {
  if (!condition) {
    throw new Error(`[FAIL] ${message}`);
  }
  console.log(`[PASS] ${message}`);
}

async function runProtocolTests() {
  console.log('==================================================');
  console.log('RUNNING PROTOCOL PIPELINE & CHUNKING VERIFICATION SUITE');
  console.log('==================================================\n');

  cleanupDataDir(TEST_DATA_DIR);
  const server = new BrokerServer({ port: PORT, host: HOST, dataDir: TEST_DATA_DIR });
  await server.start();

  try {
    // TEST 1: Connection & PING
    console.log('--- TEST 1: Connection & PING Request ---');
    const clientSocket = net.createConnection({ port: PORT, host: HOST });
    const framer = new StreamFramer();
    const responses = [];

    clientSocket.on('data', (chunk) => {
      const frames = framer.feed(chunk);
      for (const frame of frames) {
        if (!frame.error) {
          const decoded = ProtocolDecoder.decode(frame.raw);
          if (!decoded.error) {
            responses.push(decoded.parsed);
          }
        }
      }
    });

    await new Promise((resolve) => clientSocket.on('connect', resolve));
    assert(true, 'Client connected successfully to Broker Server');

    const pingReq = ProtocolEncoder.encode({ type: 'PING' });
    clientSocket.write(pingReq);
    await new Promise((resolve) => setTimeout(resolve, 200));

    assert(responses.length === 1, 'Received response from broker for PING');
    assert(responses[0].type === 'PONG', 'Received PONG response');

    // TEST 2: Create Topic
    console.log('\n--- TEST 2: Create Topic ---');
    const createTopicReq = ProtocolEncoder.encode({ type: 'CREATE_TOPIC', payload: { topic: 'orders' } });
    clientSocket.write(createTopicReq);
    await new Promise((resolve) => setTimeout(resolve, 200));

    assert(responses.length === 2, 'Received response from broker for CREATE_TOPIC');
    assert(responses[1].type === 'CREATE_TOPIC_ACK', 'Received CREATE_TOPIC_ACK response');

    // TEST 3: Producer PRODUCE message via pipeline
    console.log('\n--- TEST 3: Produce Message Over Protocol Pipeline ---');
    const produceReq = ProtocolEncoder.encode({ type: 'PRODUCE', payload: { topic: 'orders', message: 'Chunked Protocol Test Payload' } });
    clientSocket.write(produceReq);
    await new Promise((resolve) => setTimeout(resolve, 200));

    assert(responses.length === 3, 'Received response from broker for PRODUCE');
    assert(responses[2].type === 'PRODUCE_ACK', 'Received PRODUCE_ACK response');

    // TEST 4: TCP Packet Chunking (Simulate partial packet transmission over socket)
    console.log('\n--- TEST 4: TCP Packet Chunking & Buffer Reconstruction ---');
    const consumeReqStr = ProtocolEncoder.encode({ type: 'CONSUME', payload: { topic: 'orders' } });
    
    // Split request frame string into 3 separate arbitrary TCP byte chunks
    const chunk1 = consumeReqStr.slice(0, 5);
    const chunk2 = consumeReqStr.slice(5, 12);
    const chunk3 = consumeReqStr.slice(12);

    clientSocket.write(chunk1);
    await new Promise((r) => setTimeout(r, 100));
    clientSocket.write(chunk2);
    await new Promise((r) => setTimeout(r, 100));
    clientSocket.write(chunk3);

    await new Promise((r) => setTimeout(r, 300));

    // Verification
    console.log('\n--- Verification Results ---');
    assert(responses.length === 4, 'All 4 pipeline requests successfully received responses!');
    assert(responses[3].type === 'MESSAGE', 'Fourth response is MESSAGE response');
    assert(responses[3].payload.message === 'Chunked Protocol Test Payload', 'Consumed message payload matches chunked PRODUCE');

    clientSocket.destroy();
    await server.stop();
    cleanupDataDir(TEST_DATA_DIR);

    console.log('\n==================================================');
    console.log('ALL PROTOCOL PIPELINE & CHUNKING TESTS PASSED SUCCESSFULLY! 🎉');
    console.log('==================================================\n');
  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    await server.stop();
    cleanupDataDir(TEST_DATA_DIR);
    process.exit(1);
  }
}

runProtocolTests();
