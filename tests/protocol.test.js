import net from 'net';
import { Server } from '../src/broker/server.js';
import { ProtocolParser } from '../src/protocol/parser.js';

const PORT = 4223;
const HOST = '127.0.0.1';

// Simple assertion helper
function assert(condition, message) {
  if (!condition) {
    throw new Error(`[FAIL] ${message}`);
  }
  console.log(`[PASS] ${message}`);
}

async function runProtocolTests() {
  console.log('==================================================');
  console.log('RUNNING PROTOCOL PARSER VERIFICATION SUITE');
  console.log('==================================================\n');

  const server = new Server({ port: PORT, host: HOST });
  await server.start();

  try {
    // Step 1: Create Consumer Client and Subscribe to 'orders'
    console.log('--- TEST 1: Consumer Connection & Subscription ---');
    const consumerSocket = net.createConnection({ port: PORT, host: HOST });
    const consumerParser = new ProtocolParser();
    const receivedMessages = [];

    consumerSocket.on('data', (chunk) => {
      consumerParser.feed(chunk);
    });

    consumerParser.on('command', (cmd) => {
      if (cmd.type === 'MSG') {
        console.log(`[Consumer Received] Topic: ${cmd.topic}, ID: ${cmd.messageId}, Payload: "${cmd.payload.toString()}"`);
        receivedMessages.push(cmd);
      }
    });

    await new Promise((resolve) => consumerSocket.on('connect', resolve));
    assert(true, 'Consumer connected successfully');

    consumerSocket.write('SUB orders\r\n');
    await new Promise((resolve) => setTimeout(resolve, 200));
    assert(true, 'Sent SUB orders command');

    // Step 2: Create Producer Client and Publish Messages
    console.log('\n--- TEST 2: Producer Connection & Publish ---');
    const producerSocket = net.createConnection({ port: PORT, host: HOST });
    await new Promise((resolve) => producerSocket.on('connect', resolve));
    assert(true, 'Producer connected successfully');

    const payloadStr = JSON.stringify({ id: 101, item: 'Laptop', price: 1200 });
    const payloadBytes = Buffer.byteLength(payloadStr);

    console.log('[Test] Producer sending message 1...');
    producerSocket.write(`PUB orders ${payloadBytes}\r\n${payloadStr}\r\n`);
    await new Promise((resolve) => setTimeout(resolve, 300));

    // Step 3: Test TCP Packet Chunking (Simulate partial packet transmission)
    console.log('\n--- TEST 3: TCP Packet Chunking ---');
    const msg2Payload = 'Chunked Payload Data Stream';
    const msg2Bytes = Buffer.byteLength(msg2Payload);
    const fullFrame = `PUB orders ${msg2Bytes}\r\n${msg2Payload}\r\n`;

    const chunk1 = fullFrame.slice(0, 10);
    const chunk2 = fullFrame.slice(10, 25);
    const chunk3 = fullFrame.slice(25);

    producerSocket.write(chunk1);
    await new Promise((r) => setTimeout(r, 100));
    producerSocket.write(chunk2);
    await new Promise((r) => setTimeout(r, 100));
    producerSocket.write(chunk3);

    await new Promise((r) => setTimeout(r, 500));

    // Verification
    console.log('\n--- Verification Results ---');
    assert(receivedMessages.length === 2, 'Both messages were successfully parsed and delivered to consumer!');
    assert(receivedMessages[0].payload.toString() === payloadStr, 'First message payload matches expected JSON');
    assert(receivedMessages[1].payload.toString() === msg2Payload, 'Second (chunked) message payload matches expected text');

    consumerSocket.destroy();
    producerSocket.destroy();
    await server.stop();

    console.log('\n==================================================');
    console.log('ALL PROTOCOL PARSER TESTS PASSED SUCCESSFULLY! 🎉');
    console.log('==================================================\n');
  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    await server.stop();
    process.exit(1);
  }
}

runProtocolTests();
