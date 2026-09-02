import net from 'net';
import fs from 'fs';
import path from 'path';
import { BrokerServer } from '../src/broker/server.js';
import { StreamFramer } from '../src/protocol/framing.js';
import { ProtocolEncoder, ProtocolDecoder } from '../src/protocol/codec.js';

const TEST_DATA_DIR = path.join(process.cwd(), 'scratch', 'test-broker-5002');

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

async function runTests() {
  console.log('==================================================');
  console.log('RUNNING MILESTONE 3 BROKER PIPELINE VERIFICATION SUITE');
  console.log('==================================================\n');

  cleanupDataDir(TEST_DATA_DIR);
  const testPort = 5002;
  const broker = new BrokerServer({ port: testPort, host: '127.0.0.1', dataDir: TEST_DATA_DIR });

  try {
    // TEST 1: Broker Startup
    console.log('--- TEST 1: Broker Startup ---');
    await broker.start();
    assert(broker.server !== null && broker.server.listening, 'Broker server starts and listens on port 5002');

    // Helper function to send raw or encoded frame to broker and await parsed response
    const sendCommand = (cmdObj) => {
      return new Promise((resolve, reject) => {
        const socket = net.createConnection({ port: testPort, host: '127.0.0.1' });
        const framer = new StreamFramer();
        let result = null;

        socket.on('connect', () => {
          const payload = typeof cmdObj === 'string' ? cmdObj : ProtocolEncoder.encode(cmdObj);
          socket.write(payload);
        });

        socket.on('data', (chunk) => {
          const frames = framer.feed(chunk);
          if (frames.length > 0) {
            if (frames[0].error) {
              result = { error: frames[0].error };
            } else {
              result = ProtocolDecoder.decode(frames[0].raw);
            }
          }
          socket.end();
        });

        socket.on('close', () => resolve(result));
        socket.on('error', reject);
      });
    };

    // TEST 2: PING / PONG
    console.log('\n--- TEST 2: PING / PONG ---');
    const pingRes = await sendCommand({ requestId: 'req-ping', type: 'PING' });
    assert(pingRes && pingRes.parsed && pingRes.parsed.type === 'PONG', 'Broker responds to PING with PONG');
    assert(pingRes.parsed.success === true, 'PONG indicates success: true');
    assert(pingRes.parsed.requestId === 'req-ping', 'PONG preserves requestId');

    // TEST 3: Create Topic
    console.log('\n--- TEST 3: Create Topic ---');
    const createRes = await sendCommand({ requestId: 'req-1', type: 'CREATE_TOPIC', payload: { topic: 'orders' } });
    assert(createRes && createRes.parsed && createRes.parsed.type === 'CREATE_TOPIC_ACK', 'Broker responds with CREATE_TOPIC_ACK');
    assert(createRes.parsed.success === true, 'CREATE_TOPIC_ACK indicates success');
    assert(createRes.parsed.payload.topic === 'orders', 'Ack payload contains topic name');

    // TEST 4: Produce to Unknown Topic (Fails)
    console.log('\n--- TEST 4: Produce to Unknown Topic ---');
    const produceUnknownRes = await sendCommand({ requestId: 'req-2', type: 'PRODUCE', payload: { topic: 'unknown', message: 'Test' } });
    assert(produceUnknownRes && produceUnknownRes.parsed && produceUnknownRes.parsed.type === 'ERROR', 'Produce to unknown topic returns ERROR');
    assert(produceUnknownRes.parsed.error.code === 'TOPIC_NOT_FOUND', 'Error code is TOPIC_NOT_FOUND');

    // TEST 5: Produce Message to Existing Topic
    console.log('\n--- TEST 5: Produce Message to Existing Topic ---');
    const produceRes = await sendCommand({ requestId: 'req-3', type: 'PRODUCE', payload: { topic: 'orders', message: 'Hello Distributed Systems' } });
    assert(produceRes && produceRes.parsed && produceRes.parsed.type === 'PRODUCE_ACK', 'Broker responds with PRODUCE_ACK');
    assert(produceRes.parsed.success === true, 'PRODUCE_ACK indicates success');
    assert(broker.topicManager.getTopicInfo('orders').messageCount === 1, 'Broker stores message in topic queue');

    // TEST 6: Consume Message (Message exists)
    console.log('\n--- TEST 6: Consume Message from Topic ---');
    const consumeRes1 = await sendCommand({ requestId: 'req-4', type: 'CONSUME', payload: { topic: 'orders' } });
    assert(consumeRes1 && consumeRes1.parsed && consumeRes1.parsed.type === 'MESSAGE', 'Broker responds with MESSAGE');
    assert(consumeRes1.parsed.payload.message === 'Hello Distributed Systems', 'Consumer receives "Hello Distributed Systems"');

    // TEST 7: Consume Message (No message exists)
    console.log('\n--- TEST 7: Consume when Queue is Empty ---');
    const consumeRes2 = await sendCommand({ requestId: 'req-5', type: 'CONSUME', payload: { topic: 'orders' } });
    assert(consumeRes2 && consumeRes2.parsed && consumeRes2.parsed.type === 'NO_MESSAGES', 'Broker responds with NO_MESSAGES');
    assert(consumeRes2.parsed.success === true, 'NO_MESSAGES indicates success');

    // TEST 8: List Topics
    console.log('\n--- TEST 8: List Topics ---');
    await sendCommand({ requestId: 'req-6', type: 'CREATE_TOPIC', payload: { topic: 'payments' } });
    const listRes = await sendCommand({ requestId: 'req-7', type: 'LIST_TOPICS', payload: {} });
    assert(listRes && listRes.parsed && listRes.parsed.type === 'TOPICS', 'Broker responds with TOPICS');
    assert(listRes.parsed.payload.topics.length === 2, 'Returns 2 created topics');

    // TEST 9: Get Topic Info
    console.log('\n--- TEST 9: Get Topic Info ---');
    const infoRes = await sendCommand({ requestId: 'req-8', type: 'GET_TOPIC_INFO', payload: { topic: 'orders' } });
    assert(infoRes && infoRes.parsed && infoRes.parsed.type === 'TOPIC_INFO', 'Broker responds with TOPIC_INFO');
    assert(infoRes.parsed.payload.messageCount === 1, 'Topic message count is 1 (messages remain stored in log)');

    // TEST 10: Malformed JSON Handling
    console.log('\n--- TEST 10: Malformed JSON Handling ---');
    const malformedRes = await sendCommand('{ invalid json string\n');
    assert(malformedRes && malformedRes.parsed && malformedRes.parsed.type === 'ERROR', 'Broker catches malformed JSON and returns ERROR');
    assert(malformedRes.parsed.success === false, 'ERROR response success is false');
    assert(broker.server.listening, 'Broker remains running after handling malformed JSON');

    // TEST 11: Validation Error Handling
    console.log('\n--- TEST 11: Request Validation Error Handling ---');
    const validationErrRes = await sendCommand({ type: 'PRODUCE', payload: { topic: 'orders' } }); // Missing message
    assert(validationErrRes && validationErrRes.parsed && validationErrRes.parsed.type === 'ERROR', 'Broker catches missing message and returns ERROR response');
    const errText = typeof validationErrRes.parsed.error === 'string' ? validationErrRes.parsed.error : validationErrRes.parsed.error.message;
    assert(errText.includes('must include a string "message"'), 'Error details explain validation failure');

    // Stop Broker for offline test
    await broker.stop();
    cleanupDataDir(TEST_DATA_DIR);

    // TEST 12: Connection Failure Handling
    console.log('\n--- TEST 12: Connection Failure Handling ---');
    let connectionFailedGracefully = false;
    await new Promise((resolve) => {
      const socket = net.createConnection({ port: testPort, host: '127.0.0.1' });
      socket.on('error', () => {
        connectionFailedGracefully = true;
        resolve();
      });
    });
    assert(connectionFailedGracefully, 'Client connection failure caught without unhandled process crash');

    console.log('\n==================================================');
    console.log('ALL MILESTONE 3 BROKER PIPELINE TESTS PASSED SUCCESSFULLY! 🎉');
    console.log('==================================================\n');
  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    await broker.stop();
    cleanupDataDir(TEST_DATA_DIR);
    process.exit(1);
  }
}

runTests();
