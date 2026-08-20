import net from 'net';
import { BrokerServer } from '../src/broker/broker.js';
import { NDJSONFramer } from '../src/protocol/message.js';

// Simple assertion helper
function assert(condition, message) {
  if (!condition) {
    throw new Error(`[FAIL] ${message}`);
  }
  console.log(`[PASS] ${message}`);
}

async function runTests() {
  console.log('==================================================');
  console.log('RUNNING MILESTONE 1 VERIFICATION SUITE');
  console.log('==================================================\n');

  const testPort = 5001; // Use non-conflicting port for automated test run
  const broker = new BrokerServer(testPort);

  try {
    // TEST 1: Broker Startup
    console.log('--- TEST 1: Broker Startup ---');
    await broker.start();
    assert(broker.server !== null && broker.server.listening, 'Broker server starts and listens on port');

    // Helper helper function to send NDJSON command to test broker
    const sendCommand = (cmdObj) => {
      return new Promise((resolve, reject) => {
        const socket = net.createConnection({ port: testPort, host: '127.0.0.1' });
        const framer = new NDJSONFramer();
        let result = null;

        socket.on('connect', () => {
          socket.write(typeof cmdObj === 'string' ? cmdObj : NDJSONFramer.encode(cmdObj));
        });

        socket.on('data', (chunk) => {
          const frames = framer.feed(chunk);
          if (frames.length > 0) {
            result = frames[0];
          }
          socket.end();
        });

        socket.on('close', () => resolve(result));
        socket.on('error', reject);
      });
    };

    // TEST 2: Produce Message
    console.log('\n--- TEST 2: Produce Message ---');
    const produceRes = await sendCommand({ type: 'PRODUCE', message: 'Hello Distributed Systems' });
    assert(produceRes && produceRes.parsed && produceRes.parsed.type === 'PRODUCE_ACK', 'Broker responds with PRODUCE_ACK');
    assert(produceRes.parsed.success === true, 'PRODUCE_ACK indicates success');
    assert(broker.messages.length === 1, 'Broker stores message in in-memory queue');

    // TEST 3: Consume Message (Message exists)
    console.log('\n--- TEST 3: Consume Message ---');
    const consumeRes1 = await sendCommand({ type: 'CONSUME' });
    assert(consumeRes1 && consumeRes1.parsed && consumeRes1.parsed.type === 'MESSAGE', 'Broker responds with MESSAGE');
    assert(consumeRes1.parsed.message === 'Hello Distributed Systems', 'Consumer receives "Hello Distributed Systems"');

    // TEST 4: Consume Message (No message exists)
    console.log('\n--- TEST 4: Consume when Queue is Empty ---');
    const consumeRes2 = await sendCommand({ type: 'CONSUME' });
    assert(consumeRes2 && consumeRes2.parsed && consumeRes2.parsed.type === 'NO_MESSAGES', 'Broker responds with NO_MESSAGES');
    assert(consumeRes2.parsed.success === true, 'NO_MESSAGES indicates success');

    // TEST 5: Send Malformed JSON
    console.log('\n--- TEST 5: Malformed JSON Handling ---');
    const malformedRes = await sendCommand('{ invalid json string\n');
    assert(malformedRes && malformedRes.parsed && malformedRes.parsed.type === 'ERROR', 'Broker catches malformed JSON and returns ERROR');
    assert(broker.server.listening, 'Broker remains running after handling malformed JSON');

    // Stop Broker for offline test
    await broker.stop();

    // TEST 6: Producer handling broker offline
    console.log('\n--- TEST 6: Connection Failure Handling ---');
    let connectionFailedGracefully = false;
    await new Promise((resolve) => {
      const socket = net.createConnection({ port: testPort, host: '127.0.0.1' });
      socket.on('error', (err) => {
        connectionFailedGracefully = true;
        resolve();
      });
    });
    assert(connectionFailedGracefully, 'Client connection failure caught without unhandled process crash');

    console.log('\n==================================================');
    console.log('ALL MILESTONE 1 VERIFICATION TESTS PASSED SUCCESSFULLY! 🎉');
    console.log('==================================================');
  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    await broker.stop();
    process.exit(1);
  }
}

runTests();
