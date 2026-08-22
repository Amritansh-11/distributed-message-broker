import net from 'net';
import { BrokerServer } from '../src/broker/server.js';
import { StreamFramer } from '../src/protocol/framing.js';
import { ProtocolEncoder, ProtocolDecoder } from '../src/protocol/codec.js';

function assert(condition, message) {
  if (!condition) {
    throw new Error(`[FAIL] ${message}`);
  }
  console.log(`[PASS] ${message}`);
}

async function runTests() {
  console.log('==================================================');
  console.log('RUNNING MILESTONE 2 BROKER PIPELINE VERIFICATION SUITE');
  console.log('==================================================\n');

  const testPort = 5002;
  const broker = new BrokerServer(testPort);

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
    const pingRes = await sendCommand({ type: 'PING' });
    assert(pingRes && pingRes.parsed && pingRes.parsed.type === 'PONG', 'Broker responds to PING with PONG');
    assert(pingRes.parsed.success === true, 'PONG indicates success: true');

    // TEST 3: Produce Message
    console.log('\n--- TEST 3: Produce Message ---');
    const produceRes = await sendCommand({ type: 'PRODUCE', message: 'Hello Distributed Systems' });
    assert(produceRes && produceRes.parsed && produceRes.parsed.type === 'PRODUCE_ACK', 'Broker responds with PRODUCE_ACK');
    assert(produceRes.parsed.success === true, 'PRODUCE_ACK indicates success');
    assert(broker.messages.length === 1, 'Broker stores message in in-memory queue');

    // TEST 4: Consume Message (Message exists)
    console.log('\n--- TEST 4: Consume Message ---');
    const consumeRes1 = await sendCommand({ type: 'CONSUME' });
    assert(consumeRes1 && consumeRes1.parsed && consumeRes1.parsed.type === 'MESSAGE', 'Broker responds with MESSAGE');
    assert(consumeRes1.parsed.message === 'Hello Distributed Systems', 'Consumer receives "Hello Distributed Systems"');

    // TEST 5: Consume Message (No message exists)
    console.log('\n--- TEST 5: Consume when Queue is Empty ---');
    const consumeRes2 = await sendCommand({ type: 'CONSUME' });
    assert(consumeRes2 && consumeRes2.parsed && consumeRes2.parsed.type === 'NO_MESSAGES', 'Broker responds with NO_MESSAGES');
    assert(consumeRes2.parsed.success === true, 'NO_MESSAGES indicates success');

    // TEST 6: Malformed JSON Handling
    console.log('\n--- TEST 6: Malformed JSON Handling ---');
    const malformedRes = await sendCommand('{ invalid json string\n');
    assert(malformedRes && malformedRes.parsed && malformedRes.parsed.type === 'ERROR', 'Broker catches malformed JSON and returns ERROR');
    assert(malformedRes.parsed.success === false, 'ERROR response success is false');
    assert(broker.server.listening, 'Broker remains running after handling malformed JSON');

    // TEST 7: Validation Error Handling
    console.log('\n--- TEST 7: Request Validation Error Handling ---');
    const validationErrRes = await sendCommand({ type: 'PRODUCE' }); // Missing message
    assert(validationErrRes && validationErrRes.parsed && validationErrRes.parsed.type === 'ERROR', 'Broker catches missing message and returns ERROR response');
    assert(validationErrRes.parsed.error.includes('must include a string "message"'), 'Error details explain validation failure');

    // Stop Broker for offline test
    await broker.stop();

    // TEST 8: Connection Failure Handling
    console.log('\n--- TEST 8: Connection Failure Handling ---');
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
    console.log('ALL MILESTONE 2 BROKER PIPELINE TESTS PASSED SUCCESSFULLY! 🎉');
    console.log('==================================================\n');
  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    await broker.stop();
    process.exit(1);
  }
}

runTests();
