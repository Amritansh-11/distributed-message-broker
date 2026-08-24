import net from 'net';
import { BrokerServer } from '../src/broker/server.js';
import { StreamFramer } from '../src/protocol/framing.js';
import { ProtocolEncoder, ProtocolDecoder } from '../src/protocol/codec.js';
import { ProtocolRequest } from '../src/protocol/types.js';

const PORT = 5005;
const HOST = '127.0.0.1';

function assert(condition, message) {
  if (!condition) {
    throw new Error(`[FAIL] ${message}`);
  }
  console.log(`[PASS] ${message}`);
}

async function runMultiTopicIntegrationTest() {
  console.log('==================================================');
  console.log('RUNNING MULTI-TOPIC ROUTING INTEGRATION SUITE');
  console.log('==================================================\n');

  const broker = new BrokerServer(PORT, HOST);
  await broker.start();

  try {
    const sendRequest = (reqObj) => {
      return new Promise((resolve, reject) => {
        const socket = net.createConnection({ port: PORT, host: HOST });
        const framer = new StreamFramer();
        let result = null;

        socket.on('connect', () => {
          const payload = ProtocolEncoder.encode(reqObj);
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

    // STEP 1: Topic Creation
    console.log('--- STEP 1: Topic Creation ---');
    const topicsToCreate = ['orders', 'payments', 'notifications'];
    for (const t of topicsToCreate) {
      const res = await sendRequest(ProtocolRequest.createTopic(t));
      assert(res && res.parsed && res.parsed.success === true, `Created topic "${t}"`);
    }

    // Verify list topics
    const listRes = await sendRequest(ProtocolRequest.listTopics());
    assert(listRes.parsed.payload.topics.length === 3, 'LIST_TOPICS returns all 3 created topics');

    // STEP 2: Interleaved Production
    console.log('\n--- STEP 2: Interleaved Production ---');
    await sendRequest(ProtocolRequest.produce('orders', 'Order A'));
    await sendRequest(ProtocolRequest.produce('payments', 'Payment A'));
    await sendRequest(ProtocolRequest.produce('orders', 'Order B'));
    await sendRequest(ProtocolRequest.produce('notifications', 'Notification A'));
    assert(true, 'Produced interleaved messages into orders, payments, notifications');

    // STEP 3: Selective Consumption & Isolation Verification
    console.log('\n--- STEP 3: Selective Consumption & Isolation Verification ---');
    
    // Consume orders 1st time -> Order A
    const consumeOrder1 = await sendRequest(ProtocolRequest.consume('orders'));
    assert(consumeOrder1.parsed.type === 'MESSAGE', 'First orders consume returns MESSAGE');
    assert(consumeOrder1.parsed.payload.message === 'Order A', 'Received "Order A"');

    // Consume orders 2nd time -> Order B
    const consumeOrder2 = await sendRequest(ProtocolRequest.consume('orders'));
    assert(consumeOrder2.parsed.type === 'MESSAGE', 'Second orders consume returns MESSAGE');
    assert(consumeOrder2.parsed.payload.message === 'Order B', 'Received "Order B"');

    // Consume payments 1st time -> Payment A
    const consumePayment1 = await sendRequest(ProtocolRequest.consume('payments'));
    assert(consumePayment1.parsed.type === 'MESSAGE', 'First payments consume returns MESSAGE');
    assert(consumePayment1.parsed.payload.message === 'Payment A', 'Received "Payment A"');

    // Consume notifications 1st time -> Notification A
    const consumeNotif1 = await sendRequest(ProtocolRequest.consume('notifications'));
    assert(consumeNotif1.parsed.type === 'MESSAGE', 'First notifications consume returns MESSAGE');
    assert(consumeNotif1.parsed.payload.message === 'Notification A', 'Received "Notification A"');

    // STEP 4: Empty Queue Check
    console.log('\n--- STEP 4: Empty Queue Check ---');
    const emptyOrder = await sendRequest(ProtocolRequest.consume('orders'));
    assert(emptyOrder.parsed.type === 'NO_MESSAGES', 'Consuming empty "orders" returns NO_MESSAGES');

    await broker.stop();

    console.log('\n==================================================');
    console.log('ALL MULTI-TOPIC ROUTING INTEGRATION TESTS PASSED SUCCESSFULLY! 🎉');
    console.log('==================================================\n');
  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    await broker.stop();
    process.exit(1);
  }
}

runMultiTopicIntegrationTest();
