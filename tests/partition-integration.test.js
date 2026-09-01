import net from 'net';
import { BrokerServer } from '../src/broker/server.js';
import { StreamFramer } from '../src/protocol/framing.js';
import { ProtocolEncoder, ProtocolDecoder } from '../src/protocol/codec.js';
import { ProtocolRequest } from '../src/protocol/types.js';

const PORT = 5006;
const HOST = '127.0.0.1';

function assert(condition, message) {
  if (!condition) {
    throw new Error(`[FAIL] ${message}`);
  }
  console.log(`[PASS] ${message}`);
}

async function runPartitionIntegrationTest() {
  console.log('==================================================');
  console.log('RUNNING MILESTONE 4 PARTITION ROUTING INTEGRATION SUITE');
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

    // STEP 1: Topic Creation with 3 partitions
    console.log('--- STEP 1: Topic Creation ---');
    const createRes = await sendRequest(ProtocolRequest.createTopic('orders', 3));
    assert(createRes && createRes.parsed && createRes.parsed.success === true, 'Created topic "orders" with 3 partitions');
    assert(createRes.parsed.payload.partitions === 3, 'Payload returns partitions: 3');

    // STEP 2: Produce to explicit partitions
    console.log('\n--- STEP 2: Explicit Partition Production ---');
    // Order A -> partition 0
    const prodA = await sendRequest(ProtocolRequest.produce('orders', 'Order A', 0));
    assert(prodA.parsed.success === true && prodA.parsed.payload.partition === 0, 'Produced Order A to partition 0');

    // Order B -> partition 1
    const prodB = await sendRequest(ProtocolRequest.produce('orders', 'Order B', 1));
    assert(prodB.parsed.success === true && prodB.parsed.payload.partition === 1, 'Produced Order B to partition 1');

    // Order C -> partition 2
    const prodC = await sendRequest(ProtocolRequest.produce('orders', 'Order C', 2));
    assert(prodC.parsed.success === true && prodC.parsed.payload.partition === 2, 'Produced Order C to partition 2');

    // Order D -> partition 0
    const prodD = await sendRequest(ProtocolRequest.produce('orders', 'Order D', 0));
    assert(prodD.parsed.success === true && prodD.parsed.payload.partition === 0, 'Produced Order D to partition 0');

    // STEP 3: Consume in specific partition sequence
    console.log('\n--- STEP 3: Explicit Partition Consumption ---');
    // Consume partition 0 -> Order A
    const cons1 = await sendRequest(ProtocolRequest.consume('orders', 0));
    assert(cons1.parsed.type === 'MESSAGE' && cons1.parsed.payload.message === 'Order A', 'First consume from partition 0 returns "Order A"');

    // Consume partition 0 -> Order D
    const cons2 = await sendRequest(ProtocolRequest.consume('orders', 0));
    assert(cons2.parsed.type === 'MESSAGE' && cons2.parsed.payload.message === 'Order D', 'Second consume from partition 0 returns "Order D" (FIFO preserved)');

    // Consume partition 1 -> Order B
    const cons3 = await sendRequest(ProtocolRequest.consume('orders', 1));
    assert(cons3.parsed.type === 'MESSAGE' && cons3.parsed.payload.message === 'Order B', 'First consume from partition 1 returns "Order B"');

    // Consume partition 2 -> Order C
    const cons4 = await sendRequest(ProtocolRequest.consume('orders', 2));
    assert(cons4.parsed.type === 'MESSAGE' && cons4.parsed.payload.message === 'Order C', 'First consume from partition 2 returns "Order C"');

    // STEP 4: Empty Partition Verification
    console.log('\n--- STEP 4: Empty Partition Checks ---');
    const empty0 = await sendRequest(ProtocolRequest.consume('orders', 0));
    assert(empty0.parsed.type === 'NO_MESSAGES', 'Consuming partition 0 when empty returns NO_MESSAGES');

    await broker.stop();

    console.log('\n==================================================');
    console.log('ALL PARTITION ROUTING INTEGRATION TESTS PASSED SUCCESSFULLY! 🎉');
    console.log('==================================================\n');
  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    await broker.stop();
    process.exit(1);
  }
}

runPartitionIntegrationTest();
