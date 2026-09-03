import fs from 'fs';
import path from 'path';
import net from 'net';
import { BrokerServer } from '../src/broker/server.js';
import { MessageBroker } from '../src/broker/broker.js';
import { StorageEngine } from '../src/storage/storage-engine.js';
import { StreamFramer } from '../src/protocol/framing.js';
import { ProtocolEncoder, ProtocolDecoder } from '../src/protocol/codec.js';
import { ProtocolRequest } from '../src/protocol/types.js';

const TEST_DATA_DIR = path.join(process.cwd(), 'scratch', `test-storage-data-${Date.now()}`);

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

async function runStorageTests() {
  console.log('==================================================');
  console.log('RUNNING MILESTONE 6 PERSISTENT STORAGE TEST SUITE');
  console.log('==================================================\n');

  try {
    cleanupDataDir(TEST_DATA_DIR);

    // TEST 1: Messages Persisted to Disk on PRODUCE
    console.log('--- TEST 1: Message Persisted to Disk ---');
    const broker1 = new MessageBroker({ dataDir: TEST_DATA_DIR });
    await broker1.handleRequest({ type: 'CREATE_TOPIC', payload: { topic: 'orders', partitions: 1 } });
    await broker1.handleRequest({ type: 'PRODUCE', payload: { topic: 'orders', partition: 0, message: 'Order Persistent Payload 1' } });
    await broker1.handleRequest({ type: 'PRODUCE', payload: { topic: 'orders', partition: 0, message: 'Order Persistent Payload 2' } });

    const logFile = path.join(TEST_DATA_DIR, 'topics', 'orders', 'partition-0', '000000000000.log');
    assert(fs.existsSync(logFile), 'Segment log file 000000000000.log exists on disk');

    // Close first broker instance to simulate shutdown
    broker1.clear();

    // TEST 2 & 3: Messages Recovered on Restart
    console.log('\n--- TEST 2 & 3: Messages Survive Broker Restart ---');
    const broker2 = new MessageBroker({ dataDir: TEST_DATA_DIR });
    broker2.recover();

    assert(broker2.topicManager.hasTopic('orders'), 'Topic "orders" recovered from disk');
    const orderInfo = broker2.topicManager.getTopicInfo('orders');
    assert(orderInfo.messageCount === 2, '2 messages recovered into in-memory partition log');

    // TEST 4 & 5: Multiple Topics & Partitions Recovered
    console.log('\n--- TEST 4 & 5: Multiple Topics & Partitions Survive Restart ---');
    await broker2.handleRequest({ type: 'CREATE_TOPIC', payload: { topic: 'payments', partitions: 2 } });
    await broker2.handleRequest({ type: 'PRODUCE', payload: { topic: 'payments', partition: 0, message: 'Payment 1' } });
    await broker2.handleRequest({ type: 'PRODUCE', payload: { topic: 'payments', partition: 1, message: 'Payment 2' } });

    broker2.clear();

    // Restart broker again
    const broker3 = new MessageBroker({ dataDir: TEST_DATA_DIR });
    broker3.recover();
    assert(broker3.topicManager.hasTopic('orders') && broker3.topicManager.hasTopic('payments'), 'Both orders and payments recovered');
    const payInfo = broker3.topicManager.getTopicInfo('payments');
    assert(payInfo.partitions === 2, 'Payments recovered with 2 partitions');
    assert(payInfo.messageCount === 2, 'Payments total message count recovered correctly');

    // TEST 6 & 7: Offsets Survive Restart & Next Offset is Correct
    console.log('\n--- TEST 6 & 7: Offsets & Next Offset After Restart ---');
    const prodResD = await broker3.handleRequest({ type: 'PRODUCE', payload: { topic: 'orders', partition: 0, message: 'Order Persistent Payload 3' } });
    assert(prodResD.success === true, 'PRODUCE succeeds after restart');
    assert(prodResD.payload.offset === 2, 'Next produced offset is 2 (continued after 0, 1)');

    // TEST 8: CONSUME by Offset Works After Restart
    console.log('\n--- TEST 8: CONSUME by Offset After Restart ---');
    const consRes1 = await broker3.handleRequest({ type: 'CONSUME', payload: { topic: 'orders', partition: 0, offset: 1 } });
    assert(consRes1.success === true && consRes1.payload.message === 'Order Persistent Payload 2', 'CONSUME offset 1 returns "Order Persistent Payload 2"');

    // TEST 9: Consumed Messages Remain Persisted
    console.log('\n--- TEST 9: Consumed Messages Remain Persisted ---');
    const consResRepeat = await broker3.handleRequest({ type: 'CONSUME', payload: { topic: 'orders', partition: 0, offset: 1 } });
    assert(consResRepeat.success === true && consResRepeat.payload.message === 'Order Persistent Payload 2', 'Message at offset 1 readable again after consumption');

    // TEST 10 & 11: Consumer Group Committed Offsets & Independent Offsets Survive Restart
    console.log('\n--- TEST 10 & 11: Consumer Group Offsets Survive Restart ---');
    await broker3.handleRequest({ type: 'JOIN_GROUP', payload: { groupId: 'order-workers', consumerId: 'worker-1', topics: ['orders'] } });
    await broker3.handleRequest({ type: 'JOIN_GROUP', payload: { groupId: 'analytics-workers', consumerId: 'analyst-1', topics: ['orders'] } });

    await broker3.handleRequest({ type: 'COMMIT_OFFSET', payload: { groupId: 'order-workers', topic: 'orders', partition: 0, offset: 2 } });
    await broker3.handleRequest({ type: 'COMMIT_OFFSET', payload: { groupId: 'analytics-workers', topic: 'orders', partition: 0, offset: 0 } });

    // Restart broker
    const broker4 = new MessageBroker({ dataDir: TEST_DATA_DIR });
    broker4.recover();

    const gInfoOrders = await broker4.handleRequest({ type: 'GET_GROUP_INFO', payload: { groupId: 'order-workers' } });
    const gInfoAnalytics = await broker4.handleRequest({ type: 'GET_GROUP_INFO', payload: { groupId: 'analytics-workers' } });

    assert(gInfoOrders.success === true && gInfoOrders.payload.committedOffsets[0].offset === 2, 'order-workers committed offset 2 recovered');
    assert(gInfoAnalytics.success === true && gInfoAnalytics.payload.committedOffsets[0].offset === 0, 'analytics-workers committed offset 0 recovered independently');

    // TEST 12 & 13: Segment Rollover & Multiple Segment Recovery
    console.log('\n--- TEST 12 & 13: Segment Rollover & Multiple Segment Recovery ---');
    const ROLLOVER_DIR = path.join(process.cwd(), 'scratch', `test-rollover-data-${Date.now()}`);
    cleanupDataDir(ROLLOVER_DIR);

    const brokerRollover1 = new MessageBroker({ dataDir: ROLLOVER_DIR, maxMessagesPerSegment: 2 });
    await brokerRollover1.handleRequest({ type: 'CREATE_TOPIC', payload: { topic: 'sensor_logs', partitions: 1 } });

    for (let i = 0; i < 5; i++) {
      await brokerRollover1.handleRequest({ type: 'PRODUCE', payload: { topic: 'sensor_logs', partition: 0, message: `Sensor Msg ${i}` } });
    }

    const sensorDir = path.join(ROLLOVER_DIR, 'topics', 'sensor_logs', 'partition-0');
    const segFiles = fs.readdirSync(sensorDir).filter(f => f.endsWith('.log'));
    assert(segFiles.length > 1, `Segment rollover created ${segFiles.length} log files (threshold: 2 messages/segment)`);

    // Restart broker and verify all 5 messages recovered in correct sequence
    const brokerRollover2 = new MessageBroker({ dataDir: ROLLOVER_DIR, maxMessagesPerSegment: 2 });
    brokerRollover2.recover();

    for (let i = 0; i < 5; i++) {
      const readMsg = await brokerRollover2.handleRequest({ type: 'CONSUME', payload: { topic: 'sensor_logs', partition: 0, offset: i } });
      assert(readMsg.success === true && readMsg.payload.message === `Sensor Msg ${i}`, `Recovered message ${i} ("Sensor Msg ${i}") from segment files`);
    }
    cleanupDataDir(ROLLOVER_DIR);

    // TEST 14 & 15: Incomplete Final Record Safety & Preserving Valid Preceding Records
    console.log('\n--- TEST 14 & 15: Incomplete Final Record Safety ---');
    const CRASH_DIR = path.join(process.cwd(), 'scratch', `test-crash-data-${Date.now()}`);
    cleanupDataDir(CRASH_DIR);

    const storageCrash1 = new StorageEngine({ dataDir: CRASH_DIR });
    storageCrash1.init();
    storageCrash1.appendMessage('crash_topic', 0, 0, 'Valid Msg 0');
    storageCrash1.appendMessage('crash_topic', 0, 1, 'Valid Msg 1');
    storageCrash1.close();

    // Simulate abrupt power loss / crash: write partial incomplete length & payload bytes to end of log
    const crashLogFile = path.join(CRASH_DIR, 'topics', 'crash_topic', 'partition-0', '000000000000.log');
    const partialBuffer = Buffer.from([0x00, 0x00, 0x00, 0x50, 0x7B, 0x22, 0x6F, 0x66]); // Partial header & payload
    fs.appendFileSync(crashLogFile, partialBuffer);

    // Restart broker and verify crash recovery truncates tail cleanly and recovers Valid Msg 0 & 1
    const brokerCrash = new MessageBroker({ dataDir: CRASH_DIR });
    brokerCrash.recover();

    const infoCrash = brokerCrash.topicManager.getTopicInfo('crash_topic');
    assert(infoCrash.messageCount === 2, 'Recovered exactly 2 valid messages preceding crash tail');

    const msg0 = await brokerCrash.handleRequest({ type: 'CONSUME', payload: { topic: 'crash_topic', partition: 0, offset: 0 } });
    const msg1 = await brokerCrash.handleRequest({ type: 'CONSUME', payload: { topic: 'crash_topic', partition: 0, offset: 1 } });
    assert(msg0.payload.message === 'Valid Msg 0', 'Msg 0 preserved intact');
    assert(msg1.payload.message === 'Valid Msg 1', 'Msg 1 preserved intact');

    // Produce next message after crash recovery
    const msgNext = await brokerCrash.handleRequest({ type: 'PRODUCE', payload: { topic: 'crash_topic', partition: 0, message: 'Post-Crash Msg 2' } });
    assert(msgNext.success === true && msgNext.payload.offset === 2, 'Next message after crash receives offset 2');

    cleanupDataDir(CRASH_DIR);

    // TEST 16: Corrupted Record Detection
    console.log('\n--- TEST 16: Corrupted Record Detection ---');
    const CORRUPT_DIR = path.join(process.cwd(), 'scratch', `test-corrupt-data-${Date.now()}`);
    cleanupDataDir(CORRUPT_DIR);

    const storageCorrupt = new StorageEngine({ dataDir: CORRUPT_DIR });
    storageCorrupt.init();
    storageCorrupt.appendMessage('bad_topic', 0, 0, 'Good Msg');
    storageCorrupt.close();

    const corruptLogFile = path.join(CORRUPT_DIR, 'topics', 'bad_topic', 'partition-0', '000000000000.log');
    // Overwrite payload bytes with invalid garbage characters
    const fileBuf = fs.readFileSync(corruptLogFile);
    fileBuf[fileBuf.length - 5] = 0xFF;
    fileBuf[fileBuf.length - 4] = 0xFF;
    fs.writeFileSync(corruptLogFile, fileBuf);

    let caughtCorruptErr = false;
    try {
      const brokerCorrupt = new MessageBroker({ dataDir: CORRUPT_DIR });
      brokerCorrupt.recover();
    } catch (err) {
      caughtCorruptErr = true;
      assert(err.message.includes('Corrupted storage log file'), 'Broker safely rejects startup on corrupted storage log');
    }
    assert(caughtCorruptErr === true, 'Corrupted record successfully detected and caught');
    cleanupDataDir(CORRUPT_DIR);

    // TEST 17: BrokerServer TCP Persistence Integration
    console.log('\n--- TEST 17: BrokerServer TCP Persistence Integration ---');
    const TCP_PERSIST_DIR = path.join(process.cwd(), 'scratch', `test-tcp-persist-${Date.now()}`);
    cleanupDataDir(TCP_PERSIST_DIR);

    const serverPort = 5010;
    const server1 = new BrokerServer(serverPort, '127.0.0.1', { dataDir: TCP_PERSIST_DIR });
    await server1.start();

    const sendReq = (reqObj) => {
      return new Promise((resolve, reject) => {
        const socket = net.createConnection({ port: serverPort, host: '127.0.0.1' });
        const framer = new StreamFramer();
        let result = null;

        socket.on('connect', () => {
          socket.write(ProtocolEncoder.encode(reqObj));
        });

        socket.on('data', (chunk) => {
          const frames = framer.feed(chunk);
          if (frames.length > 0) {
            result = ProtocolDecoder.decode(frames[0].raw);
          }
          socket.end();
        });

        socket.on('close', () => resolve(result));
        socket.on('error', reject);
      });
    };

    await sendReq(ProtocolRequest.createTopic('tcp-persistent-topic', 2));
    const tcpProdAck = await sendReq(ProtocolRequest.produce('tcp-persistent-topic', 'TCP Persistent Payload', 0));
    assert(tcpProdAck.parsed.payload.offset === 0, 'TCP PRODUCE returned offset 0');

    await server1.stop();

    // Restart TCP BrokerServer with same directory
    const server2 = new BrokerServer(serverPort, '127.0.0.1', { dataDir: TCP_PERSIST_DIR });
    await server2.start();

    const tcpCons = await sendReq(ProtocolRequest.consume('tcp-persistent-topic', 0, 0));
    assert(tcpCons.parsed.type === 'MESSAGE' && tcpCons.parsed.payload.message === 'TCP Persistent Payload', 'Consumed "TCP Persistent Payload" over TCP after server restart');

    await server2.stop();
    cleanupDataDir(TCP_PERSIST_DIR);
    cleanupDataDir(TEST_DATA_DIR);

    console.log('\n==================================================');
    console.log('ALL MILESTONE 6 PERSISTENT STORAGE TESTS PASSED! 🎉');
    console.log('==================================================\n');
  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    cleanupDataDir(TEST_DATA_DIR);
    process.exit(1);
  }
}

runStorageTests();
