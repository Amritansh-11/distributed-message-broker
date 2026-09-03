import { Topic } from '../src/broker/topic.js';
import { Partition } from '../src/broker/partition.js';
import { TopicManager } from '../src/broker/topic-manager.js';
import { MessageBroker } from '../src/broker/broker.js';

function assert(condition, message) {
  if (!condition) {
    throw new Error(`[FAIL] ${message}`);
  }
  console.log(`[PASS] ${message}`);
}

async function runPartitionTests() {
  console.log('==================================================');
  console.log('RUNNING MILESTONE 4 PARTITION UNIT TEST SUITE');
  console.log('==================================================\n');

  try {
    // TEST 1: Partition Abstraction Basics
    console.log('--- TEST 1: Partition Abstraction Basics ---');
    const p0 = new Partition(0);
    assert(p0.id === 0, 'Partition ID is 0');
    assert(p0.getMessageCount() === 0, 'Initial message count is 0');

    p0.enqueue('Msg 1');
    p0.enqueue('Msg 2');
    assert(p0.getMessageCount() === 2, 'Message count is 2 after enqueues');

    assert(p0.dequeue() === 'Msg 1', 'FIFO pop returns first enqueued message "Msg 1"');
    assert(p0.dequeue() === 'Msg 2', 'FIFO pop returns second enqueued message "Msg 2"');
    assert(p0.dequeue() === null, 'Dequeue on empty partition returns null');

    // TEST 2: Topic & Partition Count
    console.log('\n--- TEST 2: Topic & Partition Count ---');
    const tDefault = new Topic('default_topic');
    assert(tDefault.partitionCount === 3, 'Default topic creation yields 3 partitions');
    assert(tDefault.hasPartition(0) && tDefault.hasPartition(1) && tDefault.hasPartition(2), 'Topic contains partitions 0, 1, 2');
    assert(!tDefault.hasPartition(3), 'Partition 3 does not exist');

    const tCustom = new Topic('custom_topic', 5);
    assert(tCustom.partitionCount === 5, 'Custom topic creation yields 5 partitions');

    // TEST 3: Partition Count Validation
    console.log('\n--- TEST 3: Partition Count Validation ---');
    assert(!TopicManager.validatePartitionCount(0).valid, 'Partition count 0 correctly rejected');
    assert(!TopicManager.validatePartitionCount(-1).valid, 'Partition count -1 correctly rejected');
    assert(!TopicManager.validatePartitionCount(1.5).valid, 'Partition count 1.5 correctly rejected');
    assert(!TopicManager.validatePartitionCount('3').valid, 'Partition count "3" correctly rejected');
    assert(!TopicManager.validatePartitionCount(101).valid, 'Partition count 101 correctly rejected');
    assert(!TopicManager.validatePartitionCount(null).valid, 'Partition count null correctly rejected');

    // TEST 4: Explicit Partition Routing
    console.log('\n--- TEST 4: Explicit Partition Routing ---');
    const tRoute = new Topic('routing_topic', 3);
    tRoute.enqueue('Order A', 0);
    tRoute.enqueue('Order B', 1);
    tRoute.enqueue('Order C', 2);

    assert(tRoute.dequeue(0).message === 'Order A', 'Partition 0 explicitly produces/consumes "Order A"');
    assert(tRoute.dequeue(1).message === 'Order B', 'Partition 1 explicitly produces/consumes "Order B"');
    assert(tRoute.dequeue(2).message === 'Order C', 'Partition 2 explicitly produces/consumes "Order C"');

    // TEST 5: FIFO per Partition
    console.log('\n--- TEST 5: FIFO per Partition ---');
    tRoute.enqueue('Msg A', 0);
    tRoute.enqueue('Msg B', 0);
    tRoute.enqueue('Msg C', 0);

    assert(tRoute.dequeue(0).message === 'Msg A', 'First pop from partition 0 is Msg A');
    assert(tRoute.dequeue(0).message === 'Msg B', 'Second pop from partition 0 is Msg B');
    assert(tRoute.dequeue(0).message === 'Msg C', 'Third pop from partition 0 is Msg C');

    // TEST 6: Round-Robin Partition Selection (No key / partition)
    console.log('\n--- TEST 6: Round-Robin Routing ---');
    const tRR = new Topic('rr_topic', 3);
    tRR.enqueue('Message 1'); // -> P0
    tRR.enqueue('Message 2'); // -> P1
    tRR.enqueue('Message 3'); // -> P2
    tRR.enqueue('Message 4'); // -> P0
    tRR.enqueue('Message 5'); // -> P1
    tRR.enqueue('Message 6'); // -> P2

    assert(tRR.partitions.get(0).getMessageCount() === 2, 'Partition 0 contains 2 messages (1, 4)');
    assert(tRR.partitions.get(1).getMessageCount() === 2, 'Partition 1 contains 2 messages (2, 5)');
    assert(tRR.partitions.get(2).getMessageCount() === 2, 'Partition 2 contains 2 messages (3, 6)');

    assert(tRR.dequeue(0).message === 'Message 1', 'Partition 0 first message is Message 1');
    assert(tRR.dequeue(1).message === 'Message 2', 'Partition 1 first message is Message 2');
    assert(tRR.dequeue(2).message === 'Message 3', 'Partition 2 first message is Message 3');

    // TEST 7: Key-Based Partition Routing
    console.log('\n--- TEST 7: Key-Based Routing ---');
    const tKey = new Topic('key_topic', 3);
    const targetP = Topic.hashKey('user-123', 3);

    tKey.enqueue('Order 1', undefined, 'user-123');
    tKey.enqueue('Order 2', undefined, 'user-123');
    tKey.enqueue('Order 3', undefined, 'user-123');

    assert(tKey.partitions.get(targetP).getMessageCount() === 3, `All 3 messages with key "user-123" routed to partition ${targetP}`);
    assert(tKey.dequeue(targetP).message === 'Order 1', 'FIFO maintained for key routing Order 1');
    assert(tKey.dequeue(targetP).message === 'Order 2', 'FIFO maintained for key routing Order 2');
    assert(tKey.dequeue(targetP).message === 'Order 3', 'FIFO maintained for key routing Order 3');

    // TEST 8: Different Keys Deterministic Mapping
    console.log('\n--- TEST 8: Different Keys Deterministic Mapping ---');
    const k1 = Topic.hashKey('user-1', 3);
    const k2 = Topic.hashKey('user-2', 3);
    const k3 = Topic.hashKey('user-3', 3);
    const k4 = Topic.hashKey('user-4', 3);

    assert(k1 === Topic.hashKey('user-1', 3), `Key "user-1" consistently maps to partition ${k1}`);
    assert(k2 === Topic.hashKey('user-2', 3), `Key "user-2" consistently maps to partition ${k2}`);
    assert(k3 === Topic.hashKey('user-3', 3), `Key "user-3" consistently maps to partition ${k3}`);
    assert(k4 === Topic.hashKey('user-4', 3), `Key "user-4" consistently maps to partition ${k4}`);

    // TEST 9: Invalid Partition Error Handling
    console.log('\n--- TEST 9: Invalid Partition Error Handling ---');
    const errEnq = tKey.enqueue('Bad Partition Msg', 99);
    assert(errEnq.success === false, 'Produce to invalid partition returns success: false');
    assert(errEnq.code === 'PARTITION_NOT_FOUND', 'Error code is PARTITION_NOT_FOUND');

    const errDeq = tKey.dequeue(99);
    assert(errDeq.success === false, 'Consume from invalid partition returns success: false');
    assert(errDeq.code === 'PARTITION_NOT_FOUND', 'Error code is PARTITION_NOT_FOUND');

    // TEST 10: Multi-Topic Partition Isolation
    console.log('\n--- TEST 10: Multi-Topic & Partition Isolation ---');
    const topicManager = new TopicManager();
    topicManager.createTopic('isolated-orders', 2);
    topicManager.createTopic('isolated-payments', 2);

    topicManager.enqueue('isolated-orders', 'Order Msg', 0);
    topicManager.enqueue('isolated-payments', 'Payment Msg', 0);

    const orderMsg = topicManager.dequeue('isolated-orders', 0).message;
    const paymentMsg = topicManager.dequeue('isolated-payments', 0).message;

    assert(orderMsg === 'Order Msg', 'Orders partition 0 returns "Order Msg"');
    assert(paymentMsg === 'Payment Msg', 'Payments partition 0 returns "Payment Msg"');
    assert(topicManager.dequeue('isolated-orders', 0).message === null, 'Orders partition 0 is now empty');

    // TEST 11: Empty Partition Handling
    console.log('\n--- TEST 11: Empty Partition Handling ---');
    const emptyPop = topicManager.dequeue('isolated-orders', 1);
    assert(emptyPop.success === true, 'Empty partition dequeue succeeds');
    assert(emptyPop.message === null, 'Empty partition returns message: null');

    // TEST 12: Broker Request Handler Partition Integration
    console.log('\n--- TEST 12: MessageBroker Partition Integration ---');
    const broker = new MessageBroker();
    await broker.handleRequest({ type: 'CREATE_TOPIC', payload: { topic: 'sensor_data', partitions: 3 } });

    // PRODUCE with explicit partition
    const prodRes = await broker.handleRequest({ type: 'PRODUCE', payload: { topic: 'sensor_data', partition: 1, message: 'Temp 22C' } });
    assert(prodRes.success === true && prodRes.payload.partition === 1, 'Broker PRODUCE ACK contains partition 1');

    // GET_PARTITION_INFO
    const partInfoRes = await broker.handleRequest({ type: 'GET_PARTITION_INFO', payload: { topic: 'sensor_data', partition: 1 } });
    assert(partInfoRes.success === true && partInfoRes.payload.messageCount === 1, 'GET_PARTITION_INFO returns messageCount: 1');

    // GET_TOPIC_INFO
    const topicInfoRes = await broker.handleRequest({ type: 'GET_TOPIC_INFO', payload: { topic: 'sensor_data' } });
    assert(topicInfoRes.success === true && topicInfoRes.payload.partitions === 3, 'GET_TOPIC_INFO payload includes partitions: 3');
    assert(topicInfoRes.payload.partitionInfo.length === 3, 'GET_TOPIC_INFO payload includes partitionInfo array of length 3');

    // CONSUME with explicit partition
    const consRes = await broker.handleRequest({ type: 'CONSUME', payload: { topic: 'sensor_data', partition: 1 } });
    assert(consRes.success === true && consRes.payload.message === 'Temp 22C', 'Broker CONSUME returns "Temp 22C" from partition 1');

    console.log('\n==================================================');
    console.log('ALL MILESTONE 4 PARTITION UNIT TESTS PASSED SUCCESSFULLY! 🎉');
    console.log('==================================================\n');
  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    process.exit(1);
  }
}

runPartitionTests();
