import { Partition } from '../src/broker/partition.js';
import { Topic } from '../src/broker/topic.js';
import { TopicManager } from '../src/broker/topic-manager.js';
import { MessageBroker } from '../src/broker/broker.js';
import { RequestValidator } from '../src/protocol/validator.js';

function assert(condition, message) {
  if (!condition) {
    throw new Error(`[FAIL] ${message}`);
  }
  console.log(`[PASS] ${message}`);
}

function runPartitionTests() {
  console.log('==================================================');
  console.log('RUNNING MILESTONE 4 PARTITION UNIT TEST SUITE');
  console.log('==================================================\n');

  try {
    // TEST 1: Partition Abstraction Basics
    console.log('--- TEST 1: Partition Abstraction Basics ---');
    const part0 = new Partition(0);
    assert(part0.id === 0, 'Partition ID is 0');
    assert(part0.getMessageCount() === 0, 'Initial message count is 0');
    part0.enqueue('Msg 1');
    part0.enqueue('Msg 2');
    assert(part0.getMessageCount() === 2, 'Message count is 2 after enqueues');
    assert(part0.dequeue() === 'Msg 1', 'FIFO pop returns first enqueued message "Msg 1"');
    assert(part0.dequeue() === 'Msg 2', 'FIFO pop returns second enqueued message "Msg 2"');
    assert(part0.dequeue() === null, 'Dequeue on empty partition returns null');

    // TEST 2: Topic & Default Partition Count
    console.log('\n--- TEST 2: Topic & Partition Count ---');
    const topicDefault = new Topic('orders');
    assert(topicDefault.partitionCount === 3, 'Default topic creation yields 3 partitions');
    assert(topicDefault.hasPartition(0) && topicDefault.hasPartition(1) && topicDefault.hasPartition(2), 'Topic contains partitions 0, 1, 2');
    assert(!topicDefault.hasPartition(3), 'Partition 3 does not exist');

    const topicCustom = new Topic('payments', 5);
    assert(topicCustom.partitionCount === 5, 'Custom topic creation yields 5 partitions');

    // TEST 3: Partition Count Validation
    console.log('\n--- TEST 3: Partition Count Validation ---');
    const invalidCounts = [0, -1, 1.5, "3", 101, null, undefined];
    for (const val of invalidCounts) {
      if (val === undefined) continue; // undefined is handled as default in topic creation
      const res = TopicManager.validatePartitionCount(val);
      assert(res.valid === false, `Partition count ${JSON.stringify(val)} correctly rejected`);
    }

    // TEST 4: Explicit Partition Routing
    console.log('\n--- TEST 4: Explicit Partition Routing ---');
    const topicManager = new TopicManager();
    topicManager.createTopic('orders', 3);
    topicManager.enqueue('orders', 'Order A', 0);
    topicManager.enqueue('orders', 'Order B', 1);
    topicManager.enqueue('orders', 'Order C', 2);

    const pop0 = topicManager.dequeue('orders', 0);
    const pop1 = topicManager.dequeue('orders', 1);
    const pop2 = topicManager.dequeue('orders', 2);

    assert(pop0.message === 'Order A', 'Partition 0 explicitly produces/consumes "Order A"');
    assert(pop1.message === 'Order B', 'Partition 1 explicitly produces/consumes "Order B"');
    assert(pop2.message === 'Order C', 'Partition 2 explicitly produces/consumes "Order C"');

    // TEST 5: FIFO per Partition
    console.log('\n--- TEST 5: FIFO per Partition ---');
    topicManager.enqueue('orders', 'Msg A', 0);
    topicManager.enqueue('orders', 'Msg B', 0);
    topicManager.enqueue('orders', 'Msg C', 0);

    assert(topicManager.dequeue('orders', 0).message === 'Msg A', 'First pop from partition 0 is Msg A');
    assert(topicManager.dequeue('orders', 0).message === 'Msg B', 'Second pop from partition 0 is Msg B');
    assert(topicManager.dequeue('orders', 0).message === 'Msg C', 'Third pop from partition 0 is Msg C');

    // TEST 6: Round-Robin Routing
    console.log('\n--- TEST 6: Round-Robin Routing ---');
    topicManager.createTopic('rr-topic', 3);
    for (let i = 1; i <= 6; i++) {
      topicManager.enqueue('rr-topic', `Message ${i}`);
    }

    assert(topicManager.getPartitionInfo('rr-topic', 0).messageCount === 2, 'Partition 0 contains 2 messages (1, 4)');
    assert(topicManager.getPartitionInfo('rr-topic', 1).messageCount === 2, 'Partition 1 contains 2 messages (2, 5)');
    assert(topicManager.getPartitionInfo('rr-topic', 2).messageCount === 2, 'Partition 2 contains 2 messages (3, 6)');

    const rrPop0 = topicManager.dequeue('rr-topic', 0).message;
    const rrPop1 = topicManager.dequeue('rr-topic', 1).message;
    const rrPop2 = topicManager.dequeue('rr-topic', 2).message;

    assert(rrPop0 === 'Message 1', 'Partition 0 first message is Message 1');
    assert(rrPop1 === 'Message 2', 'Partition 1 first message is Message 2');
    assert(rrPop2 === 'Message 3', 'Partition 2 first message is Message 3');

    // TEST 7: Key-Based Routing & Determinism
    console.log('\n--- TEST 7: Key-Based Routing ---');
    topicManager.createTopic('keyed-topic', 3);
    const key = 'user-123';
    topicManager.enqueue('keyed-topic', 'Order 1', undefined, key);
    topicManager.enqueue('keyed-topic', 'Order 2', undefined, key);
    topicManager.enqueue('keyed-topic', 'Order 3', undefined, key);

    const targetPId = Topic.hashKey(key, 3);
    const infoKeyed = topicManager.getPartitionInfo('keyed-topic', targetPId);
    assert(infoKeyed.messageCount === 3, `All 3 messages with key "${key}" routed to partition ${targetPId}`);

    assert(topicManager.dequeue('keyed-topic', targetPId).message === 'Order 1', 'FIFO maintained for key routing Order 1');
    assert(topicManager.dequeue('keyed-topic', targetPId).message === 'Order 2', 'FIFO maintained for key routing Order 2');
    assert(topicManager.dequeue('keyed-topic', targetPId).message === 'Order 3', 'FIFO maintained for key routing Order 3');

    // TEST 8: Different Keys Routing
    console.log('\n--- TEST 8: Different Keys Deterministic Mapping ---');
    const keys = ['user-1', 'user-2', 'user-3', 'user-4'];
    for (const k of keys) {
      const h1 = Topic.hashKey(k, 3);
      const h2 = Topic.hashKey(k, 3);
      assert(h1 === h2 && h1 >= 0 && h1 < 3, `Key "${k}" consistently maps to partition ${h1}`);
    }

    // TEST 9: Invalid Partition Error Handling
    console.log('\n--- TEST 9: Invalid Partition Error Handling ---');
    const invalidProduce = topicManager.enqueue('orders', 'Bad Order', 5);
    assert(invalidProduce.success === false, 'Produce to invalid partition returns success: false');
    assert(invalidProduce.code === 'PARTITION_NOT_FOUND', 'Error code is PARTITION_NOT_FOUND');

    const invalidConsume = topicManager.dequeue('orders', 5);
    assert(invalidConsume.success === false, 'Consume from invalid partition returns success: false');
    assert(invalidConsume.code === 'PARTITION_NOT_FOUND', 'Error code is PARTITION_NOT_FOUND');

    // TEST 10: Multi-Topic & Partition Isolation
    console.log('\n--- TEST 10: Multi-Topic & Partition Isolation ---');
    topicManager.createTopic('isolated-orders', 3);
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
    broker.handleRequest({ type: 'CREATE_TOPIC', payload: { topic: 'sensor_data', partitions: 3 } });

    // PRODUCE with explicit partition
    const prodRes = broker.handleRequest({ type: 'PRODUCE', payload: { topic: 'sensor_data', partition: 1, message: 'Temp 22C' } });
    assert(prodRes.success === true && prodRes.payload.partition === 1, 'Broker PRODUCE ACK contains partition 1');

    // GET_PARTITION_INFO
    const partInfoRes = broker.handleRequest({ type: 'GET_PARTITION_INFO', payload: { topic: 'sensor_data', partition: 1 } });
    assert(partInfoRes.success === true && partInfoRes.payload.messageCount === 1, 'GET_PARTITION_INFO returns messageCount: 1');

    // GET_TOPIC_INFO
    const topicInfoRes = broker.handleRequest({ type: 'GET_TOPIC_INFO', payload: { topic: 'sensor_data' } });
    assert(topicInfoRes.success === true && topicInfoRes.payload.partitions === 3, 'GET_TOPIC_INFO payload includes partitions: 3');
    assert(topicInfoRes.payload.partitionInfo.length === 3, 'GET_TOPIC_INFO payload includes partitionInfo array of length 3');

    // CONSUME with explicit partition
    const consRes = broker.handleRequest({ type: 'CONSUME', payload: { topic: 'sensor_data', partition: 1 } });
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
