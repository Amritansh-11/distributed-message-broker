import { TopicManager } from '../src/broker/topic-manager.js';

function assert(condition, message) {
  if (!condition) {
    throw new Error(`[FAIL] ${message}`);
  }
  console.log(`[PASS] ${message}`);
}

function runTopicManagerTests() {
  console.log('==================================================');
  console.log('RUNNING TOPIC MANAGER UNIT TEST SUITE');
  console.log('==================================================\n');

  try {
    const manager = new TopicManager();

    // TEST 1: Create topic
    console.log('--- TEST 1: Create Topic ---');
    const createRes1 = manager.createTopic('orders');
    assert(createRes1.success === true, 'Topic "orders" created successfully');
    assert(manager.hasTopic('orders') === true, 'manager.hasTopic("orders") returns true');

    // TEST 2: Create duplicate topic
    console.log('\n--- TEST 2: Create Duplicate Topic ---');
    const createRes2 = manager.createTopic('orders');
    assert(createRes2.success === false, 'Duplicate topic creation returns success: false');
    assert(createRes2.code === 'TOPIC_ALREADY_EXISTS', 'Error code is TOPIC_ALREADY_EXISTS');

    // TEST 3: List topics
    console.log('\n--- TEST 3: List Topics ---');
    manager.createTopic('payments');
    manager.createTopic('notifications');
    const topicsList = manager.listTopics();
    assert(topicsList.length === 3, 'Returns 3 active topics');
    assert(topicsList.includes('orders') && topicsList.includes('payments') && topicsList.includes('notifications'), 'List contains all created topics');

    // TEST 4: Get topic info
    console.log('\n--- TEST 4: Get Topic Info ---');
    manager.enqueue('orders', 'Order 1');
    manager.enqueue('orders', 'Order 2');
    const info = manager.getTopicInfo('orders');
    assert(info.success === true, 'Get topic info succeeds');
    assert(info.topic === 'orders', 'Topic name matches');
    assert(info.messageCount === 2, 'Message count is 2');

    // TEST 5: Unknown topic
    console.log('\n--- TEST 5: Unknown Topic ---');
    const unknownInfo = manager.getTopicInfo('unknown');
    assert(unknownInfo.success === false, 'Unknown topic info returns success: false');
    assert(unknownInfo.code === 'TOPIC_NOT_FOUND', 'Error code is TOPIC_NOT_FOUND');

    const unknownDequeue = manager.dequeue('unknown');
    assert(unknownDequeue.success === false, 'Dequeue from unknown topic returns success: false');
    assert(unknownDequeue.code === 'TOPIC_NOT_FOUND', 'Error code is TOPIC_NOT_FOUND');

    // TEST 6: Valid topic names
    console.log('\n--- TEST 6: Valid Topic Names ---');
    const validNames = ['orders', 'payments-v1', 'user_events', 'order.created', 'logs_2026'];
    for (const name of validNames) {
      const v = TopicManager.validateTopicName(name);
      assert(v.valid === true, `Topic name "${name}" is valid`);
    }

    // TEST 7: Invalid topic names
    console.log('\n--- TEST 7: Invalid Topic Names ---');
    const invalidNames = ['', 'topic with spaces', 'topic/with/slashes', 'A'.repeat(101)];
    for (const name of invalidNames) {
      const v = TopicManager.validateTopicName(name);
      assert(v.valid === false, `Topic name "${name}" is correctly rejected`);
    }

    // TEST 8: Delete empty topic
    console.log('\n--- TEST 8: Delete Empty Topic ---');
    manager.createTopic('temp-topic');
    const delRes1 = manager.deleteTopic('temp-topic');
    assert(delRes1.success === true, 'Empty topic deleted successfully');
    assert(manager.hasTopic('temp-topic') === false, 'Deleted topic is no longer present');

    // TEST 9: Delete non-empty topic
    console.log('\n--- TEST 9: Delete Non-Empty Topic ---');
    const delRes2 = manager.deleteTopic('orders'); // contains 2 messages
    assert(delRes2.success === false, 'Deletion of non-empty topic rejected');
    assert(delRes2.code === 'TOPIC_NOT_EMPTY', 'Error code is TOPIC_NOT_EMPTY');

    console.log('\n==================================================');
    console.log('ALL TOPIC MANAGER UNIT TESTS PASSED SUCCESSFULLY! 🎉');
    console.log('==================================================\n');
  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    process.exit(1);
  }
}

runTopicManagerTests();
