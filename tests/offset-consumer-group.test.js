import net from 'net';
import { Partition } from '../src/broker/partition.js';
import { Topic } from '../src/broker/topic.js';
import { TopicManager } from '../src/broker/topic-manager.js';
import { ConsumerGroup } from '../src/broker/consumer-group.js';
import { ConsumerGroupManager } from '../src/broker/consumer-group-manager.js';
import { MessageBroker, BrokerServer } from '../src/broker/broker.js';
import { StreamFramer } from '../src/protocol/framing.js';
import { ProtocolEncoder, ProtocolDecoder } from '../src/protocol/codec.js';
import { ProtocolRequest } from '../src/protocol/types.js';

function assert(condition, message) {
  if (!condition) {
    throw new Error(`[FAIL] ${message}`);
  }
  console.log(`[PASS] ${message}`);
}

async function runMilestone5Tests() {
  console.log('==================================================');
  console.log('RUNNING MILESTONE 5 OFFSETS & CONSUMER GROUPS TEST SUITE');
  console.log('==================================================\n');

  try {
    // TEST 1: First message receives offset 0
    console.log('--- TEST 1: First Message Receives Offset 0 ---');
    const p0 = new Partition(0);
    const e1 = p0.enqueue('First Payload');
    assert(e1.offset === 0, 'First produced message receives offset 0');

    // TEST 2: Offsets increase monotonically
    console.log('\n--- TEST 2: Monotonic Offset Increments ---');
    const e2 = p0.enqueue('Second Payload');
    const e3 = p0.enqueue('Third Payload');
    assert(e2.offset === 1, 'Second produced message receives offset 1');
    assert(e3.offset === 2, 'Third produced message receives offset 2');

    // TEST 3: Offsets are independent between partitions
    console.log('\n--- TEST 3: Partition Offset Independence ---');
    const p1 = new Partition(1);
    const eP1 = p1.enqueue('Partition 1 Msg');
    assert(eP1.offset === 0, 'First message in partition 1 receives offset 0 independently');

    // TEST 4: FIFO ordering preserved
    console.log('\n--- TEST 4: FIFO Ordering Preserved ---');
    const read0 = p0.readOffset(0);
    const read1 = p0.readOffset(1);
    assert(read0.message === 'First Payload', 'Offset 0 returns first payload');
    assert(read1.message === 'Second Payload', 'Offset 1 returns second payload');

    // TEST 5: CONSUME by offset
    console.log('\n--- TEST 5: Consume by Offset ---');
    const tm = new TopicManager();
    tm.createTopic('orders', 3);
    const resA = tm.enqueue('orders', 'Order A', 0);
    const resB = tm.enqueue('orders', 'Order B', 0);
    assert(resA.offset === 0, 'Order A has offset 0');
    assert(resB.offset === 1, 'Order B has offset 1');
    const readRes = tm.readOffset('orders', 0, 1);
    assert(readRes.success === true && readRes.message === 'Order B', 'CONSUME by offset 1 returns Order B');

    // TEST 6: Consuming does NOT delete the message
    console.log('\n--- TEST 6: Consuming Does NOT Delete Message ---');
    const readResRepeat = tm.readOffset('orders', 0, 0);
    assert(readResRepeat.success === true && readResRepeat.message === 'Order A', 'Offset 0 readable again after consumption');

    // TEST 7: Invalid offset returns OFFSET_OUT_OF_RANGE
    console.log('\n--- TEST 7: Invalid Offset returns OFFSET_OUT_OF_RANGE ---');
    const readInvalid = tm.readOffset('orders', 0, 99);
    assert(readInvalid.success === false, 'Reading out-of-range offset fails');
    assert(readInvalid.code === 'OFFSET_OUT_OF_RANGE', 'Error code is OFFSET_OUT_OF_RANGE');

    // TEST 8: Negative offset is rejected
    console.log('\n--- TEST 8: Negative Offset Rejected ---');
    const readNeg = tm.readOffset('orders', 0, -1);
    assert(readNeg.success === false, 'Reading negative offset fails');
    assert(readNeg.code === 'OFFSET_OUT_OF_RANGE', 'Error code is OFFSET_OUT_OF_RANGE');

    // TEST 9: JOIN_GROUP works
    console.log('\n--- TEST 9: JOIN_GROUP Works ---');
    const cgm = new ConsumerGroupManager(tm);
    const join1 = cgm.joinGroup('order-workers', 'consumer-1', ['orders']);
    assert(join1.success === true, 'consumer-1 joined order-workers');
    assert(join1.assignments.length === 3, 'consumer-1 assigned all 3 partitions of orders');

    // TEST 10: LEAVE_GROUP works
    console.log('\n--- TEST 10: LEAVE_GROUP Works ---');
    cgm.joinGroup('order-workers', 'consumer-2', ['orders']);
    const leave2 = cgm.leaveGroup('order-workers', 'consumer-2');
    assert(leave2.success === true, 'consumer-2 left order-workers');
    const infoAfterLeave = cgm.getGroupInfo('order-workers');
    assert(infoAfterLeave.consumers.length === 1, 'Group has 1 consumer remaining after leave');

    // TEST 11: Duplicate joins are handled cleanly
    console.log('\n--- TEST 11: Duplicate Joins Handled Cleanly ---');
    const dupJoin = cgm.joinGroup('order-workers', 'consumer-1', ['orders']);
    assert(dupJoin.success === true, 'Duplicate join returns success');

    // TEST 12: Partition assignment is deterministic
    console.log('\n--- TEST 12: Deterministic Partition Assignment ---');
    cgm.joinGroup('test-group', 'C2', ['orders']);
    cgm.joinGroup('test-group', 'C1', ['orders']);
    const gInfo = cgm.getGroupInfo('test-group');
    const c1Assign = gInfo.assignments['C1'];
    const c2Assign = gInfo.assignments['C2'];
    assert(c1Assign.length === 2, 'C1 assigned 2 partitions (0, 2)');
    assert(c2Assign.length === 1, 'C2 assigned 1 partition (1)');

    // TEST 13: Rebalancing occurs when consumers join
    console.log('\n--- TEST 13: Rebalance on Join ---');
    cgm.joinGroup('rebalance-group', 'C1', ['orders']);
    const initAssign = cgm.getGroupInfo('rebalance-group').assignments['C1'];
    assert(initAssign.length === 3, 'Initially C1 has 3 partitions');

    cgm.joinGroup('rebalance-group', 'C2', ['orders']);
    const postJoinAssign = cgm.getGroupInfo('rebalance-group').assignments['C1'];
    assert(postJoinAssign.length === 2, 'After C2 joins, C1 has 2 partitions');

    // TEST 14: Rebalancing occurs when consumers leave
    console.log('\n--- TEST 14: Rebalance on Leave ---');
    cgm.leaveGroup('rebalance-group', 'C2');
    const postLeaveAssign = cgm.getGroupInfo('rebalance-group').assignments['C1'];
    assert(postLeaveAssign.length === 3, 'After C2 leaves, C1 reclaims all 3 partitions');

    // TEST 15: Partition assigned to at most one consumer in group
    console.log('\n--- TEST 15: Single Consumer Ownership per Partition ---');
    cgm.joinGroup('solo-group', 'C1', ['orders']);
    cgm.joinGroup('solo-group', 'C2', ['orders']);
    cgm.joinGroup('solo-group', 'C3', ['orders']);
    const soloInfo = cgm.getGroupInfo('solo-group');
    const allAssigned = [];
    for (const c of soloInfo.consumers) {
      for (const a of soloInfo.assignments[c]) {
        allAssigned.push(a.partition);
      }
    }
    const uniqueAssigned = new Set(allAssigned);
    assert(allAssigned.length === uniqueAssigned.size, 'Each partition assigned to at most one consumer in group');

    // TEST 16: Different consumer groups have independent offsets
    console.log('\n--- TEST 16: Independent Group Offsets ---');
    cgm.joinGroup('group-A', 'C-A', ['orders']);
    cgm.joinGroup('group-B', 'C-B', ['orders']);

    const cResA = cgm.commitOffset('group-A', 'orders', 0, 1);
    const cResB = cgm.commitOffset('group-B', 'orders', 0, 0);

    assert(cResA.success === true, 'Group A committed offset 1 successfully');
    assert(cResB.success === true, 'Group B committed offset 0 successfully');

    const offsetA = cgm.getGroupInfo('group-A').committedOffsets.find(o => o.partition === 0).offset;
    const offsetB = cgm.getGroupInfo('group-B').committedOffsets.find(o => o.partition === 0).offset;
    assert(offsetA === 1, 'Group A committed offset is 1');
    assert(offsetB === 0, 'Group B committed offset is 0');

    // TEST 17: COMMIT_OFFSET works
    console.log('\n--- TEST 17: COMMIT_OFFSET Works ---');
    const commitRes = cgm.commitOffset('group-A', 'orders', 0, 1);
    assert(commitRes.success === true, 'COMMIT_OFFSET succeeds');

    // TEST 18: Invalid commit requests are rejected
    console.log('\n--- TEST 18: Invalid Commit Requests Rejected ---');
    const badCommitGroup = cgm.commitOffset('non-existent-group', 'orders', 0, 1);
    assert(badCommitGroup.success === false && badCommitGroup.code === 'GROUP_NOT_FOUND', 'Commit to unknown group rejected with GROUP_NOT_FOUND');

    const badCommitOffset = cgm.commitOffset('group-A', 'orders', 0, 999);
    assert(badCommitOffset.success === false && badCommitOffset.code === 'OFFSET_OUT_OF_RANGE', 'Out of range offset commit rejected with OFFSET_OUT_OF_RANGE');

    // TEST 19: GET_GROUP_INFO works
    console.log('\n--- TEST 19: GET_GROUP_INFO Works ---');
    const groupInfoRes = cgm.getGroupInfo('group-A');
    assert(groupInfoRes.success === true, 'GET_GROUP_INFO succeeds');
    assert(groupInfoRes.groupId === 'group-A', 'Returns correct groupId');
    assert(Array.isArray(groupInfoRes.consumers), 'Returns consumers array');

    // TEST 20: Group-based CONSUME works
    console.log('\n--- TEST 20: Group-Based CONSUME Works ---');
    tm.createTopic('group-test-topic', 1);
    tm.enqueue('group-test-topic', 'Group Msg 0', 0);
    tm.enqueue('group-test-topic', 'Group Msg 1', 0);

    cgm.joinGroup('consumer-group-1', 'cons-1', ['group-test-topic']);
    const groupConsume1 = cgm.consume('consumer-group-1', 'cons-1', 'group-test-topic');
    assert(groupConsume1.success === true && groupConsume1.message === 'Group Msg 0' && groupConsume1.offset === 0, 'Group consume reads offset 0');

    const groupConsume2 = cgm.consume('consumer-group-1', 'cons-1', 'group-test-topic');
    assert(groupConsume2.success === true && groupConsume2.message === 'Group Msg 1' && groupConsume2.offset === 1, 'Group consume advances position and reads offset 1');

    // TEST 21: Broker Server TCP Protocol Integration (MILESTONE 5)
    console.log('\n--- TEST 21: Broker Server TCP Integration ---');
    const brokerServer = new BrokerServer(5008, '127.0.0.1');
    await brokerServer.start();

    const sendProtocol = (cmdObj) => {
      return new Promise((resolve, reject) => {
        const socket = net.createConnection({ port: 5008, host: '127.0.0.1' });
        const framer = new StreamFramer();
        let result = null;

        socket.on('connect', () => {
          socket.write(ProtocolEncoder.encode(cmdObj));
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

    // Create topic
    await sendProtocol(ProtocolRequest.createTopic('tcp-orders', 2));

    // PRODUCE message and inspect offset in PRODUCE_ACK
    const prodAck = await sendProtocol(ProtocolRequest.produce('tcp-orders', 'TCP Order Payload 1'));
    assert(prodAck.parsed.type === 'PRODUCE_ACK', 'Broker returns PRODUCE_ACK');
    assert(prodAck.parsed.payload.offset === 0, 'PRODUCE_ACK payload contains offset 0');

    // JOIN_GROUP via TCP
    const joinAck = await sendProtocol(ProtocolRequest.joinGroup('tcp-workers', 'worker-1', ['tcp-orders']));
    assert(joinAck.parsed.type === 'JOIN_GROUP_ACK', 'Broker returns JOIN_GROUP_ACK');

    // Group CONSUME via TCP
    const groupCons = await sendProtocol(ProtocolRequest.consume('tcp-orders', undefined, undefined, 'tcp-workers', 'worker-1'));
    assert(groupCons.parsed.type === 'MESSAGE', 'Group CONSUME returns MESSAGE');
    assert(groupCons.parsed.payload.message === 'TCP Order Payload 1', 'Received "TCP Order Payload 1"');
    assert(groupCons.parsed.payload.offset === 0, 'MESSAGE payload includes offset 0');

    // COMMIT_OFFSET via TCP
    const commitAck = await sendProtocol(ProtocolRequest.commitOffset('tcp-workers', 'tcp-orders', 0, 0));
    assert(commitAck.parsed.type === 'COMMIT_OFFSET_ACK', 'Broker returns COMMIT_OFFSET_ACK');

    // GET_GROUP_INFO via TCP
    const gInfoAck = await sendProtocol(ProtocolRequest.getGroupInfo('tcp-workers'));
    assert(gInfoAck.parsed.type === 'GROUP_INFO', 'Broker returns GROUP_INFO');

    // LEAVE_GROUP via TCP
    const leaveAck = await sendProtocol(ProtocolRequest.leaveGroup('tcp-workers', 'worker-1'));
    assert(leaveAck.parsed.type === 'LEAVE_GROUP_ACK', 'Broker returns LEAVE_GROUP_ACK');

    await brokerServer.stop();

    console.log('\n==================================================');
    console.log('ALL MILESTONE 5 OFFSETS & CONSUMER GROUPS TESTS PASSED! 🎉');
    console.log('==================================================\n');
  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    process.exit(1);
  }
}

runMilestone5Tests();
