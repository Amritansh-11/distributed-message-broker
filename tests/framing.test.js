import { StreamFramer } from '../src/protocol/framing.js';

function assert(condition, message) {
  if (!condition) {
    throw new Error(`[FAIL] ${message}`);
  }
  console.log(`[PASS] ${message}`);
}

function runFramingTests() {
  console.log('==================================================');
  console.log('RUNNING STREAM FRAMER VERIFICATION SUITE');
  console.log('==================================================\n');

  try {
    // TEST 1: Simple single frame framing
    console.log('--- TEST 1: Single Frame Extraction ---');
    const framer1 = new StreamFramer();
    const res1 = framer1.feed('{"type":"PING"}\n');
    assert(res1.length === 1, 'Extracted 1 frame from single line chunk');
    assert(res1[0].raw === '{"type":"PING"}', 'Frame content matches expected wire string');

    // TEST 2: Multiple frames in single TCP chunk
    console.log('\n--- TEST 2: Multiple Frames in Single Chunk ---');
    const framer2 = new StreamFramer();
    const res2 = framer2.feed('{"type":"PING"}\n{"type":"CONSUME"}\r\n');
    assert(res2.length === 2, 'Extracted 2 frames from multi-line chunk');
    assert(res2[0].raw === '{"type":"PING"}', 'First frame content matches');
    assert(res2[1].raw === '{"type":"CONSUME"}', 'Second frame content matches (CRLF handled)');

    // TEST 3: Partial packet chunking across stream events
    console.log('\n--- TEST 3: Partial Packet Chunking Across Events ---');
    const framer3 = new StreamFramer();
    const chunkA = '{"type":"PRODUCE","mes';
    const chunkB = 'sage":"Hello World"}\n';

    const res3A = framer3.feed(chunkA);
    assert(res3A.length === 0, 'Buffer holds partial frame chunk until delimiter');

    const res3B = framer3.feed(chunkB);
    assert(res3B.length === 1, 'Frame extracted when completing chunk arrives');
    assert(res3B[0].raw === '{"type":"PRODUCE","message":"Hello World"}', 'Reconstructed chunked payload matches');

    // TEST 4: Max Frame Size Limit Violation
    console.log('\n--- TEST 4: Max Frame Size Safety Limit ---');
    const smallFramer = new StreamFramer({ maxFrameSize: 20 });
    const res4 = smallFramer.feed('{"type":"PRODUCE","message":"Oversized payload payload payload"}\n');
    assert(res4.length === 1, 'Emits error result on oversized frame');
    assert(res4[0].error !== undefined, 'Error object present on frame result');
    assert(res4[0].error.message.includes('Frame size exceeds maximum allowed limit'), 'Error message identifies limit breach');

    // TEST 5: Un-delimited Buffer Overflow Protection
    console.log('\n--- TEST 5: Un-delimited Buffer Overflow Protection ---');
    const overflowFramer = new StreamFramer({ maxFrameSize: 30 });
    const overflowChunk = 'A'.repeat(40); // No newline
    const res5 = overflowFramer.feed(overflowChunk);
    assert(res5.length === 1, 'Emits error result on un-delimited buffer overflow');
    assert(res5[0].error !== undefined && res5[0].error.message.includes('Frame buffer overflow'), 'Error message identifies buffer overflow');
    assert(overflowFramer.buffer === '', 'Internal buffer cleared after overflow');

    console.log('\n==================================================');
    console.log('ALL STREAM FRAMER TESTS PASSED SUCCESSFULLY! 🎉');
    console.log('==================================================\n');
  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    process.exit(1);
  }
}

runFramingTests();
