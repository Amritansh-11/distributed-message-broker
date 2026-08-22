import { ProtocolDecoder, ProtocolEncoder } from '../src/protocol/codec.js';
import { RequestValidator } from '../src/protocol/validator.js';
import { ProtocolRequest, ProtocolResponse } from '../src/protocol/types.js';

function assert(condition, message) {
  if (!condition) {
    throw new Error(`[FAIL] ${message}`);
  }
  console.log(`[PASS] ${message}`);
}

function runCodecTests() {
  console.log('==================================================');
  console.log('RUNNING PROTOCOL CODEC & VALIDATOR VERIFICATION SUITE');
  console.log('==================================================\n');

  try {
    // TEST 1: Encoding & Decoding Valid Requests
    console.log('--- TEST 1: Encoding & Decoding Requests ---');
    const produceReq = ProtocolRequest.produce('Test payload');
    const produceWire = ProtocolEncoder.encode(produceReq);
    assert(produceWire.endsWith('\n'), 'Encoder appends newline delimiter');

    const decodedReq = ProtocolDecoder.decode(produceWire.trim());
    assert(!decodedReq.error, 'Decoder parses wire string without error');
    assert(decodedReq.parsed.type === 'PRODUCE', 'Parsed request type matches');
    assert(decodedReq.parsed.message === 'Test payload', 'Parsed request message matches');

    // TEST 2: Encoding & Decoding Valid Responses
    console.log('\n--- TEST 2: Encoding & Decoding Responses ---');
    const msgRes = ProtocolResponse.message('Sample message');
    const msgWire = ProtocolEncoder.encode(msgRes);
    const decodedRes = ProtocolDecoder.decode(msgWire.trim());
    assert(!decodedRes.error, 'Decoder parses response wire string');
    assert(decodedRes.parsed.type === 'MESSAGE', 'Response type matches');
    assert(decodedRes.parsed.message === 'Sample message', 'Response payload matches');

    // TEST 3: Decoding Malformed JSON
    console.log('\n--- TEST 3: Handling Malformed JSON ---');
    const malformed = ProtocolDecoder.decode('{ invalid json payload');
    assert(malformed.error !== undefined, 'Decoder catches syntax error on invalid JSON');
    assert(malformed.error.message.includes('Malformed JSON wire payload'), 'Decoder error contains descriptive syntax message');

    // TEST 4: Request Validation — Success Cases
    console.log('\n--- TEST 4: Request Validation (Valid Requests) ---');
    const v1 = RequestValidator.validate({ type: 'PING' });
    assert(v1.valid === true, 'PING request is valid');

    const v2 = RequestValidator.validate({ type: 'PRODUCE', message: 'Hello' });
    assert(v2.valid === true, 'PRODUCE request with string message is valid');

    const v3 = RequestValidator.validate({ type: 'CONSUME' });
    assert(v3.valid === true, 'CONSUME request is valid');

    // TEST 5: Request Validation — Failure Cases
    console.log('\n--- TEST 5: Request Validation (Invalid Requests) ---');
    const vErr1 = RequestValidator.validate(null);
    assert(vErr1.valid === false && vErr1.error.includes('non-null JSON object'), 'Catches null request');

    const vErr2 = RequestValidator.validate({ type: 'UNKNOWN_CMD' });
    assert(vErr2.valid === false && vErr2.error.includes('Unknown request type'), 'Catches unknown command type');

    const vErr3 = RequestValidator.validate({ type: 'PRODUCE' });
    assert(vErr3.valid === false && vErr3.error.includes('must include a string "message"'), 'Catches PRODUCE with missing message');

    const vErr4 = RequestValidator.validate({ type: 'PRODUCE', message: 12345 });
    assert(vErr4.valid === false && vErr4.error.includes('must include a string "message"'), 'Catches PRODUCE with non-string message');

    const vErr5 = RequestValidator.validate({ type: 'PRODUCE', message: '' });
    assert(vErr5.valid === false && vErr5.error.includes('cannot be empty'), 'Catches PRODUCE with empty message');

    console.log('\n==================================================');
    console.log('ALL CODEC & VALIDATOR TESTS PASSED SUCCESSFULLY! 🎉');
    console.log('==================================================\n');
  } catch (err) {
    console.error('\n[TEST FAILURE]', err);
    process.exit(1);
  }
}

runCodecTests();
