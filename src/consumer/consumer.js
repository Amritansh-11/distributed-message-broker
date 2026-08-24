import net from 'net';
import { fileURLToPath } from 'url';
import { StreamFramer } from '../protocol/framing.js';
import { ProtocolEncoder, ProtocolDecoder } from '../protocol/codec.js';
import { ProtocolRequest } from '../protocol/types.js';

export function runConsumer(options = {}) {
  const port = options.port || 5000;
  const host = options.host || '127.0.0.1';
  const topic = options.topic || process.argv[2] || 'orders';

  return new Promise((resolve) => {
    console.log(`[Consumer] Connecting to broker at tcp://${host}:${port}...`);
    const socket = net.createConnection({ port, host });
    const framer = new StreamFramer();

    socket.on('connect', () => {
      console.log('[Consumer] Connected to broker.');
      const reqObj = ProtocolRequest.consume(topic);
      const consumeWire = ProtocolEncoder.encode(reqObj);

      console.log(`[Consumer] Sending CONSUME request for topic "${topic}": ${consumeWire.trim()}`);
      socket.write(consumeWire);
    });

    socket.on('data', (chunk) => {
      const frames = framer.feed(chunk);
      for (const frame of frames) {
        if (frame.error) {
          console.error('[Consumer] Framing error on response:', frame.error.message);
          continue;
        }

        const decoded = ProtocolDecoder.decode(frame.raw);
        if (decoded.error) {
          console.error('[Consumer] Decoding error on response:', decoded.error.message);
        } else {
          const res = decoded.parsed;
          if (res.type === 'MESSAGE' && res.payload) {
            console.log(`Topic: ${res.payload.topic}`);
            console.log(`Message: ${res.payload.message}`);
          } else {
            console.log('[Consumer] Received response from broker:', JSON.stringify(res, null, 2));
          }
        }
      }
      socket.end(); // Close connection after receiving response
    });

    socket.on('close', () => {
      console.log('[Consumer] Connection closed.');
      resolve();
    });

    socket.on('error', (err) => {
      console.error(`[Consumer Error] Could not communicate with broker: ${err.message}`);
      resolve(); // Graceful exit on network error
    });
  });
}

// Execute directly if run via CLI
const isDirectExecution = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];
if (isDirectExecution) {
  runConsumer();
}
