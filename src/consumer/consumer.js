import net from 'net';
import { NDJSONFramer } from '../protocol/message.js';

export function runConsumer(options = {}) {
  const port = options.port || 5000;
  const host = options.host || '127.0.0.1';

  return new Promise((resolve, reject) => {
    console.log(`[Consumer] Connecting to broker at ${host}:${port}...`);
    const socket = net.createConnection({ port, host });
    const framer = new NDJSONFramer();

    socket.on('connect', () => {
      console.log('[Consumer] Connected to broker.');
      const consumeCmd = NDJSONFramer.encode({ type: 'CONSUME' });

      console.log(`[Consumer] Sending CONSUME request: ${consumeCmd.trim()}`);
      socket.write(consumeCmd);
    });

    socket.on('data', (chunk) => {
      const frames = framer.feed(chunk);
      for (const frame of frames) {
        if (frame.error) {
          console.error('[Consumer] Received malformed response:', frame.raw);
        } else {
          console.log('[Consumer] Received response from broker:', JSON.stringify(frame.parsed, null, 2));
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
      resolve(); // Graceful exit on network error as per requirements
    });
  });
}

// Execute directly if run via CLI
if (process.argv[1] && process.argv[1].endsWith('consumer.js')) {
  runConsumer();
}
