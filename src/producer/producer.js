import net from 'net';
import { NDJSONFramer } from '../protocol/message.js';

export function runProducer(options = {}) {
  const port = options.port || 5000;
  const host = options.host || '127.0.0.1';
  const messageToSend = options.message || 'Hello Distributed Systems';

  return new Promise((resolve, reject) => {
    console.log(`[Producer] Connecting to broker at ${host}:${port}...`);
    const socket = net.createConnection({ port, host });
    const framer = new NDJSONFramer();

    socket.on('connect', () => {
      console.log('[Producer] Connected to broker.');
      const produceCmd = NDJSONFramer.encode({
        type: 'PRODUCE',
        message: messageToSend
      });

      console.log(`[Producer] Sending PRODUCE request: ${produceCmd.trim()}`);
      socket.write(produceCmd);
    });

    socket.on('data', (chunk) => {
      const frames = framer.feed(chunk);
      for (const frame of frames) {
        if (frame.error) {
          console.error('[Producer] Received malformed response:', frame.raw);
        } else {
          console.log('[Producer] Received response from broker:', JSON.stringify(frame.parsed, null, 2));
        }
      }
      socket.end(); // Close connection after receiving ACK
    });

    socket.on('close', () => {
      console.log('[Producer] Connection closed.');
      resolve();
    });

    socket.on('error', (err) => {
      console.error(`[Producer Error] Could not communicate with broker: ${err.message}`);
      resolve(); // Graceful exit on network error as per requirements
    });
  });
}

// Execute directly if run via CLI
if (process.argv[1] && process.argv[1].endsWith('producer.js')) {
  runProducer();
}
