import net from 'net';
import { fileURLToPath } from 'url';
import { StreamFramer } from '../protocol/framing.js';
import { ProtocolEncoder, ProtocolDecoder } from '../protocol/codec.js';
import { ProtocolRequest } from '../protocol/types.js';

export function runProducer(options = {}) {
  const port = options.port || 5000;
  const host = options.host || '127.0.0.1';
  const messageToSend = options.message || 'Hello Distributed Systems';

  return new Promise((resolve) => {
    console.log(`[Producer] Connecting to broker at tcp://${host}:${port}...`);
    const socket = net.createConnection({ port, host });
    const framer = new StreamFramer();

    socket.on('connect', () => {
      console.log('[Producer] Connected to broker.');
      const reqObj = ProtocolRequest.produce(messageToSend);
      const produceWire = ProtocolEncoder.encode(reqObj);

      console.log(`[Producer] Sending PRODUCE request: ${produceWire.trim()}`);
      socket.write(produceWire);
    });

    socket.on('data', (chunk) => {
      const frames = framer.feed(chunk);
      for (const frame of frames) {
        if (frame.error) {
          console.error('[Producer] Framing error on response:', frame.error.message);
          continue;
        }

        const decoded = ProtocolDecoder.decode(frame.raw);
        if (decoded.error) {
          console.error('[Producer] Decoding error on response:', decoded.error.message);
        } else {
          console.log('[Producer] Received response from broker:', JSON.stringify(decoded.parsed, null, 2));
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
      resolve(); // Graceful exit on network error
    });
  });
}

// Execute directly if run via CLI
const isDirectExecution = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];
if (isDirectExecution) {
  runProducer();
}
