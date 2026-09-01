import net from 'net';
import { fileURLToPath } from 'url';
import { StreamFramer } from '../protocol/framing.js';
import { ProtocolEncoder, ProtocolDecoder } from '../protocol/codec.js';
import { ProtocolRequest } from '../protocol/types.js';

export function runProducer(options = {}) {
  const port = options.port || 5000;
  const host = options.host || '127.0.0.1';
  const topic = options.topic || process.argv[2] || 'orders';

  let partition = options.partition;
  let key = options.key;
  let messageToSend = options.message;

  if (messageToSend === undefined) {
    const arg3 = process.argv[3];
    const arg4 = process.argv[4];
    if (arg3 !== undefined && /^\d+$/.test(arg3) && arg4 !== undefined) {
      partition = parseInt(arg3, 10);
      messageToSend = arg4;
    } else {
      messageToSend = arg3 || 'Hello Distributed Systems';
    }
  }

  return new Promise((resolve) => {
    console.log(`[Producer] Connecting to broker at tcp://${host}:${port}...`);
    const socket = net.createConnection({ port, host });
    const framer = new StreamFramer();

    socket.on('connect', () => {
      console.log('[Producer] Connected to broker.');
      console.log(`[Producer] Topic: ${topic}`);
      if (partition !== undefined) console.log(`[Producer] Explicit Partition: ${partition}`);
      if (key !== undefined) console.log(`[Producer] Key: ${key}`);
      console.log(`[Producer] Message: ${messageToSend}`);

      const reqObj = ProtocolRequest.produce(topic, messageToSend, partition, key);
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
      socket.end();
    });

    socket.on('close', () => {
      console.log('[Producer] Connection closed.');
      resolve();
    });

    socket.on('error', (err) => {
      console.error(`[Producer Error] Could not communicate with broker: ${err.message}`);
      resolve();
    });
  });
}

const isDirectExecution = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];
if (isDirectExecution) {
  runProducer();
}
