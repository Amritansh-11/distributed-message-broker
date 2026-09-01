import net from 'net';
import { fileURLToPath } from 'url';
import { StreamFramer } from '../protocol/framing.js';
import { ProtocolEncoder, ProtocolDecoder } from '../protocol/codec.js';
import { ProtocolRequest } from '../protocol/types.js';

export function createTopicCli(options = {}) {
  const port = options.port || 5000;
  const host = options.host || '127.0.0.1';
  const topic = options.topic || process.argv[2] || 'orders';
  const partitionsArg = options.partitions !== undefined ? options.partitions : (process.argv[3] ? parseInt(process.argv[3], 10) : 3);

  return new Promise((resolve) => {
    console.log(`[Topic CLI] Connecting to broker at tcp://${host}:${port}...`);
    const socket = net.createConnection({ port, host });
    const framer = new StreamFramer();

    socket.on('connect', () => {
      console.log(`[Topic CLI] Sending CREATE_TOPIC request for "${topic}" with ${partitionsArg} partition(s)...`);
      const reqObj = ProtocolRequest.createTopic(topic, partitionsArg);
      const wireData = ProtocolEncoder.encode(reqObj);
      socket.write(wireData);
    });

    socket.on('data', (chunk) => {
      const frames = framer.feed(chunk);
      for (const frame of frames) {
        if (!frame.error) {
          const decoded = ProtocolDecoder.decode(frame.raw);
          if (!decoded.error) {
            console.log('[Topic CLI] Response:', JSON.stringify(decoded.parsed, null, 2));
          }
        }
      }
      socket.end();
    });

    socket.on('close', () => resolve());
    socket.on('error', (err) => {
      console.error(`[Topic CLI Error] ${err.message}`);
      resolve();
    });
  });
}

const isDirectExecution = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];
if (isDirectExecution) {
  createTopicCli();
}
