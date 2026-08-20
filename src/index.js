import { BrokerServer } from './broker/broker.js';

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 5000;
const broker = new BrokerServer(PORT);

broker.start().catch((err) => {
  console.error('Failed to start broker:', err);
  process.exit(1);
});
