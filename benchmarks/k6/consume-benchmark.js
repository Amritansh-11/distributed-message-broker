import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const consumeLatency = new Trend('consume_latency_ms');
const consumeCounter = new Counter('consume_count');

export const options = {
  stages: [
    { duration: '5s', target: 10 },
    { duration: '15s', target: 50 },
    { duration: '10s', target: 100 },
    { duration: '5s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<50'],
    http_req_failed: ['rate<0.01'],
    consume_latency_ms: ['p(95)<30'],
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://127.0.0.1:8000';
const TOPIC = 'k6-consume-topic';
const TOTAL_PRELOAD = 100;

export function setup() {
  const headers = { 'Content-Type': 'application/json' };
  // Create topic
  http.post(`${BASE_URL}/topic`, JSON.stringify({ topic: TOPIC, partitions: 3, replicationFactor: 1 }), { headers });

  // Seed messages to consume
  for (let i = 0; i < TOTAL_PRELOAD; i++) {
    http.post(`${BASE_URL}/produce`, JSON.stringify({
      topic: TOPIC,
      partition: i % 3,
      message: `Preseeded consume message #${i}`
    }), { headers });
  }
}

export default function () {
  const partition = __ITER % 3;
  const offset = __ITER % TOTAL_PRELOAD;

  const startTime = Date.now();
  const res = http.get(`${BASE_URL}/consume?topic=${TOPIC}&partition=${partition}&offset=${offset}`);
  const latency = Date.now() - startTime;

  consumeLatency.add(latency);

  const success = check(res, {
    'consume status is 200': (r) => r.status === 200,
    'consume response success': (r) => r.json() && r.json().success === true,
  });

  if (success) {
    consumeCounter.add(1);
  }

  sleep(0.005);
}
