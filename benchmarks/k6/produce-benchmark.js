import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const produceLatency = new Trend('produce_latency_ms');
const produceCounter = new Counter('produce_count');

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
    produce_latency_ms: ['p(95)<30'],
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://127.0.0.1:8000';
const TOPIC = 'k6-produce-topic';

export function setup() {
  // Ensure topic exists
  const headers = { 'Content-Type': 'application/json' };
  const payload = JSON.stringify({ topic: TOPIC, partitions: 3, replicationFactor: 1 });
  http.post(`${BASE_URL}/topic`, payload, { headers });
}

export default function () {
  const vuId = __VU;
  const iter = __ITER;
  const key = `user-${vuId % 20}`;
  const message = `Benchmark message VU#${vuId} Iter#${iter} payload timestamp=${Date.now()}`;

  const payload = JSON.stringify({
    topic: TOPIC,
    message: message,
    key: key
  });

  const headers = { 'Content-Type': 'application/json' };
  const startTime = Date.now();
  const res = http.post(`${BASE_URL}/produce`, payload, { headers });
  const latency = Date.now() - startTime;

  produceLatency.add(latency);
  
  const success = check(res, {
    'produce status is 200': (r) => r.status === 200,
    'produce ACK success': (r) => r.json() && r.json().success === true,
    'has valid offset': (r) => r.json() && typeof r.json().payload.offset === 'number',
  });

  if (success) {
    produceCounter.add(1);
  }

  sleep(0.005);
}
