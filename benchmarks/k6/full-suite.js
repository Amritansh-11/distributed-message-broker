import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const produceLatency = new Trend('produce_latency_ms');
const consumeLatency = new Trend('consume_latency_ms');
const totalOps = new Counter('total_operations');

export const options = {
  scenarios: {
    smoke_health: {
      executor: 'constant-vus',
      vus: 5,
      duration: '10s',
      exec: 'testHealth',
    },
    load_metrics: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '5s', target: 25 },
        { duration: '10s', target: 50 },
        { duration: '5s', target: 0 },
      ],
      exec: 'testMetrics',
    },
    load_produce: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '5s', target: 20 },
        { duration: '15s', target: 80 },
        { duration: '5s', target: 0 },
      ],
      exec: 'testProduce',
    },
    load_consume: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '5s', target: 20 },
        { duration: '15s', target: 80 },
        { duration: '5s', target: 0 },
      ],
      exec: 'testConsume',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<100'],
    http_req_failed: ['rate<0.01'],
    produce_latency_ms: ['p(95)<50'],
    consume_latency_ms: ['p(95)<50'],
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://127.0.0.1:8000';
const TOPIC = 'k6-full-suite-topic';

export function setup() {
  const headers = { 'Content-Type': 'application/json' };
  http.post(`${BASE_URL}/topic`, JSON.stringify({ topic: TOPIC, partitions: 5, replicationFactor: 1 }), { headers });

  // Pre-seed 200 records
  for (let i = 0; i < 200; i++) {
    http.post(`${BASE_URL}/produce`, JSON.stringify({
      topic: TOPIC,
      partition: i % 5,
      message: `Full suite preseed record #${i}`
    }), { headers });
  }
}

export function testHealth() {
  const res = http.get(`${BASE_URL}/health`);
  check(res, {
    'health status 200': (r) => r.status === 200,
    'health status HEALTHY': (r) => r.json() && r.json().status === 'HEALTHY',
  });
  totalOps.add(1);
  sleep(0.01);
}

export function testMetrics() {
  const res = http.get(`${BASE_URL}/metrics`);
  check(res, {
    'metrics status 200': (r) => r.status === 200,
  });
  totalOps.add(1);
  sleep(0.01);
}

export function testProduce() {
  const vuId = __VU;
  const iter = __ITER;
  const payload = JSON.stringify({
    topic: TOPIC,
    message: `Stress produce payload VU#${vuId} Iter#${iter}`,
    key: `key-${iter % 10}`
  });
  const headers = { 'Content-Type': 'application/json' };

  const start = Date.now();
  const res = http.post(`${BASE_URL}/produce`, payload, { headers });
  produceLatency.add(Date.now() - start);

  check(res, {
    'produce 200': (r) => r.status === 200,
    'produce success': (r) => r.json() && r.json().success === true,
  });
  totalOps.add(1);
  sleep(0.005);
}

export function testConsume() {
  const partition = __ITER % 5;
  const offset = __ITER % 200;

  const start = Date.now();
  const res = http.get(`${BASE_URL}/consume?topic=${TOPIC}&partition=${partition}&offset=${offset}`);
  consumeLatency.add(Date.now() - start);

  check(res, {
    'consume 200': (r) => r.status === 200,
    'consume success': (r) => r.json() && r.json().success === true,
  });
  totalOps.add(1);
  sleep(0.005);
}
