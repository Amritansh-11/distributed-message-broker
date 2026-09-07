import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '5s', target: 20 },
    { duration: '10s', target: 100 },
    { duration: '5s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<30'],
    http_req_failed: ['rate<0.001'],
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://127.0.0.1:8000';

export default function () {
  const res = http.get(`${BASE_URL}/metrics`);
  check(res, {
    'status is 200': (r) => r.status === 200,
    'body includes active_connections': (r) => r.body.includes('active_connections'),
  });
  sleep(0.01);
}
