import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '5s', target: 20 },
    { duration: '10s', target: 100 },
    { duration: '5s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<30'], // 95% of requests must complete within 30ms
    http_req_failed: ['rate<0.001'],  // Error rate < 0.1%
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://127.0.0.1:8000';

export default function () {
  const res = http.get(`${BASE_URL}/health`);
  check(res, {
    'status is 200': (r) => r.status === 200,
    'status is HEALTHY': (r) => r.json().status === 'HEALTHY',
  });
  sleep(0.01);
}
