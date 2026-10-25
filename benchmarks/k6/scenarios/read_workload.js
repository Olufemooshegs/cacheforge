import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    reads: {
      executor: 'constant-vus',
      vus: __ENV.VUS ? parseInt(__ENV.VUS) : 50,
      duration: __ENV.DURATION || '30s',
    },
  },
  thresholds: {
    'http_req_duration': ['p(95)<1000'],
    'http_req_failed': ['rate<0.01'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8000';
const PRODUCT_COUNT = parseInt(__ENV.PRODUCT_COUNT || '1000');
// 'zipf' concentrates traffic on low IDs (hot keys); 'uniform' spreads.
const DISTRIBUTION = __ENV.DISTRIBUTION || 'zipf';

function pickProductId() {
  if (DISTRIBUTION === 'zipf') {
    const r = Math.random();
    // Approximate zipf: high probability of low IDs.
    const idx = Math.floor(1 / r) - 1;
    return (idx % PRODUCT_COUNT) + 1;
  }
  return Math.floor(Math.random() * PRODUCT_COUNT) + 1;
}

export default function () {
  const id = pickProductId();
  const res = http.get(`${BASE_URL}/products/${id}`);
  check(res, {
    'status 200': (r) => r.status === 200,
  });
}