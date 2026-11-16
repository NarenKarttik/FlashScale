import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// Custom FAANG-Grade Metrics
const successReservations = new Counter('successful_reservations');
const soldOutResponses    = new Counter('sold_out_responses');
const rateLimitedCount    = new Counter('rate_limited_count');
const reservationLatency  = new Trend('reservation_latency_ms', true);

export const options = {
  scenarios: {
    flash_sale_10k_spike: {
      executor: 'ramping-arrival-rate',
      startRate: 500,
      timeUnit: '1s',
      preAllocatedVUs: 1500,
      maxVUs: 12000,
      stages: [
        { duration: '10s', target: 2000 },  // Warm-up ramp: 2,000 req/s
        { duration: '20s', target: 10000 }, // Traffic spike: ramp up to 10,000 requests/sec
        { duration: '30s', target: 10000 }, // Sustained peak: hold 10,000 requests/sec
        { duration: '10s', target: 0 },     // Cool-down
      ],
    },
  },
  thresholds: {
    // FAANG SLA: p95 under 20ms, p99 under 35ms, 99.99% availability
    'http_req_duration': ['p(95)<20', 'p(99)<35'],
    'http_req_failed': ['rate<0.001'], // 99.99% availability
    'successful_reservations': ['count>0'],
  },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:8080';

export default function () {
  const userId = `user_${__VU}_${__ITER}`;
  const idempotencyKey = `idemp_${userId}_${Date.now()}_${Math.random()}`;

  const payload = JSON.stringify({
    eventId: 1,
    userId: userId,
    idempotencyKey: idempotencyKey,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-Request-ID': idempotencyKey,
    },
  };

  const start = Date.now();
  const res = http.post(`${BASE_URL}/api/v1/bookings/reserve`, payload, params);
  reservationLatency.add(Date.now() - start);

  if (res.status === 200) {
    successReservations.add(1);
    check(res, {
      'status is 200 (Reserved)': (r) => r.status === 200,
      'has reservationId': (r) => JSON.parse(r.body).reservationId !== undefined,
    });
  } else if (res.status === 410) {
    soldOutResponses.add(1);
    check(res, {
      'status is 410 (Sold Out)': (r) => r.status === 410,
    });
  } else if (res.status === 429) {
    rateLimitedCount.add(1);
    check(res, {
      'status is 429 (Rate Limited)': (r) => r.status === 429,
    });
  } else {
    check(res, {
      'unexpected error': (r) => false,
    });
  }
}
