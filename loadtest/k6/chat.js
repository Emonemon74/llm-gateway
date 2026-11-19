// Open-model load (constant arrival rate): requests arrive on schedule whether or not earlier ones
// finished, so a slow gateway shows up as latency / dropped iterations, not as a quietly
// reduced request rate ("coordinated omission").
//
// env: BASES (comma list of gateway URLs, round-robined = a client-side load balancer),
//      KEYS (comma list of API keys), RATE (req/s), DURATION, MODEL, EXPECT_429=1
import http from 'k6/http';
import exec from 'k6/execution';
import { Counter } from 'k6/metrics';

const BASES = __ENV.BASES.split(',');
const KEYS = __ENV.KEYS.split(',');
const MODEL = __ENV.MODEL || 'fast';
const RATE = parseInt(__ENV.RATE);

const s200 = new Counter('status_200');
const s429 = new Counter('status_429');
const s5xx = new Counter('status_5xx');
const sOther = new Counter('status_other');

if (__ENV.EXPECT_429) http.setResponseCallback(http.expectedStatuses(200, 429));

export const options = {
  scenarios: {
    load: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: __ENV.DURATION || '20s',
      preAllocatedVUs: Math.min(RATE, 400),
      maxVUs: 4000,
    },
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  discardResponseBodies: true,
};

const PAYLOAD = JSON.stringify({
  model: MODEL,
  messages: [{ role: 'user', content: 'Summarise the benefits of connection pooling.' }],
});

export default function () {
  const i = exec.scenario.iterationInTest;
  const res = http.post(`${BASES[i % BASES.length]}/v1/chat/completions`, PAYLOAD, {
    headers: { Authorization: `Bearer ${KEYS[i % KEYS.length]}`, 'Content-Type': 'application/json' },
  });
  if (res.status === 200) s200.add(1);
  else if (res.status === 429) s429.add(1);
  else if (res.status >= 500) s5xx.add(1);
  else sOther.add(1);
}
