// Rider load generator: POSTs trip requests to Dispatch to fake rider demand. A steady RPS
// plus a BURST multiplier that kicks in at BURST_AT seconds drives exp2 (rider-request burst).
//
// Dials from the root .env (override per run on the CLI):
//   RPS  steady req/sec   BURST  x-multiplier   BURST_AT  when it starts (s, 9999 = never)
//   DURATION  total s     DISPATCH_URL  target  CITY_LAT/CITY_LON  map centre
import "dotenv/config";

const DISPATCH_URL = process.env.DISPATCH_URL;
const RPS = parseInt(process.env.RPS, 10);
const BURST = parseFloat(process.env.BURST);
const BURST_AT = parseInt(process.env.BURST_AT, 10);
const DURATION = parseInt(process.env.DURATION, 10);
const CITY_LAT = parseFloat(process.env.CITY_LAT);
const CITY_LON = parseFloat(process.env.CITY_LON);
if (!DISPATCH_URL) { console.error("[riders] DISPATCH_URL is required — set it in .env"); process.exit(1); }
const SPAN = 0.05;

const rnd = (a, b) => a + Math.random() * (b - a);
let sent = 0, ok = 0, failed = 0, latencies = [];

async function sendOne() {
  const body = {
    userId: `rider-${Math.floor(rnd(0, 100000))}`,
    origin: { type: "Point", coordinates: [CITY_LON + rnd(-SPAN, SPAN), CITY_LAT + rnd(-SPAN, SPAN)] },
    destination: { type: "Point", coordinates: [CITY_LON + rnd(-SPAN, SPAN), CITY_LAT + rnd(-SPAN, SPAN)] },
  };
  const t0 = Date.now();
  sent++;
  try {
    const r = await fetch(`${DISPATCH_URL}/requests`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    latencies.push(Date.now() - t0);
    r.ok ? ok++ : failed++;
  } catch {
    failed++;
  }
}

let elapsed = 0;
console.log(`[riders] target=${DISPATCH_URL} rps=${RPS} burst=${BURST}x@${BURST_AT}s duration=${DURATION}s`);
const timer = setInterval(() => {
  const rate = elapsed >= BURST_AT ? Math.round(RPS * BURST) : RPS;
  for (let i = 0; i < rate; i++) sendOne();
  elapsed++;
  if (elapsed % 5 === 0) console.log(`[riders] t=${elapsed}s sent=${sent} ok=${ok} failed=${failed}`);
  if (elapsed >= DURATION) finish();
}, 1000);

function finish() {
  clearInterval(timer);
  latencies.sort((a, b) => a - b);
  const p = (q) => latencies.length ? latencies[Math.floor(q * latencies.length)] : 0;
  console.log(JSON.stringify({
    metric: "riders", rps: RPS, burst: BURST, burstAt: BURST_AT, duration: DURATION,
    sent, ok, failed, p50ms: p(0.5), p95ms: p(0.95), p99ms: p(0.99),
  }));
  setTimeout(() => process.exit(0), 500);
}
process.on("SIGINT", finish);
