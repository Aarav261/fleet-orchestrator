// Fleet simulator: N virtual taxis roaming a coordinate grid, each publishing JSON telemetry
// to fleet/telemetry/<id> at 1 Hz over its own mTLS connection. On-vehicle edge filtering
// suppresses redundant messages.
//
// Config + experiment dials come from the root .env; override per run on the CLI, e.g.
//   N=800 node simulator/fleet-simulator.js        (CLI wins over .env)
//   N            fleet size          -> exp1 (ingest capacity)
//   EDGE_FILTER  on|off              -> exp3 (edge-filter savings)
//   RATE_MS      tick interval, 1000 = 1 Hz    DURATION_S  0 = run until Ctrl+C
//   CITY_LAT/CITY_LON, IOT_ENDPOINT, IOT_CERT_DIR

import "dotenv/config";
import mqtt from "mqtt";
import fs from "node:fs";
import path from "node:path";

const N = parseInt(process.env.N, 10);
const RATE_MS = parseInt(process.env.RATE_MS, 10);
const EDGE_FILTER = process.env.EDGE_FILTER !== "off";
const DURATION_S = parseInt(process.env.DURATION_S, 10); // 0 = run forever
const CITY_LAT = parseFloat(process.env.CITY_LAT);
const CITY_LON = parseFloat(process.env.CITY_LON);

// One mTLS client per vehicle, each with its own X.509 cert (from provision-vehicles.js).
// ponytail: one connection per vehicle is fine to a few hundred; for large-N load use a shared cert.
const { IOT_ENDPOINT, IOT_CERT_DIR } = process.env;
for (const [k, v] of Object.entries({ IOT_ENDPOINT, IOT_CERT_DIR })) {
  if (!v) { console.error(`[sim] ${k} is required — set it in .env`); process.exit(1); }
}

const SPAN = 0.05;            // ~0.05deg box (~5.5 km) around city centre
const METERS_PER_DEG = 111_320;

// Edge-filter thresholds (plan §Algorithms): publish only on real change.
const MOVE_M = 5;             // moved > 5 m
const SOC_PCT = 1;            // or SOC changed > 1%
const HEARTBEAT_MS = 30_000;  // else force one every 30 s (idle vs offline)

const rnd = (a, b) => a + Math.random() * (b - a);
// great-circle distance in metres between two {lat, lon} points
const haversineM = (a, b) => {
  const dLat = ((b.lat - a.lat) * Math.PI) / 180;
  const dLon = ((b.lon - a.lon) * Math.PI) / 180;
  const la1 = (a.lat * Math.PI) / 180, la2 = (b.lat * Math.PI) / 180;
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(la1) * Math.cos(la2) * Math.sin(dLon / 2) ** 2;
  return 2 * 6_371_000 * Math.asin(Math.sqrt(h));
};

class Vehicle {
  constructor(id) {
    this.id = id;
    this.lat = CITY_LAT + rnd(-SPAN, SPAN);
    this.lon = CITY_LON + rnd(-SPAN, SPAN);
    this.heading = rnd(0, 2 * Math.PI);
    this.speed = rnd(0, 60); // km/h
    this.soc = rnd(20, 100); // %
    this.status = "available"; // available | busy | charging | fault
    this.faults = [];
    this.lastSent = null; // last published {lat, lon, soc}
    this.lastHeartbeat = 0;
  }

  // Advance one tick: move along heading, drain battery, maybe fault.
  step() {
    if (Math.random() < 0.1) this.heading += rnd(-0.6, 0.6);  // occasional turn
    const metres = (this.speed * 1000 / 3600) * (RATE_MS / 1000);
    this.lat += (metres * Math.cos(this.heading)) / METERS_PER_DEG;
    this.lon += (metres * Math.sin(this.heading)) / (METERS_PER_DEG * Math.cos((this.lat * Math.PI) / 180));
    this.speed = Math.max(0, Math.min(60, this.speed + rnd(-5, 5)));

    // Battery: below 15% the vehicle flips itself to "charging" (leaves the matching pool),
    // recharges, and rejoins as "available" above 80%.
    this.soc = Math.max(0, this.soc - rnd(0, 0.05));
    if (this.status !== "charging" && this.soc < 15) this.status = "charging";
    if (this.status === "charging") this.soc = Math.min(100, this.soc + 0.5);
    if (this.status === "charging" && this.soc > 80) this.status = "available";

    // Rare transient fault, clears on its own.
    if (Math.random() < 0.0005) { this.status = "fault"; this.faults = ["diagnostic"]; }
    else if (this.status === "fault" && Math.random() < 0.2) { this.status = "available"; this.faults = []; }
  }

  payload() {
    return {
      vehicleId: this.id,
      ts: new Date().toISOString(),
      location: { type: "Point", coordinates: [round(this.lon), round(this.lat)] },
      speed: round(this.speed),
      soc: round(this.soc),
      status: this.status,
      faults: this.faults,
    };
  }

  // Edge filter: returns the payload to publish, or null to suppress this tick.
  filtered(now) {
    const p = this.payload();
    if (!EDGE_FILTER || !this.lastSent) { this.mark(now, p); return p; }
    const moved = haversineM(this.lastSent, { lat: this.lat, lon: this.lon });
    const socChanged = Math.abs(this.lastSent.soc - this.soc) > SOC_PCT;
    const statusChanged = this.lastSent.status !== this.status;
    const heartbeat = now - this.lastHeartbeat >= HEARTBEAT_MS;
    if (moved > MOVE_M || socChanged || statusChanged || heartbeat) { this.mark(now, p); return p; }
    return null;
  }

  mark(now, p) {
    this.lastSent = { lat: this.lat, lon: this.lon, soc: this.soc, status: this.status };
    this.lastHeartbeat = now;
  }
}

const round = (x) => Math.round(x * 1e5) / 1e5;

const vehicles = Array.from({ length: N }, (_, i) => new Vehicle(`veh-${String(i).padStart(4, "0")}`));

// Open one mTLS connection per vehicle (cert must be provisioned first).
const ca = fs.readFileSync(path.join(IOT_CERT_DIR, "AmazonRootCA1.pem"));
for (const v of vehicles) {
  const certFile = path.join(IOT_CERT_DIR, `${v.id}.cert.pem`);
  const keyFile = path.join(IOT_CERT_DIR, `${v.id}.private.key`);
  if (!fs.existsSync(certFile)) {
    console.error(`[sim] missing cert for ${v.id} in ${IOT_CERT_DIR}\n      run: node infra/iot/provision-vehicles.js ${N}`);
    process.exit(1);
  }
  v.client = mqtt.connect(`mqtts://${IOT_ENDPOINT}:8883`, {
    clientId: v.id, ca,
    cert: fs.readFileSync(certFile),
    key: fs.readFileSync(keyFile),
    reconnectPeriod: 2000,
  });
  v.client.on("error", (e) => console.error(`[sim] ${v.id} error: ${e.message}`));
}

let ticks = 0, generated = 0, published = 0, bytes = 0;

const tick = () => {
  const now = Date.now();
  for (const v of vehicles) {
    v.step();
    generated++;
    const p = v.filtered(now);
    if (!p) continue;
    if (!v.client.connected) continue; // skip while (re)connecting; QoS 0, no buffering
    const msg = JSON.stringify(p);
    v.client.publish(`fleet/telemetry/${v.id}`, msg, { qos: 0 });
    published++; bytes += msg.length;
  }
  ticks++;
  if (ticks % 10 === 0) {
    const supp = generated ? (100 * (1 - published / generated)).toFixed(1) : "0";
    console.log(`[sim] t=${ticks}s generated=${generated} published=${published} suppressed=${supp}% bytes=${bytes}`);
  }
};

const start = () => {
  console.log(`[sim] IoT Core ${IOT_ENDPOINT} (per-vehicle mTLS) | N=${N} rate=${RATE_MS}ms edgeFilter=${EDGE_FILTER ? "on" : "off"}`);
  if (DURATION_S > 0) setTimeout(dump, DURATION_S * 1000); // clean self-exit for experiments
  setInterval(tick, RATE_MS);
};

// Start ticking on the first connect; each publish is gated on its own vehicle being connected.
let started = false;
const startOnce = () => { if (!started) { started = true; start(); } };
vehicles.forEach((v) => v.client.on("connect", startOnce));

// Emit a metrics line on exit so experiments can capture edge-filter savings.
function dump() {
  const supp = generated ? (1 - published / generated) : 0;
  console.log(JSON.stringify({ metric: "sim", N, edgeFilter: EDGE_FILTER, ticks, generated, published, bytes, suppressedPct: +(100 * supp).toFixed(2) }));
  process.exit(0);
}
process.on("SIGINT", dump);
process.on("SIGTERM", dump);
