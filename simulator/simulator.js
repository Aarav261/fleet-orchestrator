// Fleet simulator: spawns N virtual vehicles that move on a coordinate grid and
// publish JSON telemetry to MQTT at 1 Hz. Implements on-vehicle edge filtering.
//
// Dials for the scalability experiments:
//   N=<count>          number of vehicles           (ingest capacity test)
//   EDGE_FILTER=on|off toggle edge filtering         (edge-filter efficiency test)
//
// Env:
//   MQTT_URL   default mqtt://localhost:1883
//   N          default 50
//   RATE_MS    telemetry tick interval, default 1000 (1 Hz)
//   EDGE_FILTER on (default) | off
//   CITY_LAT / CITY_LON  map centre (default Melbourne CBD)

import mqtt from "mqtt";

const MQTT_URL = process.env.MQTT_URL || "mqtt://localhost:1883";
const N = parseInt(process.env.N || "50", 10);
const RATE_MS = parseInt(process.env.RATE_MS || "1000", 10);
const EDGE_FILTER = (process.env.EDGE_FILTER || "on") !== "off";
const DURATION_S = parseInt(process.env.DURATION_S || "0", 10); // 0 = run forever
const CITY_LAT = parseFloat(process.env.CITY_LAT || "-37.8136");
const CITY_LON = parseFloat(process.env.CITY_LON || "144.9631");

// ~0.05 deg box around the city centre (~5.5 km)
const SPAN = 0.05;
const METERS_PER_DEG = 111_320;

// Edge-filter thresholds (from the plan)
const MOVE_M = 5; // publish if moved > 5 m
const SOC_PCT = 1; // or SOC changed > 1%
const HEARTBEAT_MS = 30_000; // forced heartbeat every 30 s

const rnd = (a, b) => a + Math.random() * (b - a);
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

  step() {
    // Move along heading, occasionally turn. Metres travelled this tick.
    if (Math.random() < 0.1) this.heading += rnd(-0.6, 0.6);
    const metres = (this.speed * 1000 / 3600) * (RATE_MS / 1000);
    this.lat += (metres * Math.cos(this.heading)) / METERS_PER_DEG;
    this.lon += (metres * Math.sin(this.heading)) / (METERS_PER_DEG * Math.cos((this.lat * Math.PI) / 180));
    this.speed = Math.max(0, Math.min(60, this.speed + rnd(-5, 5)));

    // Battery drains; below 15% the platform will route to charge (handled by dispatch).
    this.soc = Math.max(0, this.soc - rnd(0, 0.05));
    if (this.status !== "charging" && this.soc < 15) this.status = "charging";
    if (this.status === "charging") this.soc = Math.min(100, this.soc + 0.5); // simulated recharge
    if (this.status === "charging" && this.soc > 80) this.status = "available";

    // Rare transient fault
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

  // Edge filter: suppress unless moved > 5 m, SOC changed > 1%, status changed,
  // or a 30 s heartbeat is due. Returns the payload to send, or null.
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

const client = mqtt.connect(MQTT_URL, { reconnectPeriod: 2000 });
const vehicles = Array.from({ length: N }, (_, i) => new Vehicle(`veh-${String(i).padStart(4, "0")}`));

let ticks = 0, generated = 0, published = 0, bytes = 0;

client.on("connect", () => {
  console.log(`[sim] connected ${MQTT_URL} | N=${N} rate=${RATE_MS}ms edgeFilter=${EDGE_FILTER ? "on" : "off"}`);
  if (DURATION_S > 0) setTimeout(dump, DURATION_S * 1000); // clean self-exit for experiments
  setInterval(() => {
    const now = Date.now();
    for (const v of vehicles) {
      v.step();
      generated++;
      const p = v.filtered(now);
      if (p) {
        const msg = JSON.stringify(p);
        client.publish(`fleet/telemetry/${v.id}`, msg, { qos: 0 });
        published++; bytes += msg.length;
      }
    }
    ticks++;
    if (ticks % 10 === 0) {
      const supp = generated ? (100 * (1 - published / generated)).toFixed(1) : "0";
      console.log(`[sim] t=${ticks}s generated=${generated} published=${published} suppressed=${supp}% bytes=${bytes}`);
    }
  }, RATE_MS);
});

client.on("error", (e) => console.error("[sim] mqtt error:", e.message));

// Emit a metrics line on exit so experiments can capture edge-filter savings.
const dump = () => {
  const supp = generated ? (1 - published / generated) : 0;
  console.log(JSON.stringify({ metric: "sim", N, edgeFilter: EDGE_FILTER, ticks, generated, published, bytes, suppressedPct: +(100 * supp).toFixed(2) }));
  process.exit(0);
};
process.on("SIGINT", dump);
process.on("SIGTERM", dump);
